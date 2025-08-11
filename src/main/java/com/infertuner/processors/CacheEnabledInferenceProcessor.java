package com.infertuner.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.infertuner.models.InferenceRequest;
import com.infertuner.models.InferenceResponse;
import com.infertuner.optimization.SimpleFrequencyCapture;
import com.infertuner.cache.TwoLevelCacheManager;
import com.infertuner.cache.KVCacheEntry;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * 整合二级缓存的推理处理器
 * 使用 TwoLevelCacheManager 替代简单的 Map 缓存
 */
public class CacheEnabledInferenceProcessor extends RichMapFunction<InferenceRequest, InferenceResponse> {
    
    private static final Logger logger = LoggerFactory.getLogger(CacheEnabledInferenceProcessor.class);
    
    public enum CacheStrategy {
        STATIC,     // 固定大小策略
        FLUID,      // 动态调整策略
        FREQUENCY   // 频率分析策略
    }
    
    private transient Process pythonProcess;
    private transient BufferedWriter pythonInput;
    private transient BufferedReader pythonOutput;
    private transient ObjectMapper objectMapper;
    private transient Random random;
    
    // === 使用二级缓存管理器 ===
    private transient TwoLevelCacheManager cacheManager;
    private transient int currentCacheSize;
    
    // === 策略配置 ===
    private static final CacheStrategy CACHE_STRATEGY = CacheStrategy.STATIC;
    
    // === STATIC策略配置参数 ===
    private static final int STATIC_CACHE_SIZE = 5;
    
    // === 通用策略配置参数 ===
    private static final int INITIAL_CACHE_SIZE = 5;
    private static final int MIN_CACHE_SIZE = 5;
    private static final int MAX_CACHE_SIZE = 50;
    private static final int ADJUSTMENT_INTERVAL = 15;
    
    // === FLUID策略配置参数 ===
    private static final long TIME_WINDOW_MS = 3000L;
    private static final double EXPAND_THRESHOLD = 1.35;
    private static final double SHRINK_THRESHOLD = 0.65;
    private static final double EXPAND_FACTOR = 1.25;
    private static final double SHRINK_FACTOR = 1.15;
    
    // === FREQUENCY策略配置参数 ===
    private static final int FREQUENCY_BUCKETS = 200;
    private static final double TARGET_HIT_RATE = 0.85;
    private transient SimpleFrequencyCapture frequencyCapture;
    
    // === 其他配置 ===
    private static final String MODEL_PATH = "/workspace/models/Qwen1.5-1.8B-Chat";
    private static final String PYTHON_SCRIPT = "/workspace/infertuner/scripts/simple_inference_service.py";
    private static final int REMOTE_DELAY_MS = 1000;
    
    // === 统计信息 ===
    private int totalRequests = 0;
    private int hitCount = 0;
    private double totalLatency = 0.0;
    
    // === FLUID策略专用变量 ===
    private final List<Long> requestTimestamps = new ArrayList<>();
    private double historicalAverageRate = 0.0;
    private int requestsSinceLastAdjustment = 0;
    private long lastAdjustmentTime = System.currentTimeMillis();
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // 初始化频率捕获器
        frequencyCapture = new SimpleFrequencyCapture(FREQUENCY_BUCKETS);
        
        // 初始化缓存大小
        currentCacheSize = getCurrentCacheSize();
        
        logger.info("启动二级缓存推理服务 (策略={}, 初始大小={})", 
                   CACHE_STRATEGY.name(), currentCacheSize);
        
        // 初始化二级缓存管理器
        cacheManager = new TwoLevelCacheManager(currentCacheSize);
        
        objectMapper = new ObjectMapper();
        random = new Random();
        
        // 启动Python推理服务
        ProcessBuilder pb = new ProcessBuilder("python3", PYTHON_SCRIPT, MODEL_PATH);
        pb.environment().put("PYTHONUNBUFFERED", "1");
        pythonProcess = pb.start();
        pythonInput = new BufferedWriter(new OutputStreamWriter(pythonProcess.getOutputStream()));
        pythonOutput = new BufferedReader(new InputStreamReader(pythonProcess.getInputStream()));
        Thread.sleep(5000);
        
        logger.info("二级缓存推理服务已启动");
    }
    
    /**
     * 根据策略获取当前缓存大小
     */
    private int getCurrentCacheSize() {
        switch (CACHE_STRATEGY) {
            case STATIC:
                return STATIC_CACHE_SIZE;
            case FLUID:
                return calculateFluidCacheSize();
            case FREQUENCY:
                return calculateFrequencyCacheSize();
            default:
                return STATIC_CACHE_SIZE;
        }
    }
    
    /**
     * FREQUENCY策略的缓存大小计算
     */
    private int calculateFrequencyCacheSize() {
        if (totalRequests < 10) {
            logger.debug("FREQUENCY初始化: 请求数不足，使用初始大小={}", INITIAL_CACHE_SIZE);
            return INITIAL_CACHE_SIZE;
        }
        
        long estimatedSize = frequencyCapture.calculate(TARGET_HIT_RATE);
        int newSize = (int) Math.max(MIN_CACHE_SIZE, 
                       Math.min(MAX_CACHE_SIZE, estimatedSize));
        
        if (estimatedSize <= 0) {
            newSize = currentCacheSize;
        }
        
        SimpleFrequencyCapture.Stats stats = frequencyCapture.getStats();
        logger.info("FREQUENCY计算: 目标命中率={}, 估算大小={}, 实际大小={}, 统计={}",
                   String.format("%.2f", TARGET_HIT_RATE), estimatedSize, newSize, stats);
        
        return newSize;
    }
    
    /**
     * FLUID策略的缓存大小计算
     */
    private int calculateFluidCacheSize() {
        if (totalRequests < 5) {
            historicalAverageRate = 1.0;
            logger.debug("FLUID初始化: 请求数不足，使用初始大小={}", INITIAL_CACHE_SIZE);
            return INITIAL_CACHE_SIZE;
        }
        
        double currentRate = calculateCurrentRequestRate();
        
        if (historicalAverageRate == 0.0) {
            historicalAverageRate = Math.max(currentRate, 0.5);
        } else {
            historicalAverageRate = 0.7 * historicalAverageRate + 0.3 * currentRate;
            historicalAverageRate = Math.max(historicalAverageRate, 0.1);
        }
        
        double expandThreshold = EXPAND_THRESHOLD * historicalAverageRate;
        double shrinkThreshold = SHRINK_THRESHOLD * historicalAverageRate;
        
        logger.info("FLUID调整检查: 当前速率={}, 历史均值={}, 扩容阈值={}, 缩容阈值={}, 当前缓存={}", 
                   String.format("%.2f", currentRate), 
                   String.format("%.2f", historicalAverageRate), 
                   String.format("%.2f", expandThreshold), 
                   String.format("%.2f", shrinkThreshold), 
                   currentCacheSize);
        
        if (currentRate > expandThreshold) {
            int newSize = (int) Math.ceil(currentCacheSize * EXPAND_FACTOR);
            newSize = Math.min(newSize, MAX_CACHE_SIZE);
            logger.info("FLUID扩容: 当前速率={} > 阈值={}, 缓存 {} → {}", 
                       String.format("%.2f", currentRate), 
                       String.format("%.2f", expandThreshold), 
                       currentCacheSize, newSize);
            return newSize;
        } else if (currentRate < shrinkThreshold) {
            int newSize = (int) Math.ceil(currentCacheSize / SHRINK_FACTOR);
            newSize = Math.max(newSize, MIN_CACHE_SIZE);
            logger.info("FLUID缩容: 当前速率={} < 阈值={}, 缓存 {} → {}", 
                       String.format("%.2f", currentRate), 
                       String.format("%.2f", shrinkThreshold), 
                       currentCacheSize, newSize);
            return newSize;
        } else {
            logger.debug("FLUID保持: 当前速率={}, 历史均值={}, 缓存大小={}", 
                        String.format("%.2f", currentRate), 
                        String.format("%.2f", historicalAverageRate), 
                        currentCacheSize);
            return currentCacheSize;
        }
    }
    
    /**
     * 计算当前请求速率
     */
    private double calculateCurrentRequestRate() {
        long currentTime = System.currentTimeMillis();
        long windowStart = currentTime - TIME_WINDOW_MS;
        
        requestTimestamps.removeIf(timestamp -> timestamp < windowStart);
        
        if (requestTimestamps.isEmpty()) {
            return 0.0;
        }
        
        double requestsInWindow = requestTimestamps.size();
        long actualWindowMs = Math.max(currentTime - requestTimestamps.get(0), 1000);
        
        double rate = requestsInWindow * 1000.0 / actualWindowMs;
        
        logger.debug("速率计算: 窗口内请求数={}, 实际窗口={}ms, 计算速率={}请求/秒", 
                    requestsInWindow, actualWindowMs, String.format("%.2f", rate));
        
        return rate;
    }
    
    @Override
    public InferenceResponse map(InferenceRequest request) throws Exception {
        totalRequests++;
        requestsSinceLastAdjustment++;
        
        // === 1. 生成KV缓存Key ===
        String sessionId = generateSessionId(request.userId);
        
        // === 2. 记录到对应的策略跟踪器 ===
        if (CACHE_STRATEGY == CacheStrategy.FLUID) {
            requestTimestamps.add(System.currentTimeMillis());
        }
        
        // 所有策略都记录到频率捕获器
        String kvKey = request.userId + "_" + sessionId;
        frequencyCapture.add(kvKey);
        
        // === 3. 定期检查是否需要调整缓存大小 ===
        if (CACHE_STRATEGY != CacheStrategy.STATIC && 
            requestsSinceLastAdjustment >= ADJUSTMENT_INTERVAL) {
            
            int newSize = getCurrentCacheSize();
            if (newSize != currentCacheSize) {
                adjustCacheSize(newSize);
            }
            requestsSinceLastAdjustment = 0;
            lastAdjustmentTime = System.currentTimeMillis();
        }
        
        // === 4. 使用二级缓存管理器查找缓存 ===
        KVCacheEntry kvEntry = cacheManager.get(request.userId, sessionId);
        
        InferenceResponse response;
        
        if (kvEntry != null) {
            // 🟢 缓存命中：正常推理
            hitCount++;
            response = performInference(request, false);
            response.fromCache = true;
            response.modelName = response.modelName + "-Hit";
            
            logger.info("[{}] 命中: {}ms (策略={}, 缓存大小={})", 
                       request.requestId, response.inferenceTimeMs, 
                       CACHE_STRATEGY.name(), currentCacheSize);
            
        } else {
            // 🔴 缓存未命中：推理+远端延迟，然后存储新KV
            response = performInference(request, true);
            response.fromCache = false;
            response.modelName = response.modelName + "-Miss";
            
            // 存储新KV数据到二级缓存
            byte[] kvData = generateKVData(request);
            cacheManager.put(request.userId, sessionId, kvData);
            
            logger.info("[{}] 未命中: {}ms (+{}ms) (策略={}, 缓存大小={})", 
                       request.requestId, response.inferenceTimeMs, REMOTE_DELAY_MS,
                       CACHE_STRATEGY.name(), currentCacheSize);
        }
        
        totalLatency += response.inferenceTimeMs;
        
        return response;
    }
    
    /**
     * 动态调整缓存大小
     */
    private void adjustCacheSize(int newSize) {
        if (newSize == currentCacheSize) {
            return;
        }
        
        int oldSize = currentCacheSize;
        currentCacheSize = newSize;
        
        // 调整二级缓存管理器的本地缓存大小
        cacheManager.resizeLocalCache(newSize);
        
        logger.info("缓存大小调整完成: {} → {} (二级缓存统计: {})", 
                   oldSize, newSize, cacheManager.getStats());
    }
    
    /**
     * 生成模拟的KV数据
     */
    private byte[] generateKVData(InferenceRequest request) {
        // 模拟KV cache数据：基于用户消息生成固定大小的数据
        String data = "KV_" + request.userId + "_" + request.userMessage.hashCode();
        return data.getBytes();
    }
    
    /**
     * 执行推理
     */
    private InferenceResponse performInference(InferenceRequest request, boolean addDelay) throws Exception {
        // 模拟远端KV获取延迟
        if (addDelay) {
            Thread.sleep(REMOTE_DELAY_MS);
        }
        
        // 调用Python推理服务
        Map<String, Object> req = new HashMap<>();
        req.put("user_message", request.userMessage);
        req.put("max_tokens", request.maxTokens);
        req.put("request_id", request.requestId);
        
        String requestJson = objectMapper.writeValueAsString(req);
        pythonInput.write(requestJson + "\n");
        pythonInput.flush();
        
        String responseJson = pythonOutput.readLine();
        JsonNode responseNode = objectMapper.readTree(responseJson);
        
        // 构建响应
        InferenceResponse response = new InferenceResponse();
        response.requestId = request.requestId;
        response.userId = request.userId;
        response.userMessage = request.userMessage;
        response.success = responseNode.get("success").asBoolean();
        response.aiResponse = responseNode.get("response").asText();
        response.modelName = responseNode.get("model_name").asText();
        
        // 总延迟 = 远端延迟 + 推理时间
        double inferenceTime = responseNode.get("inference_time_ms").asDouble();
        response.inferenceTimeMs = (addDelay ? REMOTE_DELAY_MS : 0) + inferenceTime;
        
        return response;
    }
    
    /**
     * 生成用户会话ID
     */
    private String generateSessionId(String userId) {
        int userNum = Integer.parseInt(userId.replaceAll("\\D", ""));
        
        if (userNum <= 5) {
            return String.valueOf(userNum % 2);
        } else if (userNum <= 10) {
            return String.valueOf(userNum % 3);
        } else {
            return String.valueOf(userNum % 5);
        }
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        
        // 输出最终统计
        if (totalRequests > 0) {
            double avgLatency = totalLatency / totalRequests;
            double hitRate = (double) hitCount / totalRequests * 100;
            
            logger.info("=== 最终统计 (策略: {}) ===", CACHE_STRATEGY.name());
            logger.info("总请求: {}", totalRequests);
            logger.info("缓存命中: {} ({}%)", hitCount, String.format("%.1f", hitRate));
            logger.info("平均延迟: {}ms", String.format("%.1f", avgLatency));
            logger.info("最终缓存大小: {}", currentCacheSize);
            
            // 输出二级缓存统计
            TwoLevelCacheManager.CacheStats cacheStats = cacheManager.getStats();
            logger.info("二级缓存统计: {}", cacheStats);
            
            if (CACHE_STRATEGY == CacheStrategy.FLUID) {
                logger.info("FLUID策略统计: 历史平均速率={}请求/秒", String.format("%.2f", historicalAverageRate));
            } else if (CACHE_STRATEGY == CacheStrategy.FREQUENCY) {
                SimpleFrequencyCapture.Stats stats = frequencyCapture.getStats();
                logger.info("FREQUENCY策略统计: {}", stats);
            }
            
            logger.info("================");
        }
        
        // 关闭推理服务
        if (pythonInput != null) {
            pythonInput.write("{\"command\": \"shutdown\"}\n");
            pythonInput.flush();
            pythonInput.close();
        }
        if (pythonOutput != null) pythonOutput.close();
        if (pythonProcess != null) pythonProcess.destroyForcibly();
        
        logger.info("二级缓存推理服务已关闭");
    }
}