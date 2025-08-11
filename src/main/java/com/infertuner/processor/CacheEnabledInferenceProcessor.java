package com.infertuner.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.infertuner.model.InferenceRequest;
import com.infertuner.model.InferenceResponse;
import com.infertuner.cache.SimpleFrequencyCapture;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * 支持多种缓存策略的推理处理器
 * 
 * 策略类型：
 * - STATIC: 固定缓存大小，全程不变
 * - FLUID: 根据请求速率动态调整缓存大小
 * - FREQUENCY: 根据访问频率分布计算最优缓存大小
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
    
    // === KV缓存存储 ===
    private transient Map<String, KVData> localKVCache;
    private transient int currentCacheSize;  // 当前实际缓存大小
    
    // === 策略配置 ===
    private static final CacheStrategy CACHE_STRATEGY = CacheStrategy.FREQUENCY;
    
    // === STATIC策略配置参数 ===
    private static final int STATIC_CACHE_SIZE = 5;        // STATIC策略的固定大小
    
    // === 通用策略配置参数 ===
    private static final int INITIAL_CACHE_SIZE = 5;       // 动态策略的初始大小
    private static final int MIN_CACHE_SIZE = 5;            // 最小缓存：8个条目
    private static final int MAX_CACHE_SIZE = 60;           // 最大缓存：60个条目
    private static final int ADJUSTMENT_INTERVAL = 20;      // 每20个请求检查一次调整
    
    // === FLUID策略配置参数 ===
    private static final long TIME_WINDOW_MS = 3000L;       // 时间窗口：3秒
    private static final double EXPAND_THRESHOLD = 1.35;    // 扩容阈值：15%上升
    private static final double SHRINK_THRESHOLD = 0.65;    // 缩容阈值：15%下降  
    private static final double EXPAND_FACTOR = 1.15;        // 扩容因子：1.3倍
    private static final double SHRINK_FACTOR = 1.08;        // 缩容因子：除以1.2
    
    // === FREQUENCY策略配置参数 ===
    private static final int FREQUENCY_BUCKETS = 500;       // 频率捕获bucket数量
    private static final double TARGET_HIT_RATE = 0.9;      // 目标命中率80%
    private transient SimpleFrequencyCapture frequencyCapture;
    
    // === 其他配置 ===
    private static final String MODEL_PATH = "/workspace/models/Qwen1.5-1.8B-Chat";
    private static final String PYTHON_SCRIPT = "/workspace/infertuner-simple/scripts/simple_inference_service.py";
    private static final int REMOTE_DELAY_MS = 1000;         // 远端获取延迟
    
    // === 统计信息 ===
    private int totalRequests = 0;
    private int hitCount = 0;
    private double totalLatency = 0.0;
    
    // === FLUID策略专用变量 ===
    private final List<Long> requestTimestamps = new ArrayList<>();  // 请求时间戳记录
    private double historicalAverageRate = 0.0;  // 历史平均请求速率
    private int requestsSinceLastAdjustment = 0; // 距离上次调整的请求数
    private long lastAdjustmentTime = System.currentTimeMillis(); // 上次调整时间
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // 初始化频率捕获器（所有策略都初始化，便于对比分析）
        frequencyCapture = new SimpleFrequencyCapture(FREQUENCY_BUCKETS);
        
        // 初始化缓存大小
        currentCacheSize = getCurrentCacheSize();
        
        logger.info("启动KV缓存推理服务 (策略={}, 初始大小={})", 
                   CACHE_STRATEGY.name(), currentCacheSize);
        
        // 初始化LRU缓存
        localKVCache = new LinkedHashMap<String, KVData>(currentCacheSize + 1, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, KVData> eldest) {
                return size() > currentCacheSize;
            }
        };
        
        objectMapper = new ObjectMapper();
        random = new Random();
        
        // 启动Python推理服务
        ProcessBuilder pb = new ProcessBuilder("python3", PYTHON_SCRIPT, MODEL_PATH);
        pb.environment().put("PYTHONUNBUFFERED", "1");
        pythonProcess = pb.start();
        pythonInput = new BufferedWriter(new OutputStreamWriter(pythonProcess.getOutputStream()));
        pythonOutput = new BufferedReader(new InputStreamReader(pythonProcess.getInputStream()));
        Thread.sleep(5000);
        
        logger.info("KV缓存推理服务已启动");
    }
    
    /**
     * 根据策略获取当前缓存大小
     */
    private int getCurrentCacheSize() {
        switch (CACHE_STRATEGY) {
            case STATIC:
                return STATIC_CACHE_SIZE;  // 固定大小，永不改变
                
            case FLUID:
                return calculateFluidCacheSize();  // 基于请求速率动态调整
                
            case FREQUENCY:
                return calculateFrequencyCacheSize();  // 基于频率分布计算
                
            default:
                return STATIC_CACHE_SIZE;
        }
    }
    
    /**
     * FREQUENCY策略的缓存大小计算
     */
    private int calculateFrequencyCacheSize() {
        if (totalRequests < 10) {
            // 请求数太少，返回初始大小
            logger.debug("FREQUENCY初始化: 请求数不足，使用初始大小={}", INITIAL_CACHE_SIZE);
            return INITIAL_CACHE_SIZE;
        }
        
        // 使用频率捕获器计算最优缓存大小
        long estimatedSize = frequencyCapture.calculate(TARGET_HIT_RATE);
        
        // 限制在合理范围内
        int newSize = (int) Math.max(MIN_CACHE_SIZE, 
                       Math.min(MAX_CACHE_SIZE, estimatedSize));
        
        // 如果估算值为0或过小，使用当前大小
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
            // 请求数太少，返回初始大小
            historicalAverageRate = 1.0; // 设置合理的初始值
            logger.debug("FLUID初始化: 请求数不足，使用初始大小={}", INITIAL_CACHE_SIZE);
            return INITIAL_CACHE_SIZE;
        }
        
        double currentRate = calculateCurrentRequestRate();
        
        // 更新历史平均速率（指数移动平均）
        if (historicalAverageRate == 0.0) {
            historicalAverageRate = Math.max(currentRate, 0.5); // 避免除零，设置最小值0.5
        } else {
            historicalAverageRate = 0.7 * historicalAverageRate + 0.3 * currentRate;
            historicalAverageRate = Math.max(historicalAverageRate, 0.1); // 确保不为0
        }
        
        // 基于请求速率的缓存调整逻辑
        double expandThreshold = EXPAND_THRESHOLD * historicalAverageRate;
        double shrinkThreshold = SHRINK_THRESHOLD * historicalAverageRate;
        
        logger.info("FLUID调整检查: 当前速率={}, 历史均值={}, 扩容阈值={}, 缩容阈值={}, 当前缓存={}", 
                   String.format("%.2f", currentRate), 
                   String.format("%.2f", historicalAverageRate), 
                   String.format("%.2f", expandThreshold), 
                   String.format("%.2f", shrinkThreshold), 
                   currentCacheSize);
        
        if (currentRate > expandThreshold) {
            // 请求速率增加 → 扩容缓存
            int newSize = (int) Math.ceil(currentCacheSize * EXPAND_FACTOR);
            newSize = Math.min(newSize, MAX_CACHE_SIZE);
            
            logger.info("FLUID扩容: 当前速率={} > 阈值={}, 缓存 {} → {}", 
                       String.format("%.2f", currentRate), 
                       String.format("%.2f", expandThreshold), 
                       currentCacheSize, newSize);
            return newSize;
            
        } else if (currentRate < shrinkThreshold) {
            // 请求速率下降 → 缩容缓存
            int newSize = (int) Math.ceil(currentCacheSize / SHRINK_FACTOR);
            newSize = Math.max(newSize, MIN_CACHE_SIZE);
            
            logger.info("FLUID缩容: 当前速率={} < 阈值={}, 缓存 {} → {}", 
                       String.format("%.2f", currentRate), 
                       String.format("%.2f", shrinkThreshold), 
                       currentCacheSize, newSize);
            return newSize;
            
        } else {
            // 请求速率稳定 → 保持当前大小
            logger.debug("FLUID保持: 当前速率={}, 历史均值={}, 缓存大小={}", 
                        String.format("%.2f", currentRate), 
                        String.format("%.2f", historicalAverageRate), 
                        currentCacheSize);
            return currentCacheSize;
        }
    }
    
    /**
     * 计算当前请求速率（请求/秒）
     */
    private double calculateCurrentRequestRate() {
        long currentTime = System.currentTimeMillis();
        long windowStart = currentTime - TIME_WINDOW_MS;
        
        // 清理过期的时间戳记录
        requestTimestamps.removeIf(timestamp -> timestamp < windowStart);
        
        // 计算时间窗口内的请求速率
        if (requestTimestamps.isEmpty()) {
            return 0.0;
        }
        
        double requestsInWindow = requestTimestamps.size();
        // 使用实际的时间窗口长度，但不要让窗口太小
        long actualWindowMs = Math.max(currentTime - requestTimestamps.get(0), 1000); // 最小1秒
        
        double rate = requestsInWindow * 1000.0 / actualWindowMs;  // 转换为请求/秒
        
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
        String kvKey = request.userId + "_" + sessionId;
        
        // === 2. 记录到对应的策略跟踪器 ===
        if (CACHE_STRATEGY == CacheStrategy.FLUID) {
            requestTimestamps.add(System.currentTimeMillis());
        }
        
        // 所有策略都记录到频率捕获器（便于对比分析）
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
        
        // === 4. 查找本地KV缓存 ===
        KVData kvData = localKVCache.get(kvKey);
        
        InferenceResponse response;
        
        if (kvData != null) {
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
            
            // 存储新KV数据到本地缓存
            localKVCache.put(kvKey, new KVData(kvKey));
            
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
        
        // 如果缓存缩小了，需要手动移除多余的条目
        while (localKVCache.size() > currentCacheSize) {
            // LinkedHashMap的迭代器按照访问顺序，第一个是最久未访问的
            Iterator<String> iterator = localKVCache.keySet().iterator();
            if (iterator.hasNext()) {
                String oldestKey = iterator.next();
                iterator.remove();
                logger.debug("缓存缩容移除条目: {}", oldestKey);
            }
        }
        
        logger.info("缓存大小调整完成: {} → {} (当前条目数: {})", 
                   oldSize, newSize, localKVCache.size());
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
     * 生成用户会话ID - 控制不同用户的缓存命中率
     */
    private String generateSessionId(String userId) {
        int userNum = Integer.parseInt(userId.replaceAll("\\D", ""));
        
        // 前面的用户会话重用率高，后面的用户会话重用率低
        if (userNum <= 5) {
            return String.valueOf(userNum % 2);  // 会话0或1，容易命中
        } else if (userNum <= 10) {
            return String.valueOf(userNum % 3);  // 会话0,1,2，中等命中率
        } else {
            return String.valueOf(userNum % 5);  // 会话0,1,2,3,4，命中率低
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
        
        logger.info("KV缓存推理服务已关闭");
    }
    
    /**
     * KV数据结构 - 简单存储
     */
    private static class KVData {
        public final String key;
        public final long timestamp;
        
        public KVData(String key) {
            this.key = key;
            this.timestamp = System.currentTimeMillis();
        }
    }
}