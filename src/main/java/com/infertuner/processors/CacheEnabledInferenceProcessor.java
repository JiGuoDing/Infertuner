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
import java.net.UnknownHostException;
import java.util.*;

/**
 * 整合二级缓存的推理处理器
 * 使用 TwoLevelCacheManager 替代简单的 Map 缓存
 */
public class CacheEnabledInferenceProcessor extends RichMapFunction<InferenceRequest, InferenceResponse> {

    private static final Logger logger = LoggerFactory.getLogger(CacheEnabledInferenceProcessor.class);

    // 缓存策略枚举类
    public enum CacheStrategy {
        STATIC, // 固定大小策略
        FLUID, // 动态调整策略
        FREQUENCY // 频率分析策略
    }

    private String nodeIP;

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
    // 模型路径
    private static final String MODEL_PATH = "/mnt/tidal-alsh01/usr/suqian/models/Falcon3-7B-Instruct";
    // 推理脚本路径
    private static final String PYTHON_SCRIPT = "/mnt/tidal-alsh01/usr/suqian/scripts/batch_inference_service_new.py";
    private static final int REMOTE_DELAY_MS = 1000;
    // 每个字符对应的KV缓存字节数
    private static final int BYTES_PER_CHAR = 2 * 1024;

    // === 统计信息 ===
    private int totalRequests = 0;
    private int hitCount = 0;
    private double totalLatency = 0.0;

    // === FLUID策略专用变量 ===
    // 记录请求到达时间
    private final List<Long> requestTimestamps = new ArrayList<>();
    private double historicalAverageRate = 0.0;
    private int requestsSinceLastAdjustment = 0;

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

        // 获取的当前节点IP
        try {
            nodeIP = java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.error("获取当前节点IP失败", e);
            nodeIP = "Unknown-hostIP";
        }

        objectMapper = new ObjectMapper();
        random = new Random();

        // 启动Python推理服务
        ProcessBuilder pb = new ProcessBuilder("/opt/conda/envs/vllm-env/bin/python", PYTHON_SCRIPT, nodeIP,
                MODEL_PATH);
        pb.redirectErrorStream(false);
        pb.environment().put("PYTHONUNBUFFERED", "1");
        pythonProcess = pb.start();
        pythonInput = new BufferedWriter(new OutputStreamWriter(pythonProcess.getOutputStream()));
        pythonOutput = new BufferedReader(new InputStreamReader(pythonProcess.getInputStream()));
        Thread.sleep(5000);

        logger.info("二级缓存推理服务已启动");
    }

    @Override
    public InferenceResponse map(InferenceRequest request) throws Exception {
        totalRequests++;
        requestsSinceLastAdjustment++;

        // === 1. 生成KV缓存Key(每个会话一份KV缓存) ===
        // userId: user_%04d, userIndex
        String sessionId = generateSessionIdOfOneUser(request.userId);

        // === 2. 如果策略是 Fluid,则记录到请求到达时间表 ===
        if (CACHE_STRATEGY == CacheStrategy.FLUID) {
            requestTimestamps.add(System.currentTimeMillis());
        }

        // 所有策略都记录到频率捕获器
        String kvKey = request.userId + "_" + sessionId;
        frequencyCapture.add(kvKey);

        // === 3. 定期检查是否需要调整缓存大小 ===
        // 每 15 个请求检查一次
        if (CACHE_STRATEGY != CacheStrategy.STATIC &&
                requestsSinceLastAdjustment >= ADJUSTMENT_INTERVAL) {

            int newSize = getCurrentCacheSize();
            if (newSize != currentCacheSize) {
                adjustCacheSize(newSize);
            }
            requestsSinceLastAdjustment = 0;
            System.currentTimeMillis();
        }

        // === 4. 使用二级缓存管理器查找缓存 ===
        KVCacheEntry kvEntry = cacheManager.get(kvKey);

        InferenceResponse response;

        if (kvEntry != null) {
            // 🟢 缓存命中在本地缓存或远端缓存：正常推理
            hitCount++;
            response = performInference(request, false);
            response.fromCache = true;
            response.responseDescription = response.responseDescription + "-Hit";

            logger.info("[{}] 命中: {}ms (策略={}, 缓存大小={})",
                    request.requestId, response.inferenceTimeMs,
                    CACHE_STRATEGY.name(), currentCacheSize);

        } else {
            // 🔴 缓存未命中：推理+远端延迟，然后模拟创建新KV值并存如缓存
            response = performInference(request, true);
            response.fromCache = false;
            response.responseDescription = response.responseDescription + "-Miss";

            // 存储新KV数据到二级缓存
            byte[] kvData = generateKVData(request);
            cacheManager.put(request.userId, sessionId, kvData);

            logger.info("[{}] 缓存未命中: 推理时延{}ms (+{}ms) (策略={}, 缓存大小={})",
                    request.requestId, response.inferenceTimeMs, REMOTE_DELAY_MS,
                    CACHE_STRATEGY.name(), currentCacheSize);
        }

        totalLatency += response.inferenceTimeMs;

        return response;
    }

    /**
     * 根据策略获取当前缓存大小
     */
    private int getCurrentCacheSize() {
        switch (CACHE_STRATEGY) {
            case FLUID:
                return calculateFluidCacheSize();
            case FREQUENCY:
                return calculateFrequencyCacheSize();
            case STATIC:
            default:
                return STATIC_CACHE_SIZE;
        }
    }

    /**
     * FREQUENCY策略的缓存大小计算
     */
    private int calculateFrequencyCacheSize() {
        if (totalRequests < 10) {
            logger.debug("FREQUENCY初始化: 请求数不足({} < 10)，使用初始大小={}", totalRequests, INITIAL_CACHE_SIZE);
            return INITIAL_CACHE_SIZE;
        }

        long estimatedSize = frequencyCapture.calculate(TARGET_HIT_RATE);
        int newSize = (int) Math.max(MIN_CACHE_SIZE,
                Math.min(MAX_CACHE_SIZE, estimatedSize));

        if (estimatedSize <= 0) {
            newSize = currentCacheSize;
        }

        SimpleFrequencyCapture.Stats stats = frequencyCapture.getStats();
        logger.info("FREQUENCY计算: 目标缓存命中率={}, 估算缓存大小={}, 实际缓存大小={}, FrequencyCapture统计={}",
                String.format("%.2f", TARGET_HIT_RATE), estimatedSize, newSize, stats);

        return newSize;
    }

    /**
     * FLUID策略的缓存大小计算
     * <br>
     * 核心思想：根据实时请求速率的变化来自动扩缩容
     */
    private int calculateFluidCacheSize() {
        // 冷启动阶段
        if (totalRequests < 5) {
            historicalAverageRate = 1.0;
            logger.debug("FLUID初始化: 请求数不足，使用初始大小={}", INITIAL_CACHE_SIZE);
            return INITIAL_CACHE_SIZE;
        }

        // 获取最近一个时间窗口的请求速率
        double currentRate = calculateCurrentRequestRate();

        if (historicalAverageRate == 0.0) {
            historicalAverageRate = Math.max(currentRate, 0.5);
        } else {
            // 为了防止因瞬间的请求抖动导致缓存大小频繁变化，使用指数移动平均(EMA)算法来计算一个平滑、长期的历史平均请求速率
            historicalAverageRate = 0.7 * historicalAverageRate + 0.3 * currentRate;
            historicalAverageRate = Math.max(historicalAverageRate, 0.1);
        }

        // 定义动态阈值，基于平滑后的历史平均请求速率，计算出扩缩容的阈值
        double expandThreshold = EXPAND_THRESHOLD * historicalAverageRate;
        double shrinkThreshold = SHRINK_THRESHOLD * historicalAverageRate;

        logger.info("FLUID调整检查: 当前请求速率={}, 历史请求速率均值={}, 扩容阈值={}, 缩容阈值={}, 当前缓存容量={}",
                String.format("%.2f", currentRate),
                String.format("%.2f", historicalAverageRate),
                String.format("%.2f", expandThreshold),
                String.format("%.2f", shrinkThreshold),
                currentCacheSize);

        // 扩容
        if (currentRate > expandThreshold) {
            int newSize = (int) Math.ceil(currentCacheSize * EXPAND_FACTOR);
            newSize = Math.min(newSize, MAX_CACHE_SIZE);
            logger.info("FLUID扩容: 当前请求速率={} > 阈值={}, 缓存容量 {} → {}",
                    String.format("%.2f", currentRate),
                    String.format("%.2f", expandThreshold),
                    currentCacheSize, newSize);
            return newSize;
            // 缩容
        } else if (currentRate < shrinkThreshold) {
            int newSize = (int) Math.ceil(currentCacheSize / SHRINK_FACTOR);
            newSize = Math.max(newSize, MIN_CACHE_SIZE);
            logger.info("FLUID缩容: 当前请求速率={} < 阈值={}, 缓存容量 {} → {}",
                    String.format("%.2f", currentRate),
                    String.format("%.2f", shrinkThreshold),
                    currentCacheSize, newSize);
            return newSize;
        } else {
            logger.debug("FLUID保持: 当前请求速率={}, 历史请求速率均值={}, 缓存容量={}",
                    String.format("%.2f", currentRate),
                    String.format("%.2f", historicalAverageRate),
                    currentCacheSize);
            return currentCacheSize;
        }
    }

    /**
     * 计算在最近一个时间窗口内的平均每秒请求数
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
        // // 模拟KV cache数据：基于用户消息生成固定大小的数据
        // String data = "KV_" + request.userId + "_" + request.userMessage.hashCode();
        // return data.getBytes();

        // 根据用户消息长度估算KV缓存大小
        int messageLength = request.userMessage.length();
        // 保证一个最小大小，避免消息过短时缓存大小为0
        int estimatedSize = Math.max(1, messageLength) * BYTES_PER_CHAR;

        // 创建一个指定大小的字节数组作为模拟数据
        byte[] kvData = new byte[estimatedSize];
        // （可选）用随机数据填充，使其内容不为空
        random.nextBytes(kvData);

        logger.debug("为请求 {} 生成模拟KV数据，消息长度: {}, 估算大小: {} bytes",
                request.getRequestId(), messageLength, estimatedSize);

        return kvData;
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
        req.put("max_tokens", request.maxNewTokens);
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
        response.responseText = responseNode.get("response").asText();
        response.responseDescription = responseNode.get("model_name").asText();

        // 总延迟 = 远端延迟 + 推理时间
        double inferenceTime = responseNode.get("inference_time_ms").asDouble();
        response.inferenceTimeMs = (addDelay ? REMOTE_DELAY_MS : 0) + inferenceTime;

        return response;
    }

    /**
     * 生成用户会话ID
     */
    private String generateSessionIdOfOneUser(String userId) {
        int userNum = Integer.parseInt(userId.replaceAll("\\D", ""));

        // 模拟一个用户可能拥有多个独立上下文会话
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
        if (pythonOutput != null)
            pythonOutput.close();
        if (pythonProcess != null)
            pythonProcess.destroyForcibly();

        logger.info("二级缓存推理服务已关闭");
    }
}