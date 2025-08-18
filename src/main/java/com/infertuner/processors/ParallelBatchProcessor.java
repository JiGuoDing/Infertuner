package com.infertuner.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.infertuner.models.InferenceRequest;
import com.infertuner.models.InferenceResponse;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 批处理器 - 不使用Flink State，避免keyBy问题
 * 使用内存缓冲实现攒批功能
 */
public class ParallelBatchProcessor extends ProcessFunction<InferenceRequest, InferenceResponse> {

    private static final Logger logger = LoggerFactory.getLogger(ParallelBatchProcessor.class);

    // GPU相关
    private int gpuId;
    private int taskIndex;
    private int totalParallelism;
    private transient Process inferenceProcess;  // 标记为transient
    private transient BufferedWriter processInput;  // 标记为transient
    private transient BufferedReader processOutput;  // 标记为transient
    private transient ObjectMapper objectMapper;  // 标记为transient

    // 攒批配置
    private int targetBatchSize = 4;
    private long maxWaitTimeMs = 3000;

    // 内存缓冲区- 在open()中初始化
    private transient Queue<InferenceRequest> requestBuffer;
    private transient Queue<Long> arrivalTimes;
    private transient volatile long firstRequestTime = 0;
    private transient volatile int batchCounter = 0;
    private transient Object batchLock;

    private static final String MODEL_PATH = "/workspace/models/Qwen1.5-1.8B-Chat";
    private static final String BATCH_SERVICE_SCRIPT = "/workspace/infertuner/scripts/batch_inference_service.py";
    private static final int MAX_GPUS = 4;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 初始化transient字段
        requestBuffer = new ConcurrentLinkedQueue<>();
        arrivalTimes = new ConcurrentLinkedQueue<>();
        batchLock = new Object();
        objectMapper = new ObjectMapper();

        taskIndex = getRuntimeContext().getIndexOfThisSubtask();
        totalParallelism = getRuntimeContext().getNumberOfParallelSubtasks();
        gpuId = taskIndex % MAX_GPUS;

        // 从全局参数获取配置
        try {
            Map<String, String> globalParams = getRuntimeContext().getExecutionConfig()
                    .getGlobalJobParameters().toMap();

            if (globalParams.containsKey("batch.size")) {
                targetBatchSize = Integer.parseInt(globalParams.get("batch.size"));
            }
            if (globalParams.containsKey("max.wait.ms")) {
                maxWaitTimeMs = Long.parseLong(globalParams.get("max.wait.ms"));
            }
        } catch (Exception e) {
            logger.warn("使用默认配置: batchSize={}, maxWait={}ms", targetBatchSize, maxWaitTimeMs);
        }

        logger.info("🎯 GPU {} 简化并行攒批处理器启动: 并行度={}, 批大小={}, 超时={}ms",
                gpuId, totalParallelism, targetBatchSize, maxWaitTimeMs);
        logger.info("📋 GPU {} 负责处理: taskIndex={}, 使用内存缓冲区", gpuId, taskIndex);

        objectMapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // 启动GPU服务
        startGPUService();

        logger.info("✅ GPU {} 简化并行攒批服务启动完成", gpuId);
    }

    private void startGPUService() throws Exception {
        logger.info("启动 GPU {} 推理服务...", gpuId);

        ProcessBuilder pb = new ProcessBuilder("python3", BATCH_SERVICE_SCRIPT, MODEL_PATH, String.valueOf(gpuId));
        pb.redirectErrorStream(false);
        inferenceProcess = pb.start();

        processInput = new BufferedWriter(new OutputStreamWriter(inferenceProcess.getOutputStream()));
        processOutput = new BufferedReader(new InputStreamReader(inferenceProcess.getInputStream()));

        // 等待服务启动
        logger.info("等待 GPU {} 服务启动...", gpuId);
        Thread.sleep(3000);

        if (!inferenceProcess.isAlive()) {
            throw new RuntimeException("GPU " + gpuId + " 推理服务启动失败");
        }

        logger.info("✅ GPU {} 推理服务启动成功", gpuId);
    }

    @Override
    public void processElement(InferenceRequest request, Context ctx, Collector<InferenceResponse> out) throws Exception {
        long arrivalTime = request.timestamp;

        synchronized (batchLock) {
            // 将请求加入缓冲区
            requestBuffer.offer(request);
            arrivalTimes.offer(arrivalTime);

            int currentSize = requestBuffer.size();

            // 记录第一个请求时间
            if (currentSize == 1) {
                firstRequestTime = arrivalTime;
                logger.info("GPU {} 开始新批次: {} (1/{}) - rebalance分发", gpuId, request.requestId, targetBatchSize);
            }

            // 🔧 修复：检查是否攒够了 - 如果批大小为1，立即处理
            if (currentSize >= targetBatchSize) {
                logger.info("🚀 GPU {} 攒够{}个请求，开始处理", gpuId, targetBatchSize);
                processBatch(out, "数量触发");
            }
        }
    }

    private void processBatch(Collector<InferenceResponse> out, String triggerReason) throws Exception {
        List<InferenceRequest> batch = new ArrayList<>();
        List<Long> batchArrivalTimes = new ArrayList<>();

        // 提取批次请求
        for (int i = 0; i < targetBatchSize && !requestBuffer.isEmpty(); i++) {
            InferenceRequest req = requestBuffer.poll();
            Long arrivalTime = arrivalTimes.poll();
            if (req != null && arrivalTime != null) {
                batch.add(req);
                batchArrivalTimes.add(arrivalTime);
            }
        }

        if (batch.isEmpty()) {
            return;
        }

        int batchSize = batch.size();
        int currentBatchNum = ++batchCounter;

        // 🔧 修复批次触发时间计算
        long realBatchTriggerTime;
        if ("数量触发".equals(triggerReason) && !batchArrivalTimes.isEmpty()) {
            // 数量触发：使用最后一个请求的到达时间作为触发时间
            realBatchTriggerTime = batchArrivalTimes.get(batchArrivalTimes.size() - 1);
            logger.info("🔥 GPU {} 批次#{} 开始: {} | {}个请求 (触发时间={})",
                    gpuId, currentBatchNum, triggerReason, batchSize,
                    new java.util.Date(realBatchTriggerTime));
        } else {
            // 超时触发：使用当前时间
            realBatchTriggerTime = System.currentTimeMillis();
            logger.info("🔥 GPU {} 批次#{} 开始: {} | {}个请求 (触发时间={})",
                    gpuId, currentBatchNum, triggerReason, batchSize,
                    new java.util.Date(realBatchTriggerTime));
        }

        // 构建批量请求
        BatchRequestData batchRequest = new BatchRequestData();
        batchRequest.requests = new ArrayList<>();

        for (InferenceRequest req : batch) {
            SingleRequestData singleReq = new SingleRequestData();
            singleReq.user_message = req.userMessage;
            singleReq.max_tokens = req.maxTokens;
            singleReq.request_id = req.requestId;
            batchRequest.requests.add(singleReq);
        }

        batchRequest.batch_size = batchSize;
        batchRequest.batch_id = String.format("gpu%d_batch_%d_%d", gpuId, currentBatchNum, realBatchTriggerTime);

        // 发送到GPU服务并获取响应
        long batchStartTime = System.currentTimeMillis();

        String requestJson = objectMapper.writeValueAsString(batchRequest);
        processInput.write(requestJson + "\n");
        processInput.flush();

        String responseJson = processOutput.readLine();
        if (responseJson == null) {
            throw new RuntimeException("GPU " + gpuId + " 无响应");
        }

        BatchResponseData batchResponse = objectMapper.readValue(responseJson, BatchResponseData.class);

        if (!batchResponse.success) {
            throw new RuntimeException("GPU " + gpuId + " 处理失败: " + batchResponse.error);
        }

        long batchEndTime = System.currentTimeMillis();
        long totalProcessTime = batchEndTime - batchStartTime;
        double avgProcessTimePerRequest = (double) totalProcessTime / batchSize;

        logger.info("📊 GPU {} 批次#{} 完成: 总时间={}ms, 平均={}ms/req",
                gpuId, currentBatchNum, totalProcessTime, String.format("%.1f", avgProcessTimePerRequest));

        // 生成响应并输出
        for (int i = 0; i < batch.size(); i++) {
            InferenceRequest originalReq = batch.get(i);
            SingleResponseData singleResp = batchResponse.responses.get(i);
            long requestArrivalTime = batchArrivalTimes.get(i);

            InferenceResponse response = new InferenceResponse();
            response.requestId = originalReq.requestId;
            response.userId = originalReq.userId;
            response.userMessage = originalReq.userMessage;
            response.aiResponse = singleResp.response;
            response.inferenceTimeMs = avgProcessTimePerRequest;
            response.success = singleResp.success;
            response.modelName = String.format("GPU-%d", gpuId);
            response.fromCache = false;
            response.batchSize = batchSize;
            response.timestamp = batchEndTime;

            // 🔧 批次触发时间计算等待时间
            // 等待时间 = 批次触发时间 - 请求到达时间
            long waitTime = realBatchTriggerTime - requestArrivalTime;
            waitTime = Math.max(0, Math.min(waitTime, maxWaitTimeMs));

            response.waitTimeMs = waitTime;
            response.batchProcessTimeMs = totalProcessTime;
            response.totalLatencyMs = waitTime + (long)avgProcessTimePerRequest;

            // 输出响应
            out.collect(response);
        }

        logger.info("✅ GPU {} 批次#{} 输出{}个响应", gpuId, currentBatchNum, batchSize);
    }

    @Override
    public void close() throws Exception {
        logger.info("关闭 GPU {} 服务...", gpuId);

        try {
            if (processInput != null) {
                ShutdownCommand shutdownCmd = new ShutdownCommand();
                String shutdownJson = objectMapper.writeValueAsString(shutdownCmd);
                processInput.write(shutdownJson + "\n");
                processInput.flush();
                processInput.close();
            }

            if (processOutput != null) {
                processOutput.close();
            }

            if (inferenceProcess != null && inferenceProcess.isAlive()) {
                if (!inferenceProcess.waitFor(5, TimeUnit.SECONDS)) {
                    inferenceProcess.destroyForcibly();
                }
            }
        } catch (Exception e) {
            logger.error("关闭GPU服务出错: {}", e.getMessage());
        }

        logger.info("✅ GPU {} 服务已关闭", gpuId);
        super.close();
    }

    // 数据结构类
    private static class BatchRequestData {
        public List<SingleRequestData> requests;
        public int batch_size;
        public String batch_id;
    }

    private static class SingleRequestData {
        public String user_message;
        public int max_tokens;
        public String request_id;
    }

    private static class BatchResponseData {
        public List<SingleResponseData> responses;
        public int batch_size;
        public String batch_id;
        public long total_inference_time_ms;
        public boolean success;
        public String error;
        public long timestamp;
    }

    private static class SingleResponseData {
        public String response;
        public double inference_time_ms;
        public String request_id;
        public boolean success;
        public long timestamp;
    }

    private static class ShutdownCommand {
        public String command = "shutdown";
    }
}