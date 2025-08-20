package com.infertuner.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.infertuner.models.InferenceRequest;
import com.infertuner.models.InferenceResponse;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 简化的ProcessFunction攒批处理器，基于 Flink KeyedProcessFunction 实现。
 */
public class KeyedProcessFunctionBatchProcessor extends KeyedProcessFunction<String, InferenceRequest, InferenceResponse> {

    private static final Logger logger = LoggerFactory.getLogger(KeyedProcessFunctionBatchProcessor.class);


    // GPU相关
    private int gpuId;
    private int taskIndex;
    private String nodeIP;
    private Process inferenceProcess;
    private BufferedWriter processInput;
    private BufferedReader processOutput;
    private ObjectMapper objectMapper;

    // 攒批配置
    private int targetBatchSize = 4;
    private long maxWaitTimeMs = 2000;

    // Flink State
    private transient ListState<InferenceRequest> requestBuffer;
    private transient ListState<Long> arrivalTimes;
    private transient ValueState<Long> firstRequestTime;
    private transient ValueState<Integer> batchCounter;

    private static final String MODEL_NAME = "Qwen3-30B-A3B-Instruct";
    private static final String MODEL_PATH = "/mnt/tidal-alsh01/usr/suqian/models/".concat(MODEL_NAME);
    private static final String BATCH_SERVICE_SCRIPT = "/mnt/tidal-alsh01/usr/suqian/scripts/batch_inference_service.py";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        taskIndex = getRuntimeContext().getIndexOfThisSubtask();
        gpuId = 0;

        // 获取的当前节点IP
        try {
            nodeIP = java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.error("获取当前节点IP失败", e);
            nodeIP = "Unknown-hostIP";
        }

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

        // 初始化State
        requestBuffer = getRuntimeContext().getListState(
                new ListStateDescriptor<>("request-buffer", InferenceRequest.class));
        arrivalTimes = getRuntimeContext().getListState(
                new ListStateDescriptor<>("arrival-times", Long.class));
        firstRequestTime = getRuntimeContext().getState(
                new ValueStateDescriptor<>("first-request-time", Long.class));
        batchCounter = getRuntimeContext().getState(
                new ValueStateDescriptor<>("batch-counter", Integer.class));

        //

        logger.info("🎯 Node {} 攒批处理器启动: 批大小={}, 超时={}ms", nodeIP, targetBatchSize, maxWaitTimeMs);

        objectMapper = new ObjectMapper();
        objectMapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // 启动GPU服务
        startGPUService();

        logger.info("✅ 节点 {} 攒批服务启动完成", nodeIP);
    }

    private void startGPUService() throws Exception {
        logger.info("启动节点 {} 推理服务...", nodeIP);

        ProcessBuilder pb = new ProcessBuilder(
                "/opt/conda/envs/vllm-env/bin/python", BATCH_SERVICE_SCRIPT, nodeIP, MODEL_PATH, String.valueOf(gpuId));
        pb.redirectErrorStream(false);
        inferenceProcess = pb.start();

        processInput = new BufferedWriter(new OutputStreamWriter(inferenceProcess.getOutputStream()));
        processOutput = new BufferedReader(new InputStreamReader(inferenceProcess.getInputStream()));

        // 简单等待3秒让服务启动
        logger.info("等待节点 {} 服务启动...", nodeIP);
        Thread.sleep(3000);

        if (!inferenceProcess.isAlive()) {
            throw new RuntimeException("节点 " + nodeIP + " 推理服务启动失败");
        }

        logger.info("✅ 节点 {} 推理服务启动成功", nodeIP);
    }

    @Override
    public void processElement(InferenceRequest request, Context ctx, Collector<InferenceResponse> out) throws Exception {
        /*
        查看 key
         */
        String currentKey = ctx.getCurrentKey();
        logger.info("处理请求 {}，当前Key: {}", request.requestId, currentKey);

        long arrivalTime = request.timestamp;
        long currentTime = System.currentTimeMillis();

        // 如果批次计数器状态未初始化
        if (batchCounter.value() == null) {
            batchCounter.update(0);
        }

        // 将请求加入缓冲区
        requestBuffer.add(request);
        arrivalTimes.add(arrivalTime);

        // 将状态中的请求和到达时间取回到本地列表
        List<InferenceRequest> currentRequests = new ArrayList<>();
        List<Long> currentArrivalTimes = new ArrayList<>();

        for (InferenceRequest req : requestBuffer.get()) {
            currentRequests.add(req);
            logger.info("requestId = {}", req.requestId);
        }
        for (Long time : arrivalTimes.get()) {
            currentArrivalTimes.add(time);
        }

        // 当前队列中请求的数量
        int currentRequestSize = currentRequests.size();

        // 如果是批次的第一个请求，设置超时Timer
        if (currentRequestSize == 1) {
            firstRequestTime.update(arrivalTime);
            // 设置超时定时器
            long timerTime = currentTime + maxWaitTimeMs;
            ctx.timerService().registerProcessingTimeTimer(timerTime);
            logger.info("节点 {} 开始新批次: {} (1/{})", nodeIP, request.requestId, targetBatchSize);
        } else {
            logger.info("节点 {} 累积请求: {}/{} - {}", nodeIP, currentRequestSize, targetBatchSize, request.requestId);
        }

        // 检查是否攒够了
        if (currentRequestSize >= targetBatchSize) {
            logger.info("🚀 节点 {} 攒够{}个请求，开始处理", nodeIP, targetBatchSize);
            processBatch(currentRequests, currentArrivalTimes, currentTime, out, "数量触发");
            clearBuffer();
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<InferenceResponse> out) throws Exception {
        // 超时触发
        List<InferenceRequest> currentBuffer = new ArrayList<>();
        List<Long> currentArrivalTimes = new ArrayList<>();

        for (InferenceRequest req : requestBuffer.get()) {
            currentBuffer.add(req);
        }
        for (Long time : arrivalTimes.get()) {
            currentArrivalTimes.add(time);
        }

        if (!currentBuffer.isEmpty()) {
            logger.info("⏰ 节点 {} 超时触发: {}个请求", nodeIP, currentBuffer.size());
            processBatch(currentBuffer, currentArrivalTimes, timestamp, out, "超时触发");
            clearBuffer();
        }
    }

    private void processBatch(List<InferenceRequest> batch,
                              List<Long> arrivalTimes,
                              long batchTriggerTime,
                              Collector<InferenceResponse> out,
                              String triggerReason) throws Exception {

        int batchSize = batch.size();
        int currentBatchNum = batchCounter.value() + 1;
        batchCounter.update(currentBatchNum);

        // 🔧 修复：计算正确的批次触发时间
        long realBatchTriggerTime;
        if ("数量触发".equals(triggerReason) && !arrivalTimes.isEmpty()) {
            // 数量触发：触发时间 = 最后一个请求的到达时间
            realBatchTriggerTime = arrivalTimes.get(arrivalTimes.size() - 1);
            logger.info("🔍 数量触发时间修正: 使用最后请求时间 {}", new java.util.Date(realBatchTriggerTime));
        } else {
            // 超时触发：使用当前时间
            realBatchTriggerTime = System.currentTimeMillis();
            logger.info("🔍 超时触发时间: {}", new java.util.Date(realBatchTriggerTime));
        }

        logger.info("🔥 节点 {} 批次#{} 开始: {} | {}个请求", nodeIP, currentBatchNum, triggerReason, batchSize);

        // 构建批量请求
        BatchRequestData batchRequest = new BatchRequestData();
        // batchRequest.requests = new ArrayList<>();

        for (InferenceRequest req : batch) {
            SingleRequestData singleReq = new SingleRequestData();
            singleReq.user_message = req.userMessage;
            singleReq.max_tokens = req.maxTokens;
            singleReq.request_id = req.requestId;
            batchRequest.requests.add(singleReq);
        }

        batchRequest.batch_size = batchSize;
        batchRequest.batch_id = String.format("batch_%d_%d", currentBatchNum, realBatchTriggerTime);

        // 发送到推理服务并获取响应
        long batchStartTime = System.currentTimeMillis();

        String requestJson = objectMapper.writeValueAsString(batchRequest);
        processInput.write(requestJson + "\n");
        processInput.flush();

        String responseJson = processOutput.readLine();
        if (responseJson == null) {
            throw new RuntimeException("节点 " + nodeIP + " 无响应");
        }

        /*
            result = {
                "responses": batch_responses,
                "batch_size": batch_size,
                "batch_id": batch_id,
                "total_inference_time_ms": round(total_batch_time, 2),
                "success": True,
                "timestamp": int(time.time() * 1000),
                "error": "No error"
            }
         */
        // 解析python进程返回的推理响应
        BatchResponseData batchResponse = objectMapper.readValue(responseJson, BatchResponseData.class);

        if (!batchResponse.success) {
            throw new RuntimeException("节点 " + nodeIP + " 处理失败: " + batchResponse.error);
        }

        long batchEndTime = System.currentTimeMillis();
        long totalProcessTime = batchEndTime - batchStartTime;
        double avgProcessTimePerRequest = (double) totalProcessTime / batchSize;

        logger.info("📊 节点 {} 批次#{} 完成: 总时间={}ms, 平均={}ms/req",
                nodeIP, currentBatchNum, totalProcessTime, String.format("%.1f", avgProcessTimePerRequest));

        // 生成响应并输出
        for (int i = 0; i < batch.size(); i++) {
            InferenceRequest originalReq = batch.get(i);
            SingleResponseData singleResp = batchResponse.responses.get(i);
            long requestArrivalTime = arrivalTimes.get(i);

            InferenceResponse response = new InferenceResponse();
            response.requestId = originalReq.requestId;
            response.userId = originalReq.userId;
            response.userMessage = originalReq.userMessage;
            response.aiResponse = singleResp.response;
            response.inferenceTimeMs = avgProcessTimePerRequest;
            response.success = singleResp.success;
            response.modelName = String.format("Node-%s", nodeIP);
            response.fromCache = false;
            response.batchSize = batchSize;
            response.timestamp = batchEndTime;

            // 🔧 修复：正确计算等待时间
            long waitTime;
            if ("数量触发".equals(triggerReason)) {
                // 数量触发：等待时间 = 最后一个请求到达时间 - 当前请求到达时间
                waitTime = realBatchTriggerTime - requestArrivalTime;
            } else {
                // 超时触发：等待时间 = 超时时刻 - 当前请求到达时间
                waitTime = realBatchTriggerTime - requestArrivalTime;
            }

            // 确保等待时间不为负数，且不超过合理范围
            waitTime = Math.max(0, Math.min(waitTime, maxWaitTimeMs));

            response.waitTimeMs = waitTime;
            response.batchProcessTimeMs = totalProcessTime;
            response.totalLatencyMs = waitTime + (long)avgProcessTimePerRequest;

            // if (i < 3) { // 只打印前3个请求的详细信息
            logger.info("请求{}: 到达={}, 触发={}, 等待={}ms",
                    i+1, new java.util.Date(requestArrivalTime),
                    new java.util.Date(realBatchTriggerTime), waitTime);
            // }

            // 输出响应
            out.collect(response);
        }

        logger.info("✅ 节点 {} 批次#{} 输出{}个响应", nodeIP, currentBatchNum, batchSize);
    }

    private void clearBuffer() throws Exception {
        requestBuffer.clear();
        arrivalTimes.clear();
        firstRequestTime.clear();
    }

    @Override
    public void close() throws Exception {
        logger.info("关闭 节点 {} 服务...", nodeIP);

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
            logger.error("关闭节点服务出错: {}", e.getMessage());
        }

        logger.info("✅ 节点 {} 服务已关闭", nodeIP);
        super.close();
    }

    // 数据结构
    private static class BatchRequestData {
        public List<SingleRequestData> requests;
        public int batch_size;
        public String batch_id;

        public BatchRequestData() {
            this.requests = new ArrayList<>();
        }
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