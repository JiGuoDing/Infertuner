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
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 批处理器 - 不使用Flink State，避免keyBy问题
 * 使用内存缓冲实现攒批功能
 */
public class ParallelBatchProcessor extends ProcessFunction<InferenceRequest, InferenceResponse> {

    private static final Logger logger = LoggerFactory.getLogger(ParallelBatchProcessor.class);

    // GPU相关
    private int gpuId;
    private String nodeIP;
    private int taskIndex;
    private int totalParallelism;
    private transient Process inferenceProcess;  // 标记为transient
    private transient BufferedWriter processInput;  // 标记为transient
    private transient BufferedReader processOutput;  // 标记为transient
    private transient ObjectMapper objectMapper;  // 标记为transient

    // 攒批配置
    private int targetBatchSize = 4;

    // 内存缓冲区- 在open()中初始化
    private transient Queue<InferenceRequest> requestBuffer;
    private transient Queue<Long> arrivalTimes;
    private transient long firstRequestTime = 0;
    private transient int batchCounter = 0;

    private static final String MODEL_NAME = "Qwen3-30B-A3B-Instruct";
    private static final String MODEL_PATH = "/mnt/tidal-alsh01/usr/suqian/models/".concat(MODEL_NAME);
    private static final String BATCH_SERVICE_SCRIPT = "/mnt/tidal-alsh01/usr/suqian/scripts/batch_inference_service.py";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 初始化transient字段
        requestBuffer = new ConcurrentLinkedQueue<>();
        arrivalTimes = new ConcurrentLinkedQueue<>();
        objectMapper = new ObjectMapper();

        taskIndex = getRuntimeContext().getIndexOfThisSubtask();
        totalParallelism = getRuntimeContext().getNumberOfParallelSubtasks();
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
        } catch (Exception e) {
            logger.warn("使用默认配置: batchSize={}", targetBatchSize);
        }

        logger.info("🎯 节点 {} 简化并行攒批处理器启动: 并行度={}, 批大小={}",
                nodeIP, totalParallelism, targetBatchSize);
        logger.info("📋 节点 {} 负责处理: taskIndex={}, 使用内存缓冲区", nodeIP, taskIndex);

        objectMapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // 启动内联推理服务进程
        startGPUService();

        logger.info("✅ 节点 {} 简化并行攒批服务启动完成", nodeIP);
    }

    private void startGPUService() throws Exception {
        logger.info("启动节点 {} 推理服务...", nodeIP);

        ProcessBuilder pb = new ProcessBuilder(
                "/opt/conda/envs/vllm-env/bin/python", BATCH_SERVICE_SCRIPT, nodeIP, MODEL_PATH, String.valueOf(gpuId));
        pb.redirectErrorStream(false);
        inferenceProcess = pb.start();

        processInput = new BufferedWriter(new OutputStreamWriter(inferenceProcess.getOutputStream()));
        processOutput = new BufferedReader(new InputStreamReader(inferenceProcess.getInputStream()));

        // 等待服务启动
        logger.info("等待节点 {} 服务启动...", nodeIP);
        Thread.sleep(3000);

        if (!inferenceProcess.isAlive()) {
            throw new RuntimeException("节点 " + nodeIP + " 推理服务启动失败");
        }

        logger.info("✅ 节点 {} 推理服务启动成功", nodeIP);
    }

    @Override
    public void processElement(InferenceRequest request, Context ctx, Collector<InferenceResponse> out) throws Exception {
        long arrivalTime = request.timestamp;

        // 将请求加入缓冲区
        requestBuffer.offer(request);
        arrivalTimes.offer(arrivalTime);

        int currentSize = requestBuffer.size();

        // 记录批次中第一个请求时间
        if (currentSize == 1) {
            firstRequestTime = arrivalTime;
            logger.info("节点 {} 开始第 {} 次攒批", nodeIP, batchCounter);
        } else {
            logger.info("节点 {} 接收到请求，当前请求数: {} / {}", nodeIP, currentSize, targetBatchSize);
        }

        // 修复：检查是否攒够了批次
        if (currentSize >= targetBatchSize) {
            logger.info("🚀 节点 {} 攒够 {} 个请求，第 {} 个批次开始处理", nodeIP, targetBatchSize, batchCounter);
            processBatch(out);
        }
    }

    private void processBatch(Collector<InferenceResponse> out) throws Exception {
        List<InferenceRequest> batch = new ArrayList<>();
        List<Long> batchArrivalTimes = new ArrayList<>();

        // 提取批次请求
        for (int i = 0; i < targetBatchSize; i++) {
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
        long batchTriggerTime = batchArrivalTimes.get(targetBatchSize-1);

        // 构建批次请求
        BatchRequestData batchRequest = new BatchRequestData();
        batchRequest.requests = new ArrayList<>();

        for (InferenceRequest req : batch) {
            SingleRequestData singleReq = new SingleRequestData();
            singleReq.user_message = req.userMessage;
            singleReq.userId = req.getUserId();
            singleReq.max_tokens = req.maxTokens;
            singleReq.request_id = req.requestId;
            batchRequest.requests.add(singleReq);
        }

        batchRequest.batch_size = batchSize;
        batchRequest.batch_id = String.format("node-%s_batch_%d_%d", nodeIP, currentBatchNum, batchTriggerTime);

        String requestJson = objectMapper.writeValueAsString(batchRequest);
        processInput.write(requestJson + "\n");
        processInput.flush();

        long batchStartTime = System.currentTimeMillis();

        // 从推理进程获取响应
        String responseJson = processOutput.readLine();
        if (responseJson == null) {
            throw new RuntimeException("节点 " + nodeIP + " 无响应");
        }

        BatchResponseData batchResponse = objectMapper.readValue(responseJson, BatchResponseData.class);

        if (!batchResponse.success) {
            throw new RuntimeException("节点 " + nodeIP + " 处理失败: " + batchResponse.error);
        }

        long batchEndTime = System.currentTimeMillis();
        long batchProcessTime = batchEndTime - batchStartTime;
        double avgProcessTimePerRequest = (double) batchProcessTime / batchSize;

        logger.info("📊 节点 {} 批次#{} 完成: 总时间={}ms, 平均={}ms/req",
                nodeIP, currentBatchNum, batchProcessTime, String.format("%.4f", avgProcessTimePerRequest));

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
            response.modelName = String.format("node-%s", nodeIP);
            response.fromCache = false;
            response.batchSize = batchSize;
            response.timestamp = batchEndTime;

            // 🔧 批次触发时间计算等待时间
            // 等待时间 = 批次触发时间 - 请求到达时间
            long waitTime = batchTriggerTime - requestArrivalTime;
            waitTime = Math.max(0, waitTime);

            response.waitTimeMs = waitTime;
            response.batchProcessTimeMs = batchProcessTime;
            response.totalLatencyMs = waitTime + (long)avgProcessTimePerRequest;

            // 输出响应
            out.collect(response);
        }

        logger.info("✅ 节点 {} 批次#{} 输出{}个响应", nodeIP, currentBatchNum, batchSize);
    }

    @Override
    public void close() throws Exception {
        logger.info("关闭节点 {} 服务...", nodeIP);

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
            logger.error("关闭节点 -{} 服务出错: {}", nodeIP, e.getMessage());
        }

        logger.info("✅ 节点 -{} 服务已关闭", nodeIP);
        super.close();
    }

    // 数据结构类
    private static class BatchRequestData {
        public List<SingleRequestData> requests;
        public int batch_size;
        public String batch_id;
    }

    private static class SingleRequestData {
        public String userId;
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