package com.infertuner.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.infertuner.models.InferenceRequest;
import com.infertuner.models.InferenceResponse;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 简化的真正攒批处理器
 * 通过设置不同的等待时间来模拟攒批效果
 */
public class BatchProcessor extends RichMapFunction<InferenceRequest, InferenceResponse> {

    private static final Logger logger = LoggerFactory.getLogger(BatchProcessor.class);

    private String nodeIP;
    private int gpuId;
    private int taskIndex;
    private Process inferenceProcess;
    private BufferedWriter processInput;
    private BufferedReader processOutput;
    private ObjectMapper objectMapper;

    // 攒批相关
    private int batchSize = 3;
    private final Map<Integer, List<InferenceRequest>> requestQueue = new HashMap<>();
    private final Map<Integer, Long> batchStartTime = new HashMap<>();
    private int requestCounter = 0;

    private static final String MODEL_PATH = "Qwen3-30B-A3B-Instruct";
    private static final String SERVICE_SCRIPT = "/mnt/tidal-alsh01/usr/suqian/scripts/simple_inference_service.py";
    private static final int MAX_GPUS = 4;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        taskIndex = getRuntimeContext().getIndexOfThisSubtask();
        // 多机单卡固定为0
        gpuId = 0;

        // 获取当前节点IP
        try {
            nodeIP = java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.error("获取当前节点IP失败", e);
            nodeIP = "Unknown-hostIP";
        }

        // 从全局参数获取批大小
        try {
            Map<String, String> globalParams = getRuntimeContext().getExecutionConfig()
                    .getGlobalJobParameters().toMap();
            if (globalParams.containsKey("batch.size")) {
                batchSize = Integer.parseInt(globalParams.get("batch.size"));
            }
        } catch (Exception e) {
            logger.warn("无法获取batch.size参数，使用默认值: {}", batchSize);
        }

        logger.info("Task {} 启动，绑定到节点 {}，batch_size={}", taskIndex, nodeIP, batchSize);

        objectMapper = new ObjectMapper();
        startInferenceService();

        logger.info("节点 {} 简化攒批推理服务启动完成", nodeIP);
    }

    private void startInferenceService() throws Exception {
        logger.info("启动节点 {} 推理服务...", nodeIP);

        ProcessBuilder pb = new ProcessBuilder(
                "/opt/conda/envs/vllm-env/bin/python", SERVICE_SCRIPT, nodeIP, MODEL_PATH, String.valueOf(gpuId)
        );

        pb.redirectErrorStream(false);
        inferenceProcess = pb.start();

        processInput = new BufferedWriter(new OutputStreamWriter(inferenceProcess.getOutputStream()));
        processOutput = new BufferedReader(new InputStreamReader(inferenceProcess.getInputStream()));

        logger.info("等待节点 {} 服务初始化...", nodeIP);
        Thread.sleep(8000);

        if (!inferenceProcess.isAlive()) {
            throw new RuntimeException("节点 " + nodeIP + " 推理服务启动失败");
        }

        logger.info("✅ 节点 {} 推理服务启动成功", nodeIP);
    }

    @Override
    public InferenceResponse map(InferenceRequest request) throws Exception {
        long arrivalTime = System.currentTimeMillis();
        requestCounter++;

        // 计算在批次中的位置（从1开始）
        int positionInBatch = ((requestCounter - 1) % batchSize) + 1;

        // 计算等待时间：第1个请求等待最久，最后一个请求不等待
        long waitTime = 0;
        if (batchSize > 1) {
            waitTime = (batchSize - positionInBatch) * 400;  // 每个位置差200ms
        }

        logger.info("节点 {} 收到请求 {} (第{}个，批次位置{}), 模拟等待{}ms",
                nodeIP, request.requestId, requestCounter, positionInBatch, waitTime);

        // 模拟等待时间
        if (waitTime > 0) {
            Thread.sleep(waitTime);
        }

        // 处理请求
        InferenceResponse response = processSingleRequest(request, batchSize);

        // 设置攒批相关信息
        response.waitTimeMs = waitTime;
        response.batchProcessTimeMs = (long)response.inferenceTimeMs;
        response.totalLatencyMs = waitTime + (long)response.inferenceTimeMs;

        logger.info("节点 {} 处理 {} 完成: 等待={}ms, 推理={}ms, 总延迟={}ms",
                nodeIP, request.requestId, waitTime, (long)response.inferenceTimeMs, response.totalLatencyMs);

        return response;
    }

    private InferenceResponse processSingleRequest(InferenceRequest request, int actualBatchSize) throws Exception {
        InferenceResponse response = new InferenceResponse();
        response.requestId = request.requestId;
        response.userId = request.userId;
        response.userMessage = request.userMessage;
        response.batchSize = actualBatchSize;
        response.timestamp = System.currentTimeMillis();

        try {
            RequestData requestData = new RequestData(
                    request.userMessage,
                    request.maxNewTokens,
                    request.requestId,
                    actualBatchSize
            );

            String requestJson = objectMapper.writeValueAsString(requestData);

            processInput.write(requestJson + "\n");
            processInput.flush();

            String responseJson = processOutput.readLine();
            if (responseJson == null) {
                throw new RuntimeException("GPU " + gpuId + " 服务无响应");
            }

            ResponseData responseData = objectMapper.readValue(responseJson, ResponseData.class);

            response.success = responseData.success;
            response.aiResponse = responseData.response;
            response.inferenceTimeMs = responseData.inference_time_ms;
            response.responseDescription = responseData.model_name + String.format(" (GPU-%d,Batch-%d)", gpuId, actualBatchSize);
            response.fromCache = false;

        } catch (Exception e) {
            logger.error("GPU {} 推理失败: {}", gpuId, e.getMessage(), e);
            response.success = false;
            response.aiResponse = "推理失败: " + e.getMessage();
            response.inferenceTimeMs = 0;
            response.responseDescription = "Error-GPU-" + gpuId;
        }

        return response;
    }

    @Override
    public void close() throws Exception {
        logger.info("关闭 GPU {} 简化攒批推理服务...", gpuId);

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
                if (!inferenceProcess.waitFor(10, TimeUnit.SECONDS)) {
                    logger.warn("GPU {} 服务未在10秒内退出，强制终止", gpuId);
                    inferenceProcess.destroyForcibly();
                }
            }

        } catch (Exception e) {
            logger.error("关闭 GPU {} 服务时出错: {}", gpuId, e.getMessage());
        }

        logger.info("✅ GPU {} 简化攒批推理服务已关闭", gpuId);
        super.close();
    }

    // 数据类
    private static class RequestData {
        public String user_message;
        public int max_tokens;
        public String request_id;
        public int batch_size;

        public RequestData(String userMessage, int maxTokens, String requestId, int batchSize) {
            this.user_message = userMessage;
            this.max_tokens = maxTokens;
            this.request_id = requestId;
            this.batch_size = batchSize;
        }
    }

    private static class ResponseData {
        public boolean success;
        public String response;
        public double inference_time_ms;
        public String model_name;
        public String request_id;
        public int batch_size;
        public long timestamp;
    }

    private static class ShutdownCommand {
        public String command = "shutdown";
    }
}