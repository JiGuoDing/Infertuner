package com.infertuner.processors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.infertuner.models.InferenceRequest;
import com.infertuner.models.InferenceResponse;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 批处理器 - 不使用Flink State，避免keyBy问题
 * 使用内存缓冲实现攒批功能
 */
public class ParallelBatchProcessor extends ProcessFunction<InferenceRequest, InferenceResponse> {

    private static final Logger logger = LoggerFactory.getLogger(ParallelBatchProcessor.class);

    // GPU相关
    private String nodeIP;
    private int taskIndex;
    private int totalParallelism;
    private transient Process inferenceProcess; // 标记为transient
    private transient BufferedWriter processInput; // 标记为transient
    private transient BufferedReader processOutput; // 标记为transient
    private transient ObjectMapper objectMapper; // 标记为transient
    private transient ListState<InferenceRequest> requestState; // Flink 托管状态，储存请求队列

    // 攒批配置
    private int inferenceBatchsize;
    private transient int batchCounter = 0;

    private static final String MODEL_NAME = "Falcon3-7B-Instruct";
    private static final String MODEL_PATH = "/mnt/tidal-alsh01/usr/suqian/models/".concat(MODEL_NAME);
    private static final String BATCH_SERVICE_SCRIPT = "/mnt/tidal-alsh01/usr/suqian/scripts/batch_inference_service_new.py";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 初始化状态存储
        ListStateDescriptor<InferenceRequest> descriptor = new ListStateDescriptor<>("processed-request-buffer", InferenceRequest.class);
        requestState = getRuntimeContext().getListState(descriptor);

        objectMapper = new ObjectMapper();

        // 获取的当前节点IP
        try {
            nodeIP = java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.error("获取当前节点IP失败", e);
            nodeIP = "Unknown-hostIP";
        }

        // 从全局参数获取 batchsize 参数
        try {
            Map<String, String> globalParams = getRuntimeContext().getExecutionConfig().getGlobalJobParameters()
                    .toMap();
            if (globalParams.containsKey("batchsize")) {
                inferenceBatchsize = Integer.parseInt(globalParams.get("batchsize"));
            }
        } catch (Exception ignored) {
        }

        objectMapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // 启动内联推理服务进程
        startGPUService();

        logger.info("✅ 节点 {} 攒批推理服务启动完成", nodeIP);
    }

    private void startGPUService() throws Exception {
        logger.info("启动节点 {} 推理服务...", nodeIP);

        ProcessBuilder pb = new ProcessBuilder(
                "/opt/conda/envs/vllm-env/bin/python", BATCH_SERVICE_SCRIPT, nodeIP, MODEL_PATH);
        pb.redirectErrorStream(false);
        pb.environment().put("PYTHONUNBUFFERED", "1");
        inferenceProcess = pb.start();

        processInput = new BufferedWriter(new OutputStreamWriter(inferenceProcess.getOutputStream()));
        processOutput = new BufferedReader(new InputStreamReader(inferenceProcess.getInputStream()));

        // 等待服务启动
        logger.info("等待节点 {} 服务启动...", nodeIP);
        Thread.sleep(5000);

        if (!inferenceProcess.isAlive()) {
            throw new RuntimeException("节点 " + nodeIP + " 推理服务启动失败");
        }

        logger.info("✅ 节点 {} Python 推理进程启动成功", nodeIP);
    }

    @Override
    public void processElement(InferenceRequest request, Context ctx, Collector<InferenceResponse> out)
            throws Exception {
        // 更新请求的被接受时间
        request.setAcceptedTimestamp(System.currentTimeMillis());

        List<InferenceRequest> currentRequestBatch = new ArrayList<>();
        for (InferenceRequest req : requestState.get()) {
            currentRequestBatch.add(req);
        }

        logger.info("节点 {} 批次 {} 攒批进度 {}/{}", nodeIP, batchCounter, currentRequestBatch.size(), inferenceBatchsize);

        if (currentRequestBatch.size() >= inferenceBatchsize) {
            logger.info("🚀 节点 {} 攒够 {} 个请求，第 {} 个批次开始处理", nodeIP, inferenceProcess, batchCounter);
            // 清空状态，准备接收下一批请求
            requestState.clear();

            for (InferenceRequest req : currentRequestBatch) {
                req.setProcessingTimestamp(System.currentTimeMillis());
            }

            // 进入批次推理服务
            long batchStartTime = System.currentTimeMillis();
            processBatch(out, currentRequestBatch);
            long batchEndTime = System.currentTimeMillis();

            logger.info("节点 {} 第 {} 个批次处理完成，耗时 {}ms", nodeIP, batchCounter - 1, batchEndTime-batchStartTime);
        }
    }

    private void processBatch(Collector<InferenceResponse> out, List<InferenceRequest> requestBatch) throws Exception {
        if (requestBatch.isEmpty()) {
            return;
        }
        // 当不考虑超时时，批次触发时间即最后一个请求的处理时间
        long batchTriggerTime = requestBatch.get(inferenceBatchsize - 1).getAcceptedTimestamp();

        List<RequestData> batchReqData = new ArrayList<>();
        for (InferenceRequest req : requestBatch) {
            // 构造单条请求体
            RequestData reaData = new RequestData(req.getRequestId(), req.getUserId(), req.getUserMessage(), req.getLlmModel().getModelName());
            batchReqData.add(reaData);
        }

        // 将请求发送到 Python 推理进程
        String requestJson = objectMapper.writeValueAsString(batchReqData);
        processInput.write(requestJson + "\n");
        processInput.flush();

        // 从 Python 推理进程获取响应
        String batchResponseJson = processOutput.readLine();
        if (batchResponseJson == null) {
            throw new RuntimeException("节点 " + nodeIP + " 无响应");
        }

        // 构造响应体，解析 Python 推理服务的响应内容
        String batchResponseString = processOutput.readLine();
        if (batchResponseString == null) {
            throw new RuntimeException("节点 " + nodeIP + " 无响应");
        }

        List<responseData> batchRespData = objectMapper.readValue(batchResponseString, new TypeReference<>() {});

        // 从响应中获取数据
        for (int i = 0; i < requestBatch.size(); i++) {
            InferenceRequest originalRequest = requestBatch.get(i);
            responseData responseData = batchRespData.get(i);

            InferenceResponse response = new InferenceResponse();
            response.setRequestId(originalRequest.getRequestId());
            response.setUserId(originalRequest.getUserId());
            response.setUserMessage(originalRequest.getUserMessage());
            response.setResponseText(responseData.getResponse());
            response.setInferenceTimeMs(responseData.getInferenceTime());
            response.setSuccess(responseData.isSuccess());
            response.setNodeIP(nodeIP);
            response.setFromCache(false);
            response.setBatchSize(originalRequest.getBatchSize());
            response.setRequestAcceptedTime(originalRequest.getAcceptedTimestamp());
            response.setWaitTimeMs(batchTriggerTime - originalRequest.getAcceptedTimestamp());
            response.setTotalLatencyMs(response.getWaitTimeMs() + response.getInferenceTimeMs());

            logger.info("请求 {} 处理完成，等待 {} 毫秒，推理 {} 毫秒，总耗时 {} 毫秒", response.requestId, response.waitTimeMs,
                    response.inferenceTimeMs, response.totalLatencyMs);
            // 输出响应
            out.collect(response);
        }
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

    private static class RequestData {
        String requestId;
        String userId;
        String userMessage;
        String llmModelName;
        int batchsize;
        long predictedGeneratedTokenNum;
        double predictedInferenceTime;
        boolean success;

        RequestData() {};

        RequestData(String requestId, String userId, String userMessage, String llmModelName) {
            this.requestId = requestId;
            this.userId = userId;
            this.userMessage = userMessage;
            this.llmModelName = llmModelName;
            this.predictedGeneratedTokenNum = 0;
            this.predictedInferenceTime = 0.0;
            this.success = false;
        }


        RequestData(String requestId, String userId, String userMessage, String llmModelName, long predictedGeneratedTokenNum, double predictedInferenceTime, boolean success) {
            this.requestId = requestId;
            this.userId = userId;
            this.userMessage = userMessage;
            this.llmModelName = llmModelName;
            this.predictedGeneratedTokenNum = predictedGeneratedTokenNum;
            this.predictedInferenceTime = predictedInferenceTime;
            this.success = success;
        }

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }

        public String getRequestId() {
            return requestId;
        }

        public void setRequestId(String requestId) {
            this.requestId = requestId;
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public String getUserMessage() {
            return userMessage;
        }

        public void setUserMessage(String userMessage) {
            this.userMessage = userMessage;
        }

        public long getPredictedGeneratedTokenNum() {
            return predictedGeneratedTokenNum;
        }

        public void setPredictedGeneratedTokenNum(long predictedGeneratedTokenNum) {
            this.predictedGeneratedTokenNum = predictedGeneratedTokenNum;
        }

        public double getPredictedInferenceTime() {
            return predictedInferenceTime;
        }

        public void setPredictedInferenceTime(double predictedInferenceTime) {
            this.predictedInferenceTime = predictedInferenceTime;
        }

        public String getLlmModelName() {
            return llmModelName;
        }

        public void setLlmModelName(String llmModelName) {
            this.llmModelName = llmModelName;
        }

        public int getBatchsize() {
            return batchsize;
        }

        public void setBatchsize(int batchsize) {
            this.batchsize = batchsize;
        }
    }

    private static class responseData {
        String requestID;
        String response;
        boolean success;
        long inferenceTime;
        int generatedTokenNum;

        public responseData(String response, boolean success, long inferenceTime, int generatedTokenNum, String requestID) {
            this.response = response;
            this.success = success;
            this.inferenceTime = inferenceTime;
            this.generatedTokenNum = generatedTokenNum;
            this.requestID = requestID;
        }

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }

        public String getResponse() {
            return response;
        }

        public void setResponse(String response) {
            this.response = response;
        }

        public String getRequestID() {
            return requestID;
        }

        public void setRequestID(String requestID) {
            this.requestID = requestID;
        }

        public int getGeneratedTokenNum() {
            return generatedTokenNum;
        }

        public void setGeneratedTokenNum(int generatedTokenNum) {
            this.generatedTokenNum = generatedTokenNum;
        }

        public long getInferenceTime() {
            return inferenceTime;
        }

        public void setInferenceTime(long inferenceTime) {
            this.inferenceTime = inferenceTime;
        }
    }

    private static class ShutdownCommand {
        public String command = "shutdown";
    }
}