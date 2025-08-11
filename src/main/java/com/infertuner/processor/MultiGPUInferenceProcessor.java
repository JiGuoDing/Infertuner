package com.infertuner.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.infertuner.model.InferenceRequest;
import com.infertuner.model.InferenceResponse;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.TimeUnit;

public class MultiGPUInferenceProcessor extends RichMapFunction<InferenceRequest, InferenceResponse> {
    
    private static final Logger logger = LoggerFactory.getLogger(MultiGPUInferenceProcessor.class);
    
    private int gpuId;
    private int taskIndex;
    private Process inferenceProcess;
    private BufferedWriter processInput;
    private BufferedReader processOutput;
    private ObjectMapper objectMapper;
    
    private static final String MODEL_PATH = "/workspace/models/Qwen1.5-1.8B-Chat";
    private static final String SERVICE_SCRIPT = "/workspace/infertuner-simple/scripts/simple_inference_service.py";
    private static final int MAX_GPUS = 4;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        taskIndex = getRuntimeContext().getIndexOfThisSubtask();
        gpuId = taskIndex % MAX_GPUS;
        
        logger.info("Task {} 启动，绑定到 GPU {}", taskIndex, gpuId);
        
        objectMapper = new ObjectMapper();
        startInferenceService();
        
        logger.info("GPU {} 推理服务启动完成", gpuId);
    }
    
    private void startInferenceService() throws Exception {
        logger.info("启动 GPU {} 推理服务...", gpuId);
        
        ProcessBuilder pb = new ProcessBuilder(
            "python3", SERVICE_SCRIPT, MODEL_PATH, String.valueOf(gpuId)
        );
        
        pb.redirectErrorStream(false);
        inferenceProcess = pb.start();
        
        processInput = new BufferedWriter(new OutputStreamWriter(inferenceProcess.getOutputStream()));
        processOutput = new BufferedReader(new InputStreamReader(inferenceProcess.getInputStream()));
        
        logger.info("等待 GPU {} 服务初始化...", gpuId);
        Thread.sleep(8000);
        
        if (!inferenceProcess.isAlive()) {
            throw new RuntimeException("GPU " + gpuId + " 推理服务启动失败");
        }
        
        logger.info("✅ GPU {} 推理服务启动成功", gpuId);
    }
    
    @Override
    public InferenceResponse map(InferenceRequest request) throws Exception {
        long startTime = System.currentTimeMillis();
        
        InferenceResponse response = new InferenceResponse();
        response.requestId = request.requestId;
        response.userId = request.userId;
        response.userMessage = request.userMessage;
        response.batchSize = request.batchSize; // 传递批大小信息
        response.timestamp = System.currentTimeMillis();
        
        try {
            RequestData requestData = new RequestData(
                request.userMessage,
                request.maxTokens,
                request.requestId,
                request.batchSize  // 传递批大小给Python服务
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
            response.modelName = responseData.model_name + String.format(" (GPU-%d,Batch-%d)", gpuId, request.batchSize);
            response.fromCache = false;
            
            // 设置批处理相关信息
            response.waitTimeMs = 0; // 简单处理器没有等待时间
            response.batchProcessTimeMs = (long)response.inferenceTimeMs;
            response.totalLatencyMs = (long)response.inferenceTimeMs;
            
            logger.debug("GPU {} 处理请求 {} 完成，耗时 {:.2f}ms，批大小: {}", 
                        gpuId, request.requestId, response.inferenceTimeMs, request.batchSize);
            
        } catch (Exception e) {
            logger.error("GPU {} 处理请求失败: {}", gpuId, e.getMessage(), e);
            
            response.success = false;
            response.aiResponse = "GPU " + gpuId + " 处理失败: " + e.getMessage();
            response.inferenceTimeMs = System.currentTimeMillis() - startTime;
            response.modelName = "Error-GPU-" + gpuId;
        }
        
        return response;
    }
    
    @Override
    public void close() throws Exception {
        logger.info("关闭 GPU {} 推理服务...", gpuId);
        
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
        
        logger.info("✅ GPU {} 推理服务已关闭", gpuId);
        super.close();
    }
    
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