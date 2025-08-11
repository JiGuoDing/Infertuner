package com.infertuner.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.infertuner.model.InferenceRequest;
import com.infertuner.model.InferenceResponse;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * 精简版推理处理器 - 专注核心功能
 */
public class SimpleInferenceProcessor extends RichMapFunction<InferenceRequest, InferenceResponse> {
    
    private static final Logger logger = LoggerFactory.getLogger(SimpleInferenceProcessor.class);
    
    private transient Process pythonProcess;
    private transient BufferedWriter pythonInput;
    private transient BufferedReader pythonOutput;
    private transient ObjectMapper objectMapper;
    
    // 配置
    private static final String MODEL_PATH = "/workspace/models/Qwen1.5-1.8B-Chat";
    private static final String PYTHON_SCRIPT = "/workspace/infertuner-simple/scripts/simple_inference_service.py";
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        logger.info("启动推理服务...");
        objectMapper = new ObjectMapper();
        
        // 启动Python服务
        ProcessBuilder pb = new ProcessBuilder("python3", PYTHON_SCRIPT, MODEL_PATH);
        pb.environment().put("PYTHONUNBUFFERED", "1");
        
        pythonProcess = pb.start();
        pythonInput = new BufferedWriter(new OutputStreamWriter(pythonProcess.getOutputStream()));
        pythonOutput = new BufferedReader(new InputStreamReader(pythonProcess.getInputStream()));
        
        // 等待初始化
        Thread.sleep(5000);
        
        logger.info("推理服务已启动");
    }
    
    @Override
    public InferenceResponse map(InferenceRequest request) throws Exception {
        InferenceResponse response = new InferenceResponse();
        response.requestId = request.requestId;
        response.userId = request.userId;
        response.userMessage = request.userMessage;
        response.batchSize = request.batchSize;
        response.timestamp = System.currentTimeMillis();
        response.fromCache = false;
        
        try {
            // 构建请求
            Map<String, Object> req = new HashMap<>();
            req.put("user_message", request.userMessage);
            req.put("max_tokens", request.maxTokens);
            req.put("request_id", request.requestId);
            
            // 发送请求
            String requestJson = objectMapper.writeValueAsString(req);
            pythonInput.write(requestJson + "\n");
            pythonInput.flush();
            
            // 读取响应
            String responseJson = pythonOutput.readLine();
            JsonNode responseNode = objectMapper.readTree(responseJson);
            
            // 解析响应
            response.success = responseNode.get("success").asBoolean();
            response.aiResponse = responseNode.get("response").asText();
            response.inferenceTimeMs = responseNode.get("inference_time_ms").asDouble();
            response.modelName = responseNode.get("model_name").asText();
            
            logger.info("处理完成: {} -> {}ms", request.requestId, response.inferenceTimeMs);
            
        } catch (Exception e) {
            logger.error("处理失败: {}", e.getMessage());
            response.success = false;
            response.aiResponse = "处理失败: " + e.getMessage();
            response.inferenceTimeMs = 0.0;
            response.modelName = "Error";
        }
        
        return response;
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        
        if (pythonInput != null) {
            pythonInput.write("{\"command\": \"shutdown\"}\n");
            pythonInput.flush();
            pythonInput.close();
        }
        if (pythonOutput != null) pythonOutput.close();
        if (pythonProcess != null) pythonProcess.destroyForcibly();
        
        logger.info("推理服务已关闭");
    }
}