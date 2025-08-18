package com.infertuner.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.infertuner.models.InferenceRequest;
import com.infertuner.models.InferenceResponse;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 简化的真正攒批处理器
 * 通过设置不同的等待时间来模拟攒批效果
 */
public class RealBatchProcessor extends RichMapFunction<InferenceRequest, InferenceResponse> {
    
    private static final Logger logger = LoggerFactory.getLogger(RealBatchProcessor.class);
    
    private int gpuId;
    private int taskIndex;
    private String nodeIP;

    private Process inferenceProcess;
    private BufferedWriter processInput;
    private BufferedReader processOutput;
    private ObjectMapper objectMapper;
    
    // 攒批相关
    private int batchSize = 4;
    private int requestCounter = 0;
    private final List<InferenceRequest> requestQueue = new ArrayList<>();
    // 记录请求到达时间
    private final Map<String, Long> arrivalTimes = new HashMap<>();
    // 记录批次开始时间
    private final Map<Integer, Long> batchStartTime = new HashMap<>();
    // 记录批次结束时间
    private final Map<Integer, Long> batchEndTime = new HashMap<>();
    private final Map<String, InferenceResponse> responseMap = new HashMap<>();
    // 记录请求与batch映射关系
    private final Map<String, Integer> requestToBatchId = new HashMap<>();

    private volatile boolean isProcessing = false;
    private int batchCounter = 0;

    private static final String MODEL_NAME = "Qwen3-30B-A3B-Instruct";
    private static final String MODEL_PATH = "/mnt/tidal-alsh01/usr/suqian/models/".concat(MODEL_NAME);
    // 推理服务脚本路径
    private static final String SERVICE_SCRIPT = "/mnt/tidal-alsh01/usr/suqian/scripts/simple_inference_service.py";
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 获取当前子任务的编号
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
        
        // 从全局参数获取批大小值
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
        logger.info("正在启动节点 {} 的推理服务...", nodeIP);
        
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
    }
    
    @Override
    public InferenceResponse map(InferenceRequest request) throws Exception {
        final long arrivalTime = System.currentTimeMillis();
        requestCounter++;

        logger.info("节点 {} 收到请求 {} (第{}个)",
                   nodeIP, request.requestId, requestCounter);

        InferenceResponse response = new InferenceResponse();
        boolean shouldProcess = false;

        // TODO 攒批的逻辑
        synchronized (this) {
            requestQueue.add(request);
            arrivalTimes.put(request.requestId, arrivalTime);

            if (!isProcessing && requestQueue.size() >= batchSize) {
                isProcessing = true;
                shouldProcess = true;
            }

            while (responseMap.get(request.requestId) == null) {
                // 当前批次推理尚未完成，持续等待
                this.wait();
            }

            response = responseMap.remove(request.requestId);
        }

        if (shouldProcess) {
            List<InferenceRequest> batchRequestsCopy;
            int actualBatchSize;
            synchronized (this) {
                actualBatchSize = Math.min(batchSize, requestQueue.size());
                batchRequestsCopy = new ArrayList<>(requestQueue.subList(0, actualBatchSize));
                requestQueue.subList(0, actualBatchSize).clear();
            }

            long start = System.currentTimeMillis();
            int batchId = batchCounter++;
            batchStartTime.put(batchId, start);

            for (InferenceRequest req : batchRequestsCopy) {
                requestToBatchId.put(req.requestId, batchId);
            }
        }


        // 设置攒批相关信息
        response.waitTimeMs = waitTime;
        response.batchProcessTimeMs = (long)response.inferenceTimeMs;
        response.totalLatencyMs = waitTime + (long)response.inferenceTimeMs;
        
        logger.info("GPU {} 处理 {} 完成: 等待={}ms, 推理={}ms, 总延迟={}ms", 
                   gpuId, request.requestId, waitTime, (long)response.inferenceTimeMs, response.totalLatencyMs);
        
        return response;
    }

    /*
    批次推理
     */
    private  List<InferenceResponse> processBatch(List<InferenceRequest> requests) {
        List<InferenceResponse> responses = new ArrayList<>();

    }

    /*
    单条推理
     */
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
                request.maxTokens,
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
            response.modelName = responseData.model_name + String.format(" (GPU-%d,Batch-%d)", gpuId, actualBatchSize);
            response.fromCache = false;
            
        } catch (Exception e) {
            logger.error("GPU {} 推理失败: {}", gpuId, e.getMessage(), e);
            response.success = false;
            response.aiResponse = "推理失败: " + e.getMessage();
            response.inferenceTimeMs = 0;
            response.modelName = "Error-GPU-" + gpuId;
        }
        
        return response;
    }

    private InferenceResponse processBatch(int batchId){
        return null;
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