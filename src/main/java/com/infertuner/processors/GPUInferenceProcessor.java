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
import java.util.concurrent.TimeUnit;

public class GPUInferenceProcessor extends RichMapFunction<InferenceRequest, InferenceResponse> {
    
    private static final Logger logger = LoggerFactory.getLogger(GPUInferenceProcessor.class);
    
    private int gpuId;
    private int taskIndex;
    private String nodeIP;
    // 推理进程
    private Process inferenceProcess;
    private BufferedWriter processInput;
    private BufferedReader processOutput;
    private ObjectMapper objectMapper;

    // 模型路径
    private static final String MODEL_NAME = "Qwen3-30B-A3B-Instruct";
    private static final String MODEL_PATH = "/mnt/tidal-alsh01/usr/suqian/models/".concat(MODEL_NAME);
    // 推理服务脚本路径
    private static final String SERVICE_SCRIPT = "/mnt/tidal-alsh01/usr/suqian/scripts/simple_inference_service.py";
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        /*
          在Flink启动时调用
         */

        // 获取当前子任务的编号
        taskIndex = getRuntimeContext().getIndexOfThisSubtask();

        // 多机单卡固定为0
        gpuId = 0;

        // 获取的当前节点IP
        try {
            nodeIP = java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.error("获取当前节点IP失败", e);
            nodeIP = "Unknown-hostIP";
        }

        logger.info("Task {} 启动，运行节点: {}，绑定 GPU {}", taskIndex, nodeIP, gpuId);

        objectMapper = new ObjectMapper();
        startInferenceService();
    }

    /**
     * 启动Python推理服务，并通过标准输入输出流与之通信
     */
    private void startInferenceService() throws Exception {
        logger.info("正在启动节点 {} 的推理服务...", nodeIP);

        // 使用ProcessBuilder启动Python推理进程，进程输入参数为nodeIP,模型路径和GPU ID
        ProcessBuilder pb = new ProcessBuilder(
            "/opt/conda/envs/vllm-env/bin/python", SERVICE_SCRIPT, nodeIP, MODEL_PATH, String.valueOf(gpuId)
        );
        
        pb.redirectErrorStream(false);
        inferenceProcess = pb.start();

        // 初始化输入输出流，用于与Python进程通信
        // Java 进程通过processInput向Python进程的stdin发送Json格式的请求
        // getOutputStream() 方法返回一个 OutputStream，用于向进程的标准输入(stdin) 写入数据
        processInput = new BufferedWriter(new OutputStreamWriter(inferenceProcess.getOutputStream()));
        // Java 进程通过processOutput从Python进程的stdout读取Json格式的响应
        // getInputStream() 方法返回一个 InputStream，用于从进程的标准输出(stdout) 读取数据
        processOutput = new BufferedReader(new InputStreamReader(inferenceProcess.getInputStream()));

        // 等待服务初始化
        logger.info("等待节点 {} 服务初始化...", nodeIP);
        Thread.sleep(8000);

        // 检查进程是否启动成功
        if (!inferenceProcess.isAlive()) {
            throw new RuntimeException("节点 " + nodeIP + " 推理服务初始化失败");
        }
        
        logger.info("✅ 节点 {} 推理服务初始化成功", nodeIP);
    }
    
    @Override
    public InferenceResponse map(InferenceRequest request) throws Exception {
        /*
        Flink 流处理的核心，每当流里有一个 InferenceRequest 进来，就会调用一次，生成对应的InferenceResponse。
        作用：调用外部Python推理服务（通过进程管道通信），获取模型推理结果并包装返回。
         */
        long startTime = System.currentTimeMillis();

        // 创建响应对象，后续将填充推理结果
        InferenceResponse response = new InferenceResponse();
        response.requestId = request.requestId;
        response.userId = request.userId;
        response.userMessage = request.userMessage;
        // 传递批大小信息
        response.batchSize = request.batchSize;
        // 记录响应生成的时间戳
        response.timestamp = System.currentTimeMillis();

        // 构造 RequestData
        try {
            // 构造发送给 Python 服务的请求数据对象，包含推理请求的必要信息
            // TODO 确认推理服务里的batch的含义
            RequestData requestData = new RequestData(
                request.userMessage,
                request.maxTokens,
                request.requestId,
                request.batchSize  // 传递批大小给Python服务
            );

            // 将请求对象序列化为 JSON 字符串，得到的requestJson在逻辑上是一行字符串（单一String对象）
            String requestJson = objectMapper.writeValueAsString(requestData);

            // 通过标准输入流，将 JSON 请求发送给 Python推理服务
            processInput.write(requestJson + "\n");
            // 使用 flush() 保证数据立刻发出
            processInput.flush();

            // 读取 Python 服务返回的一行 JSON 响应
            String responseJson = processOutput.readLine();
            if (responseJson == null) {
                throw new RuntimeException("节点 " + nodeIP + " 服务无响应");
            }

            // 将 JSON 响应反序列化为 Java 对象
            ResponseData responseData = objectMapper.readValue(responseJson, ResponseData.class);
            
            response.success = responseData.success;
            response.aiResponse = responseData.response;
            // 记录推理耗时
            response.inferenceTimeMs = responseData.inference_time_ms;
            response.modelName = responseData.model_name + String.format(" (NodeIP-%s,Batch-%d)", nodeIP, request.batchSize);
            response.fromCache = false;

            // 设置批处理相关信息
            response.waitTimeMs = 0; // 简单处理器没有等待时间
            response.batchProcessTimeMs = (long)response.inferenceTimeMs;
            response.totalLatencyMs = (long)response.inferenceTimeMs;
            
            logger.debug("节点 {} 处理请求 {} 完成，耗时 {}ms，批大小: {}",
                        nodeIP, request.requestId, String.format("%.2f", response.inferenceTimeMs), request.batchSize);
            
        } catch (Exception e) {
            logger.error("节点 {} 处理请求失败: {}", nodeIP, e.getMessage(), e);
            
            response.success = false;
            response.aiResponse = "节点 " + nodeIP + " 处理失败: " + e.getMessage();
            response.inferenceTimeMs = System.currentTimeMillis() - startTime;
            response.modelName = "Error-Node-" + nodeIP;
        }
        return response;
    }
    
    @Override
    public void close() throws Exception {
        logger.info("关闭节点 {} 的推理服务...", nodeIP);
        
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
                    logger.warn("节点 {} 的服务未在10秒内退出，强制终止", nodeIP);
                    inferenceProcess.destroyForcibly();
                }
            }

        } catch (Exception e) {
            logger.error("关闭节点 {} 的服务时出错: {}", nodeIP, e.getMessage());
        }

        logger.info("✅ 节点 {} 的推理服务已关闭", nodeIP);
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