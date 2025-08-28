package com.infertuner.processors;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.infertuner.models.InferenceRequest;

/**
 * 用于接收请求并根据请求内容预测推理时间的算子
 */
public class PredictingInferenceTimeProcessor extends RichFlatMapFunction<InferenceRequest, InferenceRequest> {

    private static final Logger logger = LoggerFactory.getLogger(PredictingInferenceTimeProcessor.class);

    // 推理进程有关变量
    private transient Process predictingInferenceTimeProcess;
    private transient BufferedWriter pythonInput;
    private transient BufferedReader pythonOutput;
    private transient ObjectMapper objectMapper;

    // Flink 托管状态(ListState, ValueState等)不需要序列化到算子对象，是由 Flink 运行时管理的
    private transient ListState<InferenceRequest> requestState;
    // 一次预测的请求条数
    private transient int predictingBatchsize = 4;
    // 当前算子所在节点的 IP
    private transient String nodeIP;
    // 当前算子已处理的批次数
    private transient int batchCounter = 0;

    private static final String PREDICTING_INFERENCE_TIME_SCRIPT = "/mnt/tidal-alsh01/usr/suqian/scripts/predict_inference_time.py";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 获取的当前节点IP
        try {
            nodeIP = java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.error("获取当前节点IP失败", e);
            nodeIP = "Unknown-hostIP";
        }

        // 初始化状态缓存队列
        ListStateDescriptor<InferenceRequest> descriptor = new ListStateDescriptor<>("request-buffer",
                InferenceRequest.class);
        requestState = getRuntimeContext().getListState(descriptor);

        // 从全局参数获取 batchsize 参数
        try {
            Map<String, String> globalParams = getRuntimeContext().getExecutionConfig().getGlobalJobParameters()
                    .toMap();
            if (globalParams.containsKey("batchsize")) {
                predictingBatchsize = Integer.parseInt(globalParams.get("batchsize"));
            }
        } catch (Exception ignored) {
        }

        // 创建 Python 预测推理时间进程
        ProcessBuilder predictingInferenceTimeProcessBuilder = new ProcessBuilder("/opt/conda/envs/vllm-env/bin/python",
                PREDICTING_INFERENCE_TIME_SCRIPT, nodeIP);
        // 决定是否将子进程的标准错误输出重定向到标准输出 false -> 保持分离(默认)，stdout 与 stderr 各自独立
        predictingInferenceTimeProcessBuilder.redirectErrorStream(false);
        // 当运行 Python 脚本时，强制 Python 的标准输出和标准错误不使用缓冲，输出会实时刷新，而不是等到缓冲区满或程序结束才输出
        predictingInferenceTimeProcessBuilder.environment().put("PYTHONUNBUFFERED", "1");

        predictingInferenceTimeProcess = predictingInferenceTimeProcessBuilder.start();
        ;
        pythonInput = new BufferedWriter(new OutputStreamWriter(predictingInferenceTimeProcess.getOutputStream()));
        pythonOutput = new BufferedReader(new InputStreamReader(predictingInferenceTimeProcess.getInputStream()));

        // 启动 Python 预测推理时间进程
        long predictingInferenceTimeServiceStartTime = System.currentTimeMillis();
        startPredictingInferenceTimeService();
        long predictingInferenceTimeServiceEndTime = System.currentTimeMillis();
        logger.info("节点 {} 推理时间预测服务启动耗时 {}ms", nodeIP,
                (predictingInferenceTimeServiceEndTime - predictingInferenceTimeServiceStartTime));

        // 初始化 JSON 处理器
        objectMapper = new ObjectMapper();
    }

    @Override
    public void flatMap(InferenceRequest request, Collector<InferenceRequest> out) throws Exception {
        request.setAcceptedTimestamp(System.currentTimeMillis());
        requestState.add(request);

        List<InferenceRequest> currentRequestBatch = new ArrayList<>();
        for (InferenceRequest req : requestState.get()) {
            currentRequestBatch.add(req);
        }

        // 如果攒够了 batchsize 个请求，进行批处理预测
        if (currentRequestBatch.size() >= predictingBatchsize) {
            logger.info("节点 {} 开始第 {} 次批处理", nodeIP, batchCounter++);

            // 清空状态，准备接收下一批请求
            requestState.clear();
            for (InferenceRequest req : currentRequestBatch) {
                req.setProcessingTimestamp(System.currentTimeMillis());
            }

            long batchStartTime = System.currentTimeMillis();
            processBatch(currentRequestBatch, out);
            long batchEndTime = System.currentTimeMillis();
            logger.info("节点 {} 第 {} 次批预测完成, 批预测耗时 {}ms", nodeIP, batchCounter - 1, (batchEndTime - batchStartTime));
        }
    }

    public void startPredictingInferenceTimeService() throws IOException, InterruptedException {
        // 等待 Python 进程启动完成
        boolean processReady = false;
        long startTime = System.currentTimeMillis();
        String line;
        // 30s 超时
        while ((System.currentTimeMillis() - startTime) < 30000) {
            // 接收 Python 进程就绪信号
            if ((line = pythonOutput.readLine()) != null) {
                if (line.contains("READY")) {
                    processReady = true;
                    break;
                }
            } else {
                // 睡眠 100ms 避免忙等待
                Thread.sleep(150);
            }
        }

        if (!processReady) {
            throw new RuntimeException("推理时间预测服务在 30s 内启动失败");
        } else {
            logger.info("推理时间预测服务启动成功");
        }
    }

    /*
     * 批量对请求进行推理时间预测
     */
    private void processBatch(List<InferenceRequest> requestBatch, Collector<InferenceRequest> out)
            throws IOException {
        List<RequestData> batchReqData = new ArrayList<>();
        for (InferenceRequest req : requestBatch) {
            RequestData reqData = new RequestData(req.getRequestId(), req.getUserId(), req.getUserMessage(), req.getLlmModel().getModelName(), predictingBatchsize);
            batchReqData.add(reqData);
        }

        // 将请求写到 Python 进程的标准输入
        String batchReqString = objectMapper.writeValueAsString(batchReqData);
        pythonInput.write(batchReqString + "\n");
        pythonInput.flush();

        // 从 Python 进程的标准输出读取带有预测生成token数及推理时间的推理请求
        String batchPredictedRequestString = pythonOutput.readLine();
        if (batchPredictedRequestString == null) {
            throw new RuntimeException("节点 " + nodeIP + " 无响应");
        }

        List<RequestData> batchPredictedRequestData = objectMapper.readValue(batchPredictedRequestString,
                new TypeReference<>() {});

        // 提取响应信息，构造完整的处理后的推理请求
        for (int i = 0; i < batchPredictedRequestData.size(); i++) {
            // 在原请求基础上添加预测信息
            requestBatch.get(i).setPredictedGeneratedTokenNum(batchPredictedRequestData.get(i).getPredictedGeneratedTokenNum());
            requestBatch.get(i).setPredictedInferenceTime(batchPredictedRequestData.get(i).getPredictedInferenceTime());

            out.collect(requestBatch.get(i));
        }

    }

    private class RequestData {
        String requestId;
        String userId;
        String userMessage;
        String llmModelName;
        int batchsize;
        long predictedGeneratedTokenNum;
        double predictedInferenceTime;
        boolean success;
        
        RequestData() {};

        RequestData(String requestId, String userId, String userMessage, String llmModelName, int batchsize) {
            this.requestId = requestId;
            this.userId = userId;
            this.userMessage = userMessage;
            this.llmModelName = llmModelName;
            this.predictedGeneratedTokenNum = 0;
            this.predictedInferenceTime = 0.0;
            this.batchsize = batchsize;
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
}
