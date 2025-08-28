package com.infertuner.models;

/**
 * 推理请求数据模型
 */
public class InferenceRequest {
    public String requestId;
    public String userId;
    public String userMessage;
    public int maxNewTokens;

    public int batchSize;
    // 请求的创建时间戳
    public long createTimestamp;
    // 请求被接受的时间戳
    public long acceptedTimestamp;
    // 请求开始处理的时间戳
    public long processingTimestamp;
    // 请求执行完成的时间戳
    public long completedTimestamp;
    // 请求对应的 LLM
    public LLMModel llmModel;

    // 预测的请求对应的推理生成的 token 数
    public long predictedGeneratedTokenNum;
    // 预测的请求对应的推理时间
    public double predictedInferenceTime;
    
    public InferenceRequest() {}

    public InferenceRequest(String requestId, String userId, String userMessage, int maxNewTokens) {
        this.requestId = requestId;
        this.userId = userId;
        this.userMessage = userMessage;
        this.maxNewTokens = maxNewTokens;
        // batchSize默认为1
        this.batchSize = 1;
        // 请求创建的时间戳
        this.createTimestamp = System.currentTimeMillis();
        // 其余时间戳初始化为0
        this.acceptedTimestamp = -1;
        this.processingTimestamp = -1;
        this.completedTimestamp = -1;
    }

    public InferenceRequest(String requestId, String userId, String userMessage, int maxNewTokens, int batchSize) {
        this.requestId = requestId;
        this.userId = userId;
        this.userMessage = userMessage;
        this.maxNewTokens = maxNewTokens;
        this.batchSize = batchSize;
        // 请求创建的时间戳
        this.createTimestamp = System.currentTimeMillis();
        // 其余时间戳初始化为0
        this.acceptedTimestamp = -1;
        this.processingTimestamp = -1;
        this.completedTimestamp = -1;
    }

    public InferenceRequest(String requestId, String userId, String userMessage, int maxNewTokens, String llmModelName) {
        this.requestId = requestId;
        this.userId = userId;
        this.userMessage = userMessage;
        this.maxNewTokens = maxNewTokens;
        // batchSize默认为1
        this.batchSize = 1;
        // 请求创建的时间戳
        this.createTimestamp = System.currentTimeMillis();
        // 其余时间戳初始化为0
        this.acceptedTimestamp = -1;
        this.processingTimestamp = -1;
        this.completedTimestamp = -1;
        // 设置请求对应的 LLM 模型
        this.setLlmModel(llmModelName);
    }

    public InferenceRequest(String requestId, String userId, String userMessage, int maxNewTokens, int batchSize, String llmModelName) {
        this.requestId = requestId;
        this.userId = userId;
        this.userMessage = userMessage;
        this.maxNewTokens = maxNewTokens;
        this.batchSize = batchSize;
        // 请求创建的时间戳
        this.createTimestamp = System.currentTimeMillis();
        // 其余时间戳初始化为0
        this.acceptedTimestamp = -1;
        this.processingTimestamp = -1;
        this.completedTimestamp = -1;
        // 设置请求对应的 LLM 模型
        this.setLlmModel(llmModelName);
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

    public int getMaxNewTokens() {
        return maxNewTokens;
    }

    public void setMaxNewTokens(int maxTokens) {
        this.maxNewTokens = maxTokens;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public void setCreateTimestamp(long createTimestamp) {
        this.createTimestamp = createTimestamp;
    }

    public long getAcceptedTimestamp() {
        return acceptedTimestamp;
    }

    public void setAcceptedTimestamp(long acceptedTimestamp) {
        this.acceptedTimestamp = acceptedTimestamp;
    }

    public long getProcessingTimestamp() {
        return processingTimestamp;
    }

    public void setProcessingTimestamp(long processingTimestamp) {
        this.processingTimestamp = processingTimestamp;
    }

    public long getCompletedTimestamp() {
        return completedTimestamp;
    }

    public void setCompletedTimestamp(long completedTimestamp) {
        this.completedTimestamp = completedTimestamp;
    }

    public void setLlmModel(String llmModelName) {
        this.llmModel = LLMModel.fromModelName(llmModelName);
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

    public LLMModel getLlmModel() {
        return this.llmModel;
    }

    @Override
    public String toString() {
        return String.format("InferenceRequest{id=%s, user=%s, message='%s', tokens=%d, batch=%d}",
                           requestId, userId, 
                           userMessage.length() > 30 ? userMessage.substring(0, 30) + "..." : userMessage,
                           maxNewTokens, batchSize);
    }
}
