package com.infertuner.models;

/**
 * 推理请求数据模型
 */
public class InferenceRequest {
    public String requestId;
    public String userId;
    public String userMessage;
    public int maxTokens;
    public int batchSize;
    // 请求的创建时间戳
    public long createTimestamp;
    // 请求被接受的时间戳
    public long acceptedTimestamp;
    // 请求开始处理的时间戳
    public long processingTimestamp;
    // 请求执行完成的时间戳
    public long completedTimestamp;
    // TODO 为每个请求添加请求模型字段
    // public LLMModel llmModel;
    
    public InferenceRequest() {}

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

    public InferenceRequest(String requestId, String userId, String userMessage, int maxTokens) {
        this.requestId = requestId;
        this.userId = userId;
        this.userMessage = userMessage;
        this.maxTokens = maxTokens;
        // batchSize默认为1
        this.batchSize = 1;
        // 请求创建的时间戳
        this.createTimestamp = System.currentTimeMillis();
        // 其余时间戳初始化为0
        this.acceptedTimestamp = -1;
        this.processingTimestamp = -1;
        this.completedTimestamp = -1;
    }
    
    public InferenceRequest(String requestId, String userId, String userMessage, int maxTokens, int batchSize) {
        this.requestId = requestId;
        this.userId = userId;
        this.userMessage = userMessage;
        this.maxTokens = maxTokens;
        this.batchSize = batchSize;
        // 请求创建的时间戳
        this.createTimestamp = System.currentTimeMillis();
        // 其余时间戳初始化为0
        this.acceptedTimestamp = -1;
        this.processingTimestamp = -1;
        this.completedTimestamp = -1;
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

    public int getMaxTokens() {
        return maxTokens;
    }

    public void setMaxTokens(int maxTokens) {
        this.maxTokens = maxTokens;
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

    @Override
    public String toString() {
        return String.format("InferenceRequest{id=%s, user=%s, message='%s', tokens=%d, batch=%d}", 
                           requestId, userId, 
                           userMessage.length() > 30 ? userMessage.substring(0, 30) + "..." : userMessage,
                           maxTokens, batchSize);
    }
}
