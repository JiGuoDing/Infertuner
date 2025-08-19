package com.infertuner.models;

/**
 * 推理请求数据模型
 */
public class InferenceRequest {
    public String requestId;
    public String userId;
    public String userMessage;
    public int maxTokens;
    public int batchSize;  // 批处理大小
    public long timestamp;
    
    public InferenceRequest() {}
    
    public InferenceRequest(String requestId, String userId, String userMessage, int maxTokens) {
        this.requestId = requestId;
        this.userId = userId;
        this.userMessage = userMessage;
        this.maxTokens = maxTokens;
        this.batchSize = 1;  // 默认批处理大小为1
        this.timestamp = System.currentTimeMillis();
    }
    
    public InferenceRequest(String requestId, String userId, String userMessage, int maxTokens, int batchSize) {
        this.requestId = requestId;
        this.userId = userId;
        this.userMessage = userMessage;
        this.maxTokens = maxTokens;
        this.batchSize = batchSize;
        this.timestamp = System.currentTimeMillis();
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

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return String.format("InferenceRequest{id=%s, user=%s, message='%s', tokens=%d, batch=%d}", 
                           requestId, userId, 
                           userMessage.length() > 30 ? userMessage.substring(0, 30) + "..." : userMessage,
                           maxTokens, batchSize);
    }
}
