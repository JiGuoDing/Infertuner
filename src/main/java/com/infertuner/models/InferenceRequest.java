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
    
    @Override
    public String toString() {
        return String.format("InferenceRequest{id=%s, user=%s, message='%s', tokens=%d, batch=%d}", 
                           requestId, userId, 
                           userMessage.length() > 30 ? userMessage.substring(0, 30) + "..." : userMessage,
                           maxTokens, batchSize);
    }
}
