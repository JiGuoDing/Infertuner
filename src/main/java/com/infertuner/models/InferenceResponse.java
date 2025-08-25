package com.infertuner.models;

/**
 * 推理响应数据模型
 */
public class InferenceResponse {
    public String requestId;
    public String userId;
    public String userMessage;
    public String aiResponse;
    public double inferenceTimeMs;
    public String modelName;
    public boolean success;
    public boolean fromCache;  // 是否来自缓存
    public int batchSize;
    public long timestamp;
    public long requestAcceptedTime;

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

    public double getInferenceTimeMs() {
        return inferenceTimeMs;
    }

    public void setInferenceTimeMs(double inferenceTimeMs) {
        this.inferenceTimeMs = inferenceTimeMs;
    }

    public String getAiResponse() {
        return aiResponse;
    }

    public void setAiResponse(String aiResponse) {
        this.aiResponse = aiResponse;
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public boolean isFromCache() {
        return fromCache;
    }

    public void setFromCache(boolean fromCache) {
        this.fromCache = fromCache;
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

    public long getRequestAcceptedTime() {
        return requestAcceptedTime;
    }

    public void setRequestAcceptedTime(long requestAcceptedTime) {
        this.requestAcceptedTime = requestAcceptedTime;
    }

    public long getWaitTimeMs() {
        return waitTimeMs;
    }

    public void setWaitTimeMs(long waitTimeMs) {
        this.waitTimeMs = waitTimeMs;
    }

    public long getBatchProcessTimeMs() {
        return batchProcessTimeMs;
    }

    public void setBatchProcessTimeMs(long batchProcessTimeMs) {
        this.batchProcessTimeMs = batchProcessTimeMs;
    }

    public long getTotalLatencyMs() {
        return totalLatencyMs;
    }

    public void setTotalLatencyMs(long totalLatencyMs) {
        this.totalLatencyMs = totalLatencyMs;
    }

    // 批处理相关字段 - 新增
    public long waitTimeMs = 0;               // 在缓冲区等待时间
    public long batchProcessTimeMs = 0;       // 批处理推理时间
    public long totalLatencyMs = 0;           // 总延迟 = 等待时间 + 处理时间
    
    public InferenceResponse() {}
    
    @Override
    public String toString() {
        // 如果有批处理信息，显示详细延迟信息
        if (batchSize > 1 || waitTimeMs > 0) {
            return String.format("InferenceResponse{id=%s, success=%s, batch=%d, wait=%dms, process=%dms, total=%dms}", 
                               requestId, success, batchSize, waitTimeMs, batchProcessTimeMs, totalLatencyMs);
        } else {
            // 保持原有格式用于兼容性
            return String.format("InferenceResponse{id=%s, success=%s, time=%.2fms, cache=%s, response='%s'}", 
                               requestId, success, inferenceTimeMs, fromCache,
                               aiResponse != null && aiResponse.length() > 50 ? 
                                   aiResponse.substring(0, 50) + "..." : aiResponse);
        }
    }
}