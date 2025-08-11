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
    
    // 批处理相关字段 - 新增
    public long waitTimeMs = 0;               // 在缓冲区等待时间
    public long batchProcessTimeMs = 0;       // 批处理执行时间
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