package com.infertuner.cache;

/**
 * KV缓存条目 - 模拟LLM推理中的键值缓存
 */
public class KVCacheEntry {
    public String userId;
    public String sessionId;
    public String cacheKey;      // 格式：user_{userId}_session_{sessionId}
    public byte[] kvData;        // 模拟KV cache数据
    public long timestamp;       // 创建或最后访问时间
    public int accessCount;      // 访问次数
    
    public KVCacheEntry(String userId, String sessionId, byte[] kvData) {
        this.userId = userId;
        this.sessionId = sessionId;
        this.cacheKey = String.format("user_%s_session_%s", userId, sessionId);
        this.kvData = kvData;
        this.timestamp = System.currentTimeMillis();
        this.accessCount = 1;
    }
    
    public void updateAccess() {
        this.timestamp = System.currentTimeMillis();
        this.accessCount++;
    }
    
    public int getDataSize() {
        return kvData != null ? kvData.length : 0;
    }
    
    @Override
    public String toString() {
        return String.format("KVCache{key=%s, size=%d, access=%d, time=%d}", 
                           cacheKey, getDataSize(), accessCount, timestamp);
    }
}