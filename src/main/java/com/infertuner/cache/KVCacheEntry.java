package com.infertuner.cache;

/**
 * KV缓存条目 - 模拟LLM推理中的 KV 缓存
 */
public class KVCacheEntry {
    public String userId;
    public String sessionId;
    public String cacheKey;      // 格式：user_{userId}_session_{sessionId}
    public byte[] kvData;        // 模拟KV cache数据
    public long timestamp;       // 创建时间或最近一次被访问时间
    public int accessCount;      // 访问次数
    
    public KVCacheEntry(String userId, String sessionId, byte[] kvData) {
        this.userId = userId;
        this.sessionId = sessionId;
        this.cacheKey = String.format("%s-%s", userId, sessionId);
        this.kvData = kvData;
        this.timestamp = System.currentTimeMillis();
        this.accessCount = 1;
    }
    
    public void updateAccess() {
        // 更新最近访问时间和访问次数
        this.timestamp = System.currentTimeMillis();
        this.accessCount++;
    }

    // 获取 KV 缓存数据大小，用字节数指代 KV 缓存大小
    public int getDataSize() {
        return kvData != null ? kvData.length : 0;
    }
    
    @Override
    public String toString() {
        return String.format("KVCache{key=%s, size=%d, access=%d, time=%d}", 
                           cacheKey, getDataSize(), accessCount, timestamp);
    }
}