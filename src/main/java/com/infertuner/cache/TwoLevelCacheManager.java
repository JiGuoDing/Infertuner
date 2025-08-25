package com.infertuner.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 二级缓存管理器 - 本地缓存 + 远端缓存
 */
public class TwoLevelCacheManager {
    
    private static final Logger logger = LoggerFactory.getLogger(TwoLevelCacheManager.class);
    
    // 本地缓存 - 快速但容量有限
    private final Map<String, KVCacheEntry> localCache = new ConcurrentHashMap<>();
    
    // 远端缓存 - 慢但容量大（实际场景中可能是Redis、分布式存储等）
    private final Map<String, KVCacheEntry> remoteCache = new ConcurrentHashMap<>();
    
    // 配置参数
    private int maxLocalCacheSize = 100;    // 本地缓存最大条目数
    private final int remoteAccessDelayMs = 50;  // 模拟远端访问延迟
    
    // 统计信息
    private long totalRequests = 0;
    // 本地缓存命中次数
    private long localHits = 0;
    // 远程缓存命中次数
    private long remoteHits = 0;
    // 缓存未命中次数
    private long misses = 0;
    
    public TwoLevelCacheManager() {
        logger.info("初始化二级缓存管理器，本地缓存大小: {}，模拟远端缓存访问延迟: {}", maxLocalCacheSize, remoteAccessDelayMs);
    }
    
    public TwoLevelCacheManager(int maxLocalCacheSize) {
        this.maxLocalCacheSize = maxLocalCacheSize;
        logger.info("初始化二级缓存管理器，本地缓存大小: {}，模拟远端缓存访问延迟: {}", maxLocalCacheSize, remoteAccessDelayMs);
    }
    
    /**
     * 获取缓存条目
     */
    public KVCacheEntry get(String userId, String sessionId) {
        String cacheKey = String.format("user_%s_session_%s", userId, sessionId);
        totalRequests++;
        
        // 1. 先查本地缓存
        KVCacheEntry entry = localCache.get(cacheKey);
        if (entry != null) {
            localHits++;
            entry.updateAccess();
            logger.debug("本地缓存命中: {}", cacheKey);
            return entry;
        } else {
            // 2. 本地缓存未命中，再查远端缓存
            logger.debug("本地缓存未命中，查询远端: {}", cacheKey);
            try {
                // 模拟远端访问延迟
                Thread.sleep(remoteAccessDelayMs);

                entry = remoteCache.get(cacheKey);
                if (entry != null) {
                    remoteHits++;
                    entry.updateAccess();

                    // 将远端数据加载到本地缓存
                    putToLocalCache(cacheKey, entry);
                    logger.debug("远端缓存命中并加载到本地: {}", cacheKey);
                    return entry;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("远端缓存访问被中断", e);
            }

            // 3. 缓存未命中
            misses++;
            logger.debug("缓存完全未命中: {}", cacheKey);
            return null;
        }
    }

    /**
     * 存储缓存条目
     */
    public void put(String userId, String sessionId, byte[] kvData) {
        KVCacheEntry entry = new KVCacheEntry(userId, sessionId, kvData);
        String cacheKey = entry.cacheKey;
        
        // 同时存储到本地和远端
        putToLocalCache(cacheKey, entry);
        remoteCache.put(cacheKey, entry);
        
        logger.debug("缓存条目已存储至本地缓存和远端缓存: {}", cacheKey);
    }
    
    /**
     * 存储到本地缓存（包含LRU淘汰逻辑）
     */
    private void putToLocalCache(String cacheKey, KVCacheEntry entry) {
        // 如果本地缓存已满，使用LRU策略淘汰
        if (localCache.size() >= maxLocalCacheSize && !localCache.containsKey(cacheKey)) {
            evictLRU();
        }
        
        localCache.put(cacheKey, entry);
    }
    
    /**
     * LRU淘汰策略 - 淘汰最久未访问的条目
     */
    private void evictLRU() {
        if (localCache.isEmpty()) return;
        
        String oldestKey = null;
        long oldestTime = Long.MAX_VALUE;
        
        for (Map.Entry<String, KVCacheEntry> entry : localCache.entrySet()) {
            if (entry.getValue().timestamp < oldestTime) {
                oldestTime = entry.getValue().timestamp;
                oldestKey = entry.getKey();
            }
        }
        
        if (oldestKey != null) {
            localCache.remove(oldestKey);
            logger.debug("LRU淘汰本地缓存条目: {}", oldestKey);
        }
    }
    
    /**
     * 动态调整本地缓存大小 - 为自适应策略预留接口
     */
    public void resizeLocalCache(int newSize) {
        logger.info("调整本地缓存大小: {} -> {}", maxLocalCacheSize, newSize);
        this.maxLocalCacheSize = newSize;
        
        // 如果新大小更小，需要淘汰多余条目
        while (localCache.size() > maxLocalCacheSize) {
            evictLRU();
        }
    }
    
    /**
     * 获取缓存统计信息
     */
    public CacheStats getStats() {
        CacheStats stats = new CacheStats();
        stats.totalRequests = totalRequests;
        stats.localHits = localHits;
        stats.remoteHits = remoteHits;
        stats.misses = misses;
        stats.localCacheSize = localCache.size();
        stats.remoteCacheSize = remoteCache.size();
        stats.maxLocalCacheSize = maxLocalCacheSize;
        
        if (totalRequests > 0) {
            stats.localHitRate = (double) localHits / totalRequests;
            stats.remoteHitRate = (double) remoteHits / totalRequests;
            stats.overallHitRate = (double) (localHits + remoteHits) / totalRequests;
        }
        
        return stats;
    }
    
    /**
     * 清空缓存统计（用于测试）
     */
    public void resetStats() {
        totalRequests = 0;
        localHits = 0;
        remoteHits = 0;
        misses = 0;
    }
    
    /**
     * 清空所有缓存
     */
    public void clearAll() {
        localCache.clear();
        remoteCache.clear();
        resetStats();
        logger.info("所有缓存已清空");
    }
    
    /**
     * 缓存统计信息
     */
    public static class CacheStats {
        public long totalRequests;
        public long localHits;
        public long remoteHits;
        public long misses;
        public int localCacheSize;
        public int remoteCacheSize;
        public int maxLocalCacheSize;
        public double localHitRate;
        public double remoteHitRate;
        public double overallHitRate;
        
        @Override
        public String toString() {
            return String.format(
                "CacheStats{总请求=%d, 本地命中=%d(%.1f%%), 远端命中=%d(%.1f%%), 未命中=%d(%.1f%%), " +
                "本地大小=%d/%d, 远端大小=%d}",
                totalRequests, localHits, localHitRate * 100, 
                remoteHits, remoteHitRate * 100, misses, (1 - overallHitRate) * 100,
                localCacheSize, maxLocalCacheSize, remoteCacheSize
            );
        }
    }
}