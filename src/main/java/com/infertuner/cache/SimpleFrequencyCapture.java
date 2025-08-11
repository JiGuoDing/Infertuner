package com.infertuner.cache;

import java.util.*;

/**
 * 简化版本的频率捕获器 - 用于FREQUENCY策略
 * 不依赖外部库，使用Java内置数据结构
 */
public class SimpleFrequencyCapture {
    
    private final int bucketCount;
    private final long[][] buckets;  // [访问次数, 唯一键数量]
    private final Set<String> seenKeys;  // 简化版布隆过滤器
    private long totalCount;
    
    public SimpleFrequencyCapture(int bucketCount) {
        this.bucketCount = bucketCount;
        this.buckets = new long[bucketCount][2];
        this.seenKeys = new HashSet<>();
        this.totalCount = 0;
    }
    
    /**
     * 添加一个键的访问记录
     */
    public void add(String key) {
        totalCount++;
        int bucketIndex = Math.abs(key.hashCode()) % bucketCount;
        
        // 增加该bucket的访问次数
        buckets[bucketIndex][0]++;
        
        // 如果是新键，增加唯一键数量
        if (!seenKeys.contains(key)) {
            seenKeys.add(key);
            buckets[bucketIndex][1]++;
        }
    }
    
    /**
     * 计算达到目标命中率需要的缓存大小
     * @param targetHitRate 目标命中率 (0.0 - 1.0)
     * @return 建议的缓存大小
     */
    public long calculate(double targetHitRate) {
        if (totalCount == 0) {
            return 0;
        }
        
        // 创建非空bucket列表并按平均频率排序
        List<BucketInfo> validBuckets = new ArrayList<>();
        for (int i = 0; i < bucketCount; i++) {
            if (buckets[i][1] > 0) {  // 有唯一键的bucket
                double avgFreq = (double) buckets[i][0] / buckets[i][1];
                validBuckets.add(new BucketInfo(i, buckets[i][0], buckets[i][1], avgFreq));
            }
        }
        
        // 按平均频率降序排序（热门的在前面）
        validBuckets.sort((a, b) -> Double.compare(b.avgFrequency, a.avgFrequency));
        
        // 累计到达目标覆盖率
        long targetAccessCount = (long) Math.ceil(totalCount * targetHitRate);
        long currentAccessSum = 0;
        long currentKeySum = 0;
        
        for (BucketInfo bucket : validBuckets) {
            currentAccessSum += bucket.accessCount;
            currentKeySum += bucket.uniqueKeys;
            
            if (currentAccessSum >= targetAccessCount) {
                break;
            }
        }
        
        return currentKeySum;
    }
    
    /**
     * 获取当前统计信息
     */
    public Stats getStats() {
        return new Stats(totalCount, seenKeys.size(), bucketCount);
    }
    
    /**
     * 重置统计信息（用于新的时间窗口）
     */
    public void reset() {
        for (int i = 0; i < bucketCount; i++) {
            buckets[i][0] = 0;
            buckets[i][1] = 0;
        }
        seenKeys.clear();
        totalCount = 0;
    }
    
    /**
     * Bucket信息辅助类
     */
    private static class BucketInfo {
        final int index;
        final long accessCount;
        final long uniqueKeys;
        final double avgFrequency;
        
        BucketInfo(int index, long accessCount, long uniqueKeys, double avgFrequency) {
            this.index = index;
            this.accessCount = accessCount;
            this.uniqueKeys = uniqueKeys;
            this.avgFrequency = avgFrequency;
        }
    }
    
    /**
     * 统计信息类
     */
    public static class Stats {
        public final long totalAccess;
        public final int uniqueKeys;
        public final int bucketCount;
        
        Stats(long totalAccess, int uniqueKeys, int bucketCount) {
            this.totalAccess = totalAccess;
            this.uniqueKeys = uniqueKeys;
            this.bucketCount = bucketCount;
        }
        
        @Override
        public String toString() {
            return String.format("Stats{总访问=%d, 唯一键=%d, Bucket数=%d}", 
                               totalAccess, uniqueKeys, bucketCount);
        }
    }
}
