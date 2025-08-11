package com.infertuner.test;

import com.infertuner.cache.KVCacheEntry;
import com.infertuner.cache.TwoLevelCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 缓存功能测试 - 验证二级缓存的正确性
 */
public class CacheTest {
    
    private static final Logger logger = LoggerFactory.getLogger(CacheTest.class);
    
    public static void main(String[] args) throws InterruptedException {
        logger.info("=== 开始缓存功能测试 ===");
        
        // 测试1: 基本功能测试
        testBasicFunctionality();
        
        // 测试2: LRU淘汰测试
        testLRUEviction();
        
        // 测试3: 缓存命中率测试
        testHitRatePattern();
        
        // 测试4: 动态调整测试
        testDynamicResize();
        
        logger.info("=== 缓存功能测试完成 ===");
    }
    
    /**
     * 测试1: 基本功能测试
     */
    private static void testBasicFunctionality() throws InterruptedException {
        logger.info("\n--- 测试1: 基本功能测试 ---");
        
        TwoLevelCacheManager cache = new TwoLevelCacheManager(3); // 本地缓存容量为3
        
        // 第一次访问 - 应该未命中
        KVCacheEntry entry1 = cache.get("user1", "session1");
        assert entry1 == null : "首次访问应该未命中";
        
        // 存储数据
        byte[] data1 = "test_kv_data_1".getBytes();
        cache.put("user1", "session1", data1);
        
        // 第二次访问 - 应该本地命中
        long startTime = System.currentTimeMillis();
        KVCacheEntry entry2 = cache.get("user1", "session1");
        long accessTime = System.currentTimeMillis() - startTime;
        
        assert entry2 != null : "存储后应该能够获取";
        assert accessTime < 10 : "本地缓存访问应该很快"; // 应该远小于50ms的远端延迟
        
        TwoLevelCacheManager.CacheStats stats = cache.getStats();
        logger.info("基本功能测试完成: {}", stats);
        
        assert stats.totalRequests == 2 : "总请求数应该为2";
        assert stats.localHits == 1 : "本地命中应该为1";
        assert stats.misses == 1 : "未命中应该为1";
    }
    
    /**
     * 测试2: LRU淘汰测试
     */
    private static void testLRUEviction() throws InterruptedException {
        logger.info("\n--- 测试2: LRU淘汰测试 ---");
        
        TwoLevelCacheManager cache = new TwoLevelCacheManager(2); // 本地缓存容量为2
        
        // 存储3个条目，应该触发LRU淘汰
        cache.put("user1", "session1", "data1".getBytes());
        cache.put("user2", "session1", "data2".getBytes());
        cache.put("user3", "session1", "data3".getBytes()); // 这个应该淘汰user1
        
        // 访问user1 - 应该从远端获取（有延迟）
        long startTime = System.currentTimeMillis();
        KVCacheEntry entry1 = cache.get("user1", "session1");
        long accessTime = System.currentTimeMillis() - startTime;
        
        assert entry1 != null : "数据仍在远端缓存中";
        assert accessTime >= 40 : "应该有远端访问延迟"; // 考虑一些误差
        
        // 访问user2 - 应该本地命中
        startTime = System.currentTimeMillis();
        KVCacheEntry entry2 = cache.get("user2", "session1");
        accessTime = System.currentTimeMillis() - startTime;
        
        assert entry2 != null : "user2应该还在本地缓存";
        assert accessTime < 10 : "本地缓存访问应该很快";
        
        TwoLevelCacheManager.CacheStats stats = cache.getStats();
        logger.info("LRU淘汰测试完成: {}", stats);
    }
    
    /**
     * 测试3: 缓存命中率测试
     */
    private static void testHitRatePattern() {
        logger.info("\n--- 测试3: 缓存命中率测试 ---");
        
        TwoLevelCacheManager cache = new TwoLevelCacheManager(5);
        
        // 模拟访问模式：热点数据重复访问
        String[] users = {"user1", "user2", "user3", "user4", "user5", "user6"};
        
        // 存储所有数据
        for (String user : users) {
            cache.put(user, "session1", (user + "_data").getBytes());
        }
        
        // 重复访问前3个用户（热点数据）
        for (int i = 0; i < 10; i++) {
            cache.get("user1", "session1");
            cache.get("user2", "session1");
            cache.get("user3", "session1");
        }
        
        TwoLevelCacheManager.CacheStats stats = cache.getStats();
        logger.info("缓存命中率测试完成: {}", stats);
        
        // 热点数据应该有较高的本地命中率
        assert stats.localHitRate > 0.5 : "热点数据应该有较高本地命中率";
        assert stats.overallHitRate > 0.8 : "整体命中率应该较高";
    }
    
    /**
     * 测试4: 动态调整测试
     */
    private static void testDynamicResize() {
        logger.info("\n--- 测试4: 动态调整测试 ---");
        
        TwoLevelCacheManager cache = new TwoLevelCacheManager(3);
        
        // 存储5个条目
        for (int i = 1; i <= 5; i++) {
            cache.put("user" + i, "session1", ("data" + i).getBytes());
        }
        
        TwoLevelCacheManager.CacheStats statsBefore = cache.getStats();
        logger.info("调整前: 本地缓存大小={}", statsBefore.localCacheSize);
        
        // 扩大缓存
        cache.resizeLocalCache(6);
        
        // 重新访问所有数据
        for (int i = 1; i <= 5; i++) {
            cache.get("user" + i, "session1");
        }
        
        TwoLevelCacheManager.CacheStats statsAfter = cache.getStats();
        logger.info("调整后: {}", statsAfter);
        
        // 缩小缓存
        cache.resizeLocalCache(2);
        
        TwoLevelCacheManager.CacheStats statsResized = cache.getStats();
        logger.info("缩小后: 本地缓存大小={}", statsResized.localCacheSize);
        
        assert statsResized.localCacheSize <= 2 : "缓存大小应该被正确调整";
        
        logger.info("动态调整测试完成");
    }
}