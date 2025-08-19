package com.infertuner.sources;

import com.infertuner.models.InferenceRequest;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * 精简版请求数据源
 */
public class BasicRequestSource implements SourceFunction<InferenceRequest> {
    
    private static final Logger logger = LoggerFactory.getLogger(BasicRequestSource.class);
    private volatile boolean isRunning = true;
    private final Random random = new Random();
    
    private final String[] users = {"user1", "user2", "user3", "user4", "user5", "user6", "user7", "user8", "user9", "user10"};
    private final String[] questions = {
        "什么是机器学习？",
        "深度学习的基本原理是什么？", 
        "神经网络如何工作？",
        "什么是过拟合？",
        "如何优化模型性能？"
    };
    
    private final int maxRequests;
    private final long interval;
    private int parallelism = 0;
    
    public BasicRequestSource() {
        this.maxRequests = 10;  // 只生成10个请求用于测试
        this.interval = 2000;   // 2秒间隔
    }
    
    public BasicRequestSource(int maxRequests, long interval, boolean enableBurst) {
        this.maxRequests = maxRequests;
        this.interval = interval;
    }

    public BasicRequestSource(int maxRequests, long interval, int parallelism) {
        this.maxRequests = maxRequests;
        this.interval = interval;
        this.parallelism = parallelism;
    }
    
    @Override
    public void run(SourceContext<InferenceRequest> ctx) throws Exception {
        logger.info("开始生成 {} 个测试请求，间隔 {}ms", maxRequests, interval);
        
        for (int i = 0; i < maxRequests && isRunning; i++) {
            // 生成请求
            String requestId = String.format("req_%03d", i);
            String userId = users[random.nextInt(users.length) % (parallelism > 0 ? parallelism : users.length)];
            String question = questions[random.nextInt(questions.length)];
            int maxTokens = 50 + random.nextInt(100); // 50-150 tokens
            int batchSize = 1 + random.nextInt(3);    // 1-3
            
            InferenceRequest request = new InferenceRequest(requestId, userId, question, maxTokens, batchSize);
            
            // 发送请求
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(request);
            }
            
            logger.info("生成请求 {}/{}: {}", i+1, maxRequests, requestId);
            // logger.info("key: {}", Integer.parseInt(requestId.substring(4)) % 4);
            
            // 等待
            if (i < maxRequests - 1) {
                Thread.sleep(interval);
            }
        }
        
        logger.info("请求生成完成，共 {} 个", maxRequests);
    }
    
    @Override
    public void cancel() {
        isRunning = false;
    }

    public int getParallelism() {
        return parallelism;
    }
}