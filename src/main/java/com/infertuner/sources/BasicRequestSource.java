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
        "Explain the interaction between warfarin and vitamin K-rich foods.",
        "List the cardiovascular benefits observed within one year of smoking cessation.",
        "Describe the proper technique for applying sunscreen to achieve labeled SPF.",
        "Name two vaccines recommended for adults over 65 and their benefits.",
        "Name two signs of lactose intolerance and how they differ from milk allergy.",
        "What makes a memorable melody in reggae music? List 3-5 key points.",
        "Suggest a day-by-day plan for Toronto with 3-4 activities per day and dining options. Focus on the most important aspects."
    };
    
    private final int maxRequests;
    private final long interval;
    private int parallelism = 0;

    public BasicRequestSource() {
        this.maxRequests = 10;  // 只生成10个请求用于测试
        this.interval = 2000;   // 2秒间隔
    }
    
    public BasicRequestSource(int maxRequests, long interval) {
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
            String requestId = String.format("req_%04d", i);
            String userId = users[random.nextInt(users.length)];
            String question = questions[random.nextInt(questions.length)];
            // int maxTokens = 640 + random.nextInt(640); // 640-1280 tokens
            int maxTokens = 320 + random.nextInt(320); // 320-640 tokens
            // int batchSize = 1 + random.nextInt(3);
            
            InferenceRequest request = new InferenceRequest(requestId, userId, question, maxTokens);
            
            // 发送请求
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(request);
            }
            
            logger.info("生成请求 {}/{}: {}", i+1, maxRequests, requestId);
            
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