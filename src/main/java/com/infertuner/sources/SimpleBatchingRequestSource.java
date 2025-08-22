package com.infertuner.source;

import com.infertuner.models.InferenceRequest;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 简单攒批请求源
 * 攒够batch_size个请求后一起发送，模拟真正的攒批行为
 */
public class SimpleBatchingRequestSource extends RichSourceFunction<InferenceRequest> {
    
    private static final Logger logger = LoggerFactory.getLogger(SimpleBatchingRequestSource.class);
    
    private final int maxRequests;
    private final long baseInterval;
    private final int batchSize;
    private final boolean verbose;
    private volatile boolean isRunning = true;
    
    public SimpleBatchingRequestSource(int maxRequests, long baseInterval, int batchSize, boolean verbose) {
        this.maxRequests = maxRequests;
        this.baseInterval = baseInterval;
        this.batchSize = batchSize;
        this.verbose = verbose;
    }
    
    @Override
    public void run(SourceContext<InferenceRequest> ctx) throws Exception {
        logger.info("开始攒批生成 {} 个请求，batch_size={}, 基础间隔={}ms", maxRequests, batchSize, baseInterval);
        
        List<InferenceRequest> batch = new ArrayList<>();
        int requestCount = 0;
        
        while (isRunning && requestCount < maxRequests) {
            // 生成一个请求
            InferenceRequest request = createRequest(requestCount);
            batch.add(request);
            requestCount++;
            
            if (verbose) {
                logger.info("生成请求 {}/{}: {} (缓冲区大小: {})", requestCount, maxRequests, request.requestId, batch.size());
            }
            
            // 检查是否达到批大小或是最后一批
            if (batch.size() >= batchSize || requestCount >= maxRequests) {
                long batchStartTime = System.currentTimeMillis();
                
                logger.info("🚀 发送第{}批，大小: {}, 请求: {}", 
                           (requestCount - 1) / batchSize + 1, 
                           batch.size(),
                           getBatchRequestIds(batch));
                
                // 一次性发送整个批次
                for (InferenceRequest req : batch) {
                    ctx.collect(req);
                }
                
                long batchTime = System.currentTimeMillis() - batchStartTime;
                logger.info("✅ 第{}批发送完成，耗时: {}ms", (requestCount - 1) / batchSize + 1, batchTime);
                
                // 清空批次，准备下一批
                batch.clear();
                
                // 如果不是最后一批，等待一段时间再生成下一批
                if (requestCount < maxRequests) {
                    long waitTime = baseInterval * batchSize;  // 等待时间 = 间隔 × 批大小
                    logger.info("⏳ 等待 {}ms 后生成下一批...", waitTime);
                    Thread.sleep(waitTime);
                }
            }
        }
        
        logger.info("✅ 攒批请求生成完成，共生成 {} 个请求", requestCount);
    }
    
    private InferenceRequest createRequest(int index) {
        InferenceRequest request = new InferenceRequest();
        request.requestId = String.format("req_%03d", index);
        request.userId = "user_" + (index % 10);
        request.userMessage = generateMessage(index);
        request.maxTokens = 50;
        request.batchSize = batchSize;  // 设置批大小
        request.createTimestamp = System.currentTimeMillis();
        return request;
    }
    
    private String generateMessage(int index) {
        String[] templates = {
            "请解释一下人工智能的基本概念",
            "描述一下机器学习的工作原理", 
            "什么是深度学习？",
            "介绍一下神经网络的结构",
            "解释什么是自然语言处理"
        };
        return templates[index % templates.length] + " (请求" + index + ")";
    }
    
    private String getBatchRequestIds(List<InferenceRequest> batch) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < batch.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(batch.get(i).requestId);
        }
        return sb.toString();
    }
    
    @Override
    public void cancel() {
        isRunning = false;
        logger.info("攒批请求源已取消");
    }
}