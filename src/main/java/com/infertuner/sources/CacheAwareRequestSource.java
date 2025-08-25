package com.infertuner.sources;

import com.infertuner.models.InferenceRequest;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * 优化的请求数据源 - 设计用于突出三种缓存策略的差异
 */
public class CacheAwareRequestSource implements SourceFunction<InferenceRequest> {
    
    private static final Logger logger = LoggerFactory.getLogger(CacheAwareRequestSource.class);
    private volatile boolean isRunning = true;
    private final Random random = new Random();
    
    // 配置参数
    private final int maxRequests;
    private final long baseInterval;
    private final boolean enableLoadVariation;

    private final String[] questions = {
        "Explain the interaction between warfarin and vitamin K-rich foods.",
        "List the cardiovascular benefits observed within one year of smoking cessation.",
        "Describe the proper technique for applying sunscreen to achieve labeled SPF.",
        "Name two vaccines recommended for adults over 65 and their benefits.",
        "Name two signs of lactose intolerance and how they differ from milk allergy.",
        "What makes a memorable melody in reggae music? List 3-5 key points.",
        "Suggest a day-by-day plan for Toronto with 3-4 activities per day and dining options. Focus on the most important aspects."
    };
    
    public CacheAwareRequestSource() {
        this.maxRequests = 100;
        this.baseInterval = 1000;
        this.enableLoadVariation = true;
    }
    
    public CacheAwareRequestSource(int maxRequests, long baseInterval, boolean enableLoadVariation) {
        this.maxRequests = maxRequests;
        this.baseInterval = baseInterval;
        this.enableLoadVariation = enableLoadVariation;
    }
    
    @Override
    public void run(SourceContext<InferenceRequest> ctx) throws Exception {
        logger.info("开始生成优化负载请求流，总数: {}", maxRequests);
        
        for (int i = 0; i < maxRequests && isRunning; i++) {
            // 计算当前负载阶段
            LoadPhase phase = calculateLoadPhase(i, maxRequests);
            LoadConfig config = getLoadConfig(phase);
            
            // 根据负载选择用户
            String userId = selectUserForLoad(config, i);
            
            // 生成请求
            String requestId = String.format("req_%04d", i);
            String question = questions[random.nextInt(questions.length)];
            // 最大生成 token 数 320-640 之间随机
            int maxTokens = 320 + random.nextInt(320);
            int batchSize = 1;
            
            InferenceRequest request = new InferenceRequest(requestId, userId, question, maxTokens, batchSize);
            
            // 发送请求
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(request);
            }
            
            // 每12个请求输出一次负载信息
            if (i % 12 == 0) {
                logger.info("负载阶段: {} | 活跃用户: {}个 | 每用户session: {} | 请求间隔: {}ms | 进度: {}/{}",
                           phase.description, config.activeUsers, 
                           config.sessionsPerUser == -1 ? "随机1-5" : config.sessionsPerUser,
                           config.interval, i+1, maxRequests);
            }
            
            // 动态间隔
            if (i < maxRequests - 1) {
                Thread.sleep(config.interval);
            }
        }
        
        logger.info("优化负载请求流生成完成，共 {} 个请求", maxRequests);
    }
    
    /**
     * 计算当前负载阶段
     */
    private LoadPhase calculateLoadPhase(int currentRequest, int totalRequests) {
        if (!enableLoadVariation) {
            return LoadPhase.WIDE_SPREAD;
        }
        
        double progress = (double) currentRequest / totalRequests;

        // 根据当前已发送的请求数确定当前的负载阶段
        if (progress < 0.15) {
            return LoadPhase.CONCENTRATED;      // 0-15%: 高度集中访问
        } else if (progress < 0.35) {
            return LoadPhase.MODERATE_SPREAD;   // 15-35%: 中等分散
        } else if (progress < 0.55) {
            return LoadPhase.WIDE_SPREAD;       // 35-55%: 高度分散
        } else if (progress < 0.75) {
            return LoadPhase.CHAOS;             // 55-75%: 混乱访问
        } else {
            return LoadPhase.RETURN_CONCENTRATED; // 75-100%: 回归集中
        }
    }
    
    /**
     * 获取负载配置
     */
    private LoadConfig getLoadConfig(LoadPhase phase) {
        switch (phase) {
            case CONCENTRATED:
                return new LoadConfig(3, 1, baseInterval / 2);
            case MODERATE_SPREAD:
                return new LoadConfig(8, 2, baseInterval / 2);
            case WIDE_SPREAD:
                return new LoadConfig(25, 3, baseInterval / 2);
            case CHAOS:
                return new LoadConfig(40, -1, baseInterval / 3);
            case RETURN_CONCENTRATED:
                return new LoadConfig(5, 1, baseInterval / 3);
            default:
                return new LoadConfig(10, 2, baseInterval);
        }
    }
    
    /**
     * 根据负载配置选择用户
     */
    private String selectUserForLoad(LoadConfig config, int requestIndex) {
        int userIndex;

        if (config.sessionsPerUser == -1) {
            // 混乱模式：在 1 到 config.activeUsers 之间完全随机选择一个用户
            userIndex = random.nextInt(config.activeUsers) + 1;
        } else {
            // 其他模式：根据访问集中度选择
            if (config.activeUsers <= 5) {
                // 集中访问：80%的请求访问来自前60%的用户
                if (random.nextDouble() < 0.8) {
                    double top60PercentCount = Math.ceil(config.activeUsers * 3.0 / 5.0);
                    userIndex = random.nextInt((int) top60PercentCount) + 1;
                } else {
                    userIndex = random.nextInt(config.activeUsers) + 1;
                }
            } else {
                // 分散访问：相对均匀分布
                userIndex = random.nextInt(config.activeUsers) + 1;
            }
        }

        return String.format("user_%04d", userIndex);
    }
    
    @Override
    public void cancel() {
        isRunning = false;
    }
    
    /**
     * 负载阶段
     */
    private enum LoadPhase {
        CONCENTRATED("高度集中访问"),
        MODERATE_SPREAD("中等分散访问"), 
        WIDE_SPREAD("高度分散访问"),
        CHAOS("混乱访问模式"),
        RETURN_CONCENTRATED("回归集中访问");
        
        public final String description;
        
        LoadPhase(String description) {
            this.description = description;
        }
    }
    
    /**
     * 负载配置
     */
    private static class LoadConfig {
        public final int activeUsers;      // 活跃用户数
        public final int sessionsPerUser;  // 每用户session数 (-1表示随机1-5)
        public final long interval;        // 请求间隔
        
        public LoadConfig(int activeUsers, int sessionsPerUser, long interval) {
            this.activeUsers = activeUsers;
            this.sessionsPerUser = sessionsPerUser;
            this.interval = interval;
        }
    }
}
