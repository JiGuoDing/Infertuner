package com.infertuner.jobs;

import com.infertuner.models.InferenceRequest;
import com.infertuner.models.InferenceResponse;
import com.infertuner.processors.CacheEnabledInferenceProcessor;
import com.infertuner.sinks.SimpleResultSink;
import com.infertuner.sources.CacheAwareRequestSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 缓存实验主作业 - 验证二级缓存系统
 */
public class CacheExperimentJob {
    
    private static final Logger logger = LoggerFactory.getLogger(CacheExperimentJob.class);
    
    public static void main(String[] args) throws Exception {
        logger.info("=== InferTuner 缓存实验启动 ===");
        

        int maxRequests = 30;
        long baseInterval = 2000;
        boolean enableLoadVariation = true;
        

        java.util.List<String> userArgs = new java.util.ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            // 跳过Flink内部参数
            if (arg.startsWith("--class") || arg.startsWith("--classpath") || 
                arg.startsWith("--jobmanager") || arg.startsWith("--parallelism")) {
                // 如果是带值的参数，跳过下一个参数
                if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    i++; // 跳过参数值
                }
                continue;
            }
            // 收集用户参数
            if (!arg.startsWith("--")) {
                userArgs.add(arg);
            }
        }
        
        // 解析用户参数
        if (userArgs.size() > 0) {
            try {
                maxRequests = Integer.parseInt(userArgs.get(0));
            } catch (NumberFormatException e) {
                logger.warn("无法解析maxRequests参数: {}, 使用默认值: {}", userArgs.get(0), maxRequests);
            }
        }
        if (userArgs.size() > 1) {
            try {
                baseInterval = Long.parseLong(userArgs.get(1));
            } catch (NumberFormatException e) {
                logger.warn("无法解析baseInterval参数: {}, 使用默认值: {}", userArgs.get(1), baseInterval);
            }
        }
        if (userArgs.size() > 2) {
            try {
                enableLoadVariation = Boolean.parseBoolean(userArgs.get(2));
            } catch (Exception e) {
                logger.warn("无法解析enableLoadVariation参数: {}, 使用默认值: {}", userArgs.get(2), enableLoadVariation);
            }
        }
        
        logger.info("实验参数: 请求数={}, 基础间隔={}ms, 负载变化={}", 
                   maxRequests, baseInterval, enableLoadVariation);
        
        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 单并行度便于观察缓存效果
        
        // 构建流水线
        DataStream<InferenceRequest> requests = env
            .addSource(new CacheAwareRequestSource(maxRequests, baseInterval, enableLoadVariation))
            .name("Cache-Aware Request Source");
        
        DataStream<InferenceResponse> responses = requests
            .map(new CacheEnabledInferenceProcessor())
            .name("Cache-Enabled Inference Processor");
        
        responses.addSink(new SimpleResultSink())
            .name("Result Sink");
        
        // 执行实验
        env.execute("InferTuner Cache Experiment");
        
        logger.info("=== 缓存实验完成 ===");
    }
}