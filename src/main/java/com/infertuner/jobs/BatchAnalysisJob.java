package com.infertuner.jobs;

import com.infertuner.models.InferenceRequest;
import com.infertuner.models.InferenceResponse;
import com.infertuner.processors.BatchProcessor;
import com.infertuner.sinks.BatchAnalysisSink;
import com.infertuner.sources.BasicRequestSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 简化的真正攒批作业
 */
public class BatchAnalysisJob {
    
    private static final Logger logger = LoggerFactory.getLogger(BatchAnalysisJob.class);
    
    public static void main(String[] args) throws Exception {
        logger.info("=== 简化的真正攒批验证开始 ===");
        
        // 解析参数
        int batchSize = args.length > 0 ? Integer.parseInt(args[0]) : 4;     // 批大小
        int parallelism = args.length > 1 ? Integer.parseInt(args[1]) : 4;   // 并行度
        int maxRequests = args.length > 2 ? Integer.parseInt(args[2]) : 100;   // 总请求数
        long interval = args.length > 3 ? Long.parseLong(args[3]) : 400;     // 请求间隔
        
        logger.info("简化攒批配置: batchSize={}, parallelism={}, maxRequests={}, interval={}ms", 
                   batchSize, parallelism, maxRequests, interval);
        
        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        
        // 设置批处理参数
        Configuration config = new Configuration();
        config.setString("batch.size", String.valueOf(batchSize));
        env.getConfig().setGlobalJobParameters(config);
        
        // 构建简化的攒批流水线
        DataStream<InferenceRequest> requests = env
            .addSource(new BasicRequestSource(maxRequests, interval))
            .name("Normal Request Source");
        
        DataStream<InferenceResponse> responses = requests
            .map(new BatchProcessor())
            .name("Simple Real Batch Processor");
        
        responses.addSink(new BatchAnalysisSink())
            .name("Batch Validation Sink");
        
        logger.info("简化攒批流水线构建完成，开始执行...");
        
        // 执行
        env.execute("Simple Real Batch Job");
        
        logger.info("=== 简化攒批验证完成 ===");
    }
}