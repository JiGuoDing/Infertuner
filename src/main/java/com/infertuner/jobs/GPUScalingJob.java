package com.infertuner.jobs;

import com.infertuner.models.InferenceRequest;
import com.infertuner.models.InferenceResponse;
import com.infertuner.processors.GPUInferenceProcessor;
import com.infertuner.sinks.GPUMonitorSink;
import com.infertuner.sources.BasicRequestSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 多GPU推理作业
 */
public class GPUScalingJob {
    
    private static final Logger logger = LoggerFactory.getLogger(GPUScalingJob.class);
    
    public static void main(String[] args) throws Exception {
        logger.info("=== InferTuner 多GPU版本启动 ===");
        
        // 解析参数
        int parallelism = args.length > 0 ? Integer.parseInt(args[0]) : 4; // 默认4卡
        int maxRequests = args.length > 1 ? Integer.parseInt(args[1]) : 100; // 默认100个请求
        long interval = args.length > 2 ? Long.parseLong(args[2]) : 1000; // 默认1秒间隔
        
        logger.info("配置: parallelism={}, maxRequests={}, interval={}ms", parallelism, maxRequests, interval);
        
        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism); // 设置并行度
        
        // 构建流水线
        DataStream<InferenceRequest> requests = env
            .addSource(new BasicRequestSource(maxRequests, interval, false))
            .name("Request Source");
        
        DataStream<InferenceResponse> responses = requests
            .map(new GPUInferenceProcessor())  // 使用多GPU处理器
            .name("Multi-GPU Inference Processor");
        
        responses.addSink(new GPUMonitorSink())
            .name("Multi-GPU Monitor Sink");
        
        logger.info("多GPU流水线构建完成，开始执行...");
        logger.info("Web UI: http://localhost:8081");
        
        // 执行
        env.execute("InferTuner Multi-GPU Test");
        
        logger.info("=== 多GPU执行完成 ===");
    }
}