package com.infertuner.jobs;

import com.infertuner.models.InferenceRequest;
import com.infertuner.models.InferenceResponse;
import com.infertuner.processors.GPUInferenceProcessor;
import com.infertuner.sinks.GPUMonitorSink;
import com.infertuner.sinks.UnifiedPerformanceSink;
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
        int parallelism = args.length > 0 ? Integer.parseInt(args[0]) : 10; // 默认10卡
        int maxRequests = args.length > 1 ? Integer.parseInt(args[1]) : 100; // 默认100个请求
        long interval = args.length > 2 ? Long.parseLong(args[2]) : 1000; // 默认1秒间隔
        
        logger.info("配置: parallelism={}, maxRequests={}, interval={}ms", parallelism, maxRequests, interval);

        // 创建Flink流执行环境，构建流水线
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置全局并行度为GPU数量
        env.setParallelism(parallelism);

        // 生成模拟请求流
        DataStream<InferenceRequest> requests = env
            .addSource(new BasicRequestSource(maxRequests, interval))
            .name("Request Source");

        // 将推理请求映射为推理响应流
        DataStream<InferenceResponse> responses = requests
            .map(new GPUInferenceProcessor())
            .name("Multi-GPU Inference Processor");

        String experimentId = String.format("%d nodes - %d requests", parallelism, maxRequests);
        // 添加 .keyBy(r -> 0) 将所有响应聚合到一个分区，并把 Sink 并行度强制设为1
        responses.keyBy(r -> 0).addSink(new UnifiedPerformanceSink(
                UnifiedPerformanceSink.ExperimentType.GPU_SCALING,
                experimentId
        )).name("Unified Node GPU Performance Sink").setParallelism(1);
        
        logger.info("多节点推理流水线构建完成，开始执行...");
        
        // 提交并运行作业
        env.execute("InferTuner Multi-Nodes Test");
        
        logger.info("=== 多节点推理执行完成 ===");
    }
}