package com.infertuner;

import com.infertuner.model.InferenceRequest;
import com.infertuner.model.InferenceResponse;
import com.infertuner.processor.SimpleInferenceProcessor;
import com.infertuner.sink.SimpleResultSink;
import com.infertuner.source.SimpleRequestSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 精简版主作业 - 专注核心功能测试
 */
public class SimpleInferenceJob {
    
    private static final Logger logger = LoggerFactory.getLogger(SimpleInferenceJob.class);
    
    public static void main(String[] args) throws Exception {
        logger.info("=== InferTuner 精简版启动 ===");
        
        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 简化为单并行度
        
        // 构建流水线
        DataStream<InferenceRequest> requests = env
            .addSource(new SimpleRequestSource())
            .name("Request Source");
        
        DataStream<InferenceResponse> responses = requests
            .map(new SimpleInferenceProcessor())
            .name("Inference Processor");
        
        responses.addSink(new SimpleResultSink())
            .name("Result Sink");
        
        logger.info("流水线构建完成，开始执行...");
        logger.info("Web UI: http://localhost:8081");
        
        // 执行
        env.execute("InferTuner Simple Test");
        
        logger.info("=== 执行完成 ===");
    }
}