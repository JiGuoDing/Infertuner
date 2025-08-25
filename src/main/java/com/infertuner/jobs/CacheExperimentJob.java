package com.infertuner.jobs;

import com.infertuner.models.InferenceRequest;
import com.infertuner.models.InferenceResponse;
import com.infertuner.processors.CacheEnabledInferenceProcessor;
import com.infertuner.sinks.SimpleResultSink;
import com.infertuner.sources.CacheAwareRequestSource;
import org.apache.flink.api.java.utils.ParameterTool;
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

        // 使用 Flink 自带的 ParameterTool 解析参数
        ParameterTool params = ParameterTool.fromArgs(args);

        // 参数带默认值，支持 --maxRequests 30 --baseInterval 2000 --enableLoadVariation true
        int maxRequests = params.getInt("maxRequests", 30);
        long baseInterval = params.getLong("baseInterval", 2000L);
        boolean enableLoadVariation = params.getBoolean("enableLoadVariation", true);

        logger.info("实验参数: 请求数={}, 基础间隔={}ms, 负载变化={}",
                maxRequests, baseInterval, enableLoadVariation);

        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 单并行度便于观察缓存效果

        // 将参数注册为全局参数（方便算子获取）
        env.getConfig().setGlobalJobParameters(params);

        // 请求生成流
        DataStream<InferenceRequest> requests = env
                .addSource(new CacheAwareRequestSource(maxRequests, baseInterval, enableLoadVariation))
                .name("Cache-Aware Request Source");

        // 带缓存的推理处理器
        DataStream<InferenceResponse> responses = requests
                .map(new CacheEnabledInferenceProcessor())
                .name("Cache-Enabled Inference Processor");

        // 数据汇总
        responses.addSink(new SimpleResultSink())
                .name("Result Sink");

        // 执行实验
        env.execute("InferTuner Cache Experiment");

        logger.info("=== 缓存实验完成 ===");
    }
}
