package com.infertuner.jobs;

import javax.xml.crypto.Data;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.infertuner.models.InferenceRequest;
import com.infertuner.models.InferenceResponse;
import com.infertuner.processors.KeyedInferenceProcessor;
import com.infertuner.processors.ParallelBatchProcessor;
import com.infertuner.processors.PredictingInferenceTimeProcessor;
import com.infertuner.sinks.JointOptimizationSink;
import com.infertuner.sources.CacheAwareRequestSource;

/**
 * InferTuner 流式推理动态扩缩GPU测试作业
 */
public class InferTunerTestingJob {
        private static final Logger logger = LoggerFactory.getLogger(InferTunerTestingJob.class);

        public static void main(String[] args) {

                logger.info("InferTuner 端到端测试作业启动");

                // 使用 Flink 自带的参数解析工具
                ParameterTool paramsFromArgs = ParameterTool.fromArgs(args);
                // 参数格式：--maxRequests 100 --parallelism 4 --batchsize 4 --baseInterval 2000
                // --enableLoadVariation true
                // 默认 100 个请求
                int maxRequests = paramsFromArgs.getInt("maxRequests", 100);
                // 默认以4并行度启动服务
                int parallelism = paramsFromArgs.getInt("parallelism", 4);
                // 默认以4为批次大小
                int batchsize = paramsFromArgs.getInt("batchsize", 4);
                // 默认基础请求间隔 1000ms
                long baseInterval = paramsFromArgs.getLong("baseInterval", 1000);
                // 默认开启负载波动
                boolean enableLoadVariation = paramsFromArgs.getBoolean("enableLoadVariation", true);

                logger.info("启动参数: maxRequests={}, parallelism={}, batchsize={} baseInterval={}, enableLoadVariation={}",
                                maxRequests, parallelism, batchsize, baseInterval, enableLoadVariation);

                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(parallelism);

                // 将启动参数注册为全局参数，以便所有算子访问
                env.getConfig().setGlobalJobParameters(paramsFromArgs);

                /*
                 * TODO 添加数据源
                 */
                DataStream<InferenceRequest> requests = env
                                .addSource(new CacheAwareRequestSource(maxRequests, baseInterval, enableLoadVariation))
                                .name("Cache-aware Requests Source");

                /*
                 * 请求预处理算子(预测请求的推理时间)
                 */
                DataStream<InferenceRequest> predicted_requests = requests
                                .flatMap(new PredictingInferenceTimeProcessor())
                                .name("Predicting Inference Time Operator");
                /*
                 * TODO 添加请求推理算子(执行推理)，根据请求对应的模型进行分流推理
                 */
                // 1. 按请求模型分流
                // DataStream<InferenceResponse> responses = predicted_requests.rebalance()
                //                 .keyBy(req -> req.getLlmModel().getModelName()).process(new KeyedInferenceProcessor())
                //                 .name("Keyed Inference Operator");
                // 2. 系统仅支持一种模型
                DataStream<InferenceResponse> responses = predicted_requests.rebalance().process(new ParallelBatchProcessor()).name("Common Inference Operator");

                /*
                 * TODO 添加 Sink 算子，根据当前推理负载情况确定是否要改变并行度(GPU节点数量)、批大小(batchSize)
                 */
                String experimentId = String.format("p%db%d_%dreq", parallelism, batchsize, maxRequests);
                responses.addSink(new JointOptimizationSink(experimentId, parallelism, batchsize, baseInterval)).name("Inference Responses Sink");

        }
}
