package com.infertuner.jobs;

import com.infertuner.models.InferenceRequest;
import com.infertuner.models.InferenceResponse;
import com.infertuner.processors.KeyedProcessFunctionBatchProcessor;
import com.infertuner.sinks.UnifiedPerformanceSink;
import com.infertuner.sources.BasicRequestSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * 基于数量的真实攒批分析作业
 *
 * 核心特点：
 * 1. 真正的"攒够N个请求立即处理"语义
 * 2. 使用ProcessFunction + State实现可靠的缓冲
 * 3. 超时保护机制，避免死锁
 * 4. 一个批次输出多个响应，完美解决Window问题
 */
public class CountBasedBatchAnalysisJob {

    private static final Logger logger = LoggerFactory.getLogger(CountBasedBatchAnalysisJob.class);

    public static void main(String[] args) throws Exception {
        logger.info("=== InferTuner 基于数量的真实攒批验证 ===");

        // 解析参数
        int batchSize = args.length > 0 ? Integer.parseInt(args[0]) : 4;
        int parallelism = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        int maxRequests = args.length > 2 ? Integer.parseInt(args[2]) : 30;
        long interval = args.length > 3 ? Long.parseLong(args[3]) : 200;

        // 🔧 超时配置：确保不会死锁
        long maxWaitTimeMs = Math.max(batchSize * interval * 2, 2000);

        // 键组
        ArrayList<String> keyList = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            keyList.add("key" + i);
        }

        logger.info("🎯 基于数量的真实攒批配置:");
        logger.info("  目标批大小: {} (攒够立即处理)", batchSize);
        logger.info("  并行度: {}", parallelism);
        logger.info("  总请求数: {}", maxRequests);
        logger.info("  请求间隔: {}ms", interval);
        logger.info("  超时保护: {}ms (避免死锁)", maxWaitTimeMs);
        logger.info("  攒批语义: 数量优先 + 时间兜底");

        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        // 🔧 配置全局参数
        Configuration config = new Configuration();
        config.setString("batch.size", String.valueOf(batchSize));
        config.setString("max.wait.ms", String.valueOf(maxWaitTimeMs));
        config.setString("request.interval", String.valueOf(interval));
        config.setString("max.requests", String.valueOf(maxRequests));
        env.getConfig().setGlobalJobParameters(config);

        logger.info("✅ 全局参数配置完成，确保ProcessFunction获取正确配置");
        logger.info("🔍 请求源配置验证:");
        logger.info("  请求间隔: {}ms", interval);
        logger.info("  总请求数: {}", maxRequests);
        logger.info("  预期总时长: 约{}秒", (maxRequests * interval) / 1000.0);

        // 构建基于数量的真实攒批流水线
        DataStream<InferenceRequest> requests = env
                .addSource(new BasicRequestSource(maxRequests, interval)) // 设置为true，等待所有请求处理x完成
                .name("Count Batch Request Source");

        // 用request的requestID作为键，并且使用rebalance()进行负载均衡
        // 注：通过keyBy获取key后，flink还会进行key分区，通过targetTaskIndex = hash(key) % numTasks得到下游subtask的索引
        // 将key进行放大，防止flink内部键分区hash后请求落到固定几个subtask
        // 不使用requestId，而使用userId
        // req -> (Integer.parseInt(req.getRequestId().substring(4)) % parallelism) * x
        // int x = (int) Math.pow(parallelism, parallelism) % 2 == 0 ? (int) Math.pow(parallelism, parallelism) + 1 : (int) Math.pow(parallelism, parallelism);
        // req -> keyList.get(Integer.parseInt(req.getRequestId().substring(4)) % parallelism)
        // 使用InferenceRequest::getRequestId作为key，配合rebalance实现均匀分布，在后续处理算子中再自定义状态存储。
        DataStream<InferenceResponse> responses = requests.rebalance().keyBy(InferenceRequest::getRequestId)
                .process(new KeyedProcessFunctionBatchProcessor())
                .name("Count Based Batch Processor");

        // 使用统一性能统计
        String experimentId = String.format("%d batch - %d requests", batchSize, maxRequests);
        responses.keyBy(r -> 0).addSink(new UnifiedPerformanceSink(
                        UnifiedPerformanceSink.ExperimentType.BATCH_ANALYSIS,
                        experimentId))
                .name("Count Batch Performance Sink").setParallelism(1);

        logger.info("🚀 基于数量的真实攒批流水线构建完成");
        logger.info("📊 核心优势:");
        logger.info("  1. 真正攒够{}个请求立即处理（不等时间）", batchSize);
        logger.info("  2. ProcessFunction支持一对多输出（一个批次→多个响应）");
        logger.info("  3. State-based缓冲，支持故障恢复");
        logger.info("  4. Timer超时保护，避免最后几个请求永远等待");
        logger.info("  5. 真实GPU批量并行，不是伪攒批");

        logger.info("📈 预期效果:");
        logger.info("  - Batch-1: 无等待，直接处理");
        logger.info("  - Batch-4: 第1个请求等待最久，第4个立即处理");
        logger.info("  - GPU利用率: 大批次显著提升吞吐量");
        logger.info("  - 延迟分布: 批次内请求有不同等待时间");

        // 执行
        env.execute("InferTuner Count Based Real Batch Analysis Test");

        logger.info("=== 基于数量的真实攒批验证完成 ===");
    }
}
