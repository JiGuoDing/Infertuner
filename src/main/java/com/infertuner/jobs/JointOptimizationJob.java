package com.infertuner.jobs;

import com.infertuner.models.InferenceRequest;
import com.infertuner.models.InferenceResponse;
import com.infertuner.processors.ParallelBatchProcessor;
import com.infertuner.sinks.JointOptimizationSink;
import com.infertuner.sources.BasicRequestSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * p×b联合优化作业
 * 核心功能：
 * 1. 测试不同并行度(p)和批大小(b)的组合
 * 2. 每个GPU独立攒批，实现真正的并行批处理
 * 3. 生成完整的性能矩阵用于联合优化
 */
public class JointOptimizationJob {

    private static final Logger logger = LoggerFactory.getLogger(JointOptimizationJob.class);

    public static void main(String[] args) throws Exception {
        logger.info("=== InferTuner p×b联合优化验证 ===");

        // 解析参数
        int parallelism = args.length > 0 ? Integer.parseInt(args[0]) : 2;        // p: 并行度/GPU节点数量
        int batchSize = args.length > 1 ? Integer.parseInt(args[1]) : 4;          // b: 每批大小
        int batchesPerNode = args.length > 2 ? Integer.parseInt(args[2]) : 6;      // batch_num_per_node 每节点处理的批次数
        long interval = args.length > 3 ? Long.parseLong(args[3]) : 200;          // 请求间隔

        // 计算总请求数：确保每个GPU都能处理足够的完整批次
        int totalRequests = parallelism * batchSize * batchesPerNode;

        logger.info("🎯 p×b联合优化配置:");
        logger.info("  并行度(p): {} —— GPU节点个数", parallelism);
        logger.info("  批大小(b): {} —— 批次内请求数", batchSize);
        logger.info("  每节点处理的总批次数: {}", batchesPerNode);
        logger.info("  总请求数: {} = {}×{}×{}", totalRequests, parallelism, batchSize, batchesPerNode);
        logger.info("  请求间隔: {}ms", interval);
        logger.info("  预期总批次: {}", parallelism * batchesPerNode);

        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        // 配置全局参数
        Configuration config = new Configuration();
        config.setString("batch.size", String.valueOf(batchSize));
        config.setString("parallelism", String.valueOf(parallelism));
        config.setString("total.requests", String.valueOf(totalRequests));
        config.setString("batches.per.gpu", String.valueOf(batchesPerNode));
        env.getConfig().setGlobalJobParameters(config);

        logger.info("✅ 全局参数配置完成");

        // 理论性能预测
        // double theoreticalThroughputPerGpu = 1000.0 / (300 + batchSize * 50); // 假设单请求50ms推理时间
        // double theoreticalTotalThroughput = theoreticalThroughputPerGpu * parallelism;
        // double theoreticalAvgWaitTime = (batchSize - 1) * interval / 2.0; // 平均等待时间
        //
        // logger.info("📈 理论性能预测:");
        // logger.info("  单GPU吞吐量: {} req/s", theoreticalThroughputPerGpu);
        // logger.info("  总吞吐量: {} req/s", theoreticalTotalThroughput);
        // logger.info("  平均等待时间: {}ms", theoreticalAvgWaitTime);

        DataStream<InferenceRequest> requests = env
                .addSource(new BasicRequestSource(totalRequests, interval))
                .name("Joint Optimization Request Source");

        DataStream<InferenceResponse> responses = requests
                .rebalance()
                .process(new ParallelBatchProcessor())
                .name("Parallel Batch Processor");

        String experimentId = String.format("p%db%d_%dreq", parallelism, batchSize, totalRequests);
        responses.addSink(new JointOptimizationSink(experimentId, parallelism, batchSize, interval))
                .name("Joint Optimization Performance Sink").setParallelism(1);

        logger.info("🚀 p×b联合优化流水线构建完成");
        // logger.info("📊 核心特点:");
        // logger.info("  1. 请求通过rebalance()自动轮询分发到{}个GPU", parallelism);
        // logger.info("  2. 每个GPU独立攒批{}个请求（使用内存缓冲）", batchSize);
        // logger.info("  3. 真实GPU并行处理，无资源冲突");
        // logger.info("  4. 精确测量等待时间、处理时间、吞吐量");
        // logger.info("  5. 避免Flink State和keyBy的复杂性");

        // logger.info("📈 预期验证效果:");
        // if (batchSize == 1) {
        //     logger.info("  - b=1: 无攒批开销，延迟最低，但吞吐量受启动开销限制");
        // } else {
        //     logger.info("  - b={}: 攒批分摊启动开销，提升吞吐量，但增加等待时间", batchSize);
        // }
        //
        // if (parallelism == 1) {
        //     logger.info("  - p=1: 单GPU限制总吞吐量");
        // } else {
        //     logger.info("  - p={}: 多GPU并行，吞吐量线性扩展（理想情况）", parallelism);
        // }

        // 执行
        env.execute("InferTuner p×b Joint Optimization Test");

        logger.info("=== p×b联合优化验证完成 ===");
    }
}

/*
 * 🔧 使用说明：
 *
 * 1. 编译：mvn clean package
 *
 * 2. 单次运行示例：
 * $FLINK_HOME/bin/flink run -c com.infertuner.jobs.JointOptimizationJob \
 *   target/infertuner-1.0.0.jar 2 4 6 200
 *   # 含义：2GPU，批大小4，每GPU 6个批次，请求间隔200ms
 *   # 总请求：2×4×6=48个
 *
 * 3. 配合脚本进行参数扫描：
 * ./run_joint_optimization.sh
 *
 * 4. 关键验证点：
 * - 并行度扩展性：p1 vs p2 vs p4 的吞吐量提升
 * - 批大小优化：b1 vs b2 vs b4 vs b8 的延迟/吞吐量权衡
 * - 资源利用率：高并行度下的GPU利用效率
 * - 等待时间分布：不同批大小下的请求等待时间
 *
 */