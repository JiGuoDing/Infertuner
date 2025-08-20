package com.infertuner.sinks;

import com.infertuner.models.InferenceResponse;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;

/**
 * p×b联合优化性能统计汇聚器
 * 核心功能：
 * 1. 收集并行度(p)和批大小(b)的联合性能数据
 * 2. 统计每个GPU的处理情况和负载均衡
 * 3. 生成用于参数优化的详细指标
 */
public class JointOptimizationSink extends RichSinkFunction<InferenceResponse> {

    private static final Logger logger = LoggerFactory.getLogger(JointOptimizationSink.class);

    private final String experimentId;
    private final int parallelism;
    private final int batchSize;

    // 全局统计变量（跨所有Sink实例共享）
    private static final AtomicInteger globalTotalRequests = new AtomicInteger(0);
    private static final AtomicInteger globalSuccessRequests = new AtomicInteger(0);
    private static final AtomicLong globalTotalInferenceTime = new AtomicLong(0);
    private static final AtomicLong globalTotalWaitTime = new AtomicLong(0);
    private static final AtomicLong globalTotalLatency = new AtomicLong(0);
    private static final AtomicInteger globalTotalBatches = new AtomicInteger(0);

    private static final String[] CSV_HEADER = {
        "experiment_id",
        "parallelism",
        "batch_size",
        "total_requests",
        "success_requests",
        "success_rate_pct",
        "throughput_rps",
        "avg_latency_ms",
        "avg_wait_ms",
        "avg_inference_ms",
        "processing_time_sec",
        "actual_batches",
        "avg_batch_size",
        "load_balance_pct",
        "resource_utilization_pct"
    };

    // 节点分布统计
    private static final ConcurrentHashMap<String, AtomicInteger> nodeRequestsCount = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, AtomicInteger> batchSizeDistribution = new ConcurrentHashMap<>();

    // 时间跟踪
    private static volatile long globalStartTime = 0;
    private static volatile long globalFirstResponseTime = 0;
    private static volatile long globalLastResponseTime = 0;
    private static volatile boolean globalFinalStatsOutputted = false;

    // 实例变量
    private final AtomicInteger localRequests = new AtomicInteger(0);
    private final int expectedTotalRequests;

    public JointOptimizationSink(String experimentId, int parallelism, int batchSize) {
        this.experimentId = experimentId;
        this.parallelism = parallelism;
        this.batchSize = batchSize;

        // 从experimentId解析预期请求数
        this.expectedTotalRequests = parseExpectedRequestsFromId(experimentId);

        // 重置全局统计
        synchronized (JointOptimizationSink.class) {
            globalTotalRequests.set(0);
            globalSuccessRequests.set(0);
            globalTotalInferenceTime.set(0);
            globalTotalWaitTime.set(0);
            globalTotalLatency.set(0);
            globalTotalBatches.set(0);
            nodeRequestsCount.clear();
            batchSizeDistribution.clear();
            globalStartTime = System.currentTimeMillis();
            globalFirstResponseTime = 0;
            globalLastResponseTime = 0;
            globalFinalStatsOutputted = false;
        }

        logger.info("p×b联合优化统计初始化: 实验={}, p={}, b={}, 预期请求={}",
                experimentId, parallelism, batchSize, expectedTotalRequests);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 1. 获取输出目录（支持在 flink-conf.yaml 中覆盖）
        Configuration cfg = (Configuration) getRuntimeContext()
                .getExecutionConfig().getGlobalJobParameters();
        String baseDir = cfg.getString(
                "pipeline.job-experiment.output-dir",
                "/tmp/flink-exp-results"
        );

        FileSystem fs = FileSystem.get(new URI(baseDir));
        Path dir = new Path(baseDir);

        // 2. 确保目录存在
        fs.mkdirs(dir);

        // 3. 生成输出文件路径
        String safeExperimentId = experimentId.replaceAll("[^a-zA-Z0-9\\-]", "_");
        Path csvPath = new Path(dir, String.format("p%db%d_%s.csv", parallelism, batchSize, safeExperimentId));

        // 4. 如果文件不存在则写入表头
        if (!fs.exists(csvPath)) {
            try (
                    FSDataOutputStream out = fs.create(csvPath, FileSystem.WriteMode.NO_OVERWRITE);
                    PrintWriter pw = new PrintWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
            ) {
                pw.println(String.join(",", CSV_HEADER));
            }
        }
    }


    private int parseExpectedRequestsFromId(String experimentId) {
        try {
            if (experimentId.contains("req")) {
                String[] parts = experimentId.split("_");
                for (String part : parts) {
                    if (part.endsWith("req")) {
                        return Integer.parseInt(part.replace("req", ""));
                    }
                }
            }
        } catch (Exception e) {
            // 静默处理
        }
        return parallelism * batchSize * 6;
    }

    @Override
    public void invoke(InferenceResponse response, Context context) {
        long currentTime = System.currentTimeMillis();

        int globalCount = globalTotalRequests.incrementAndGet();
        int localCount = localRequests.incrementAndGet();

        // 更新全局时间跟踪
        if (globalFirstResponseTime == 0) {
            synchronized (JointOptimizationSink.class) {
                if (globalFirstResponseTime == 0) {
                    globalFirstResponseTime = currentTime;
                }
            }
        }
        globalLastResponseTime = currentTime;

        // 更新全局统计
        if (response.success) {
            globalSuccessRequests.incrementAndGet();
            globalTotalInferenceTime.addAndGet((long) response.inferenceTimeMs);

            if (response.waitTimeMs > 0) {
                globalTotalWaitTime.addAndGet(response.waitTimeMs);
            }
            if (response.totalLatencyMs > 0) {
                globalTotalLatency.addAndGet(response.totalLatencyMs);
            } else {
                globalTotalLatency.addAndGet((long) response.inferenceTimeMs);
            }
        }

        // 统计节点分布
        String nodeIP = response.modelName != null ? response.modelName : "Unknown-GPU";
        nodeRequestsCount.computeIfAbsent(nodeIP, k -> new AtomicInteger(0)).incrementAndGet();

        // 统计批大小分布
        batchSizeDistribution.computeIfAbsent(response.batchSize, k -> new AtomicInteger(0)).incrementAndGet();

        // 日志输出
        if (localCount % 10 == 0) {
            logger.info("响应 #{}: {} | 节点: {} | 批大小: {} | 等待: {}ms | {}",
                    globalCount, response.requestId, nodeIP, response.batchSize,
                    response.waitTimeMs, response.success ? "✅" : "❌");
        }

        // 检查是否输出最终统计 - 修复：确保能正确触发统计输出
        if (globalCount >= expectedTotalRequests && !globalFinalStatsOutputted) {
            logger.info("触发最终统计: 收到{}个请求，预期{}个", globalCount, expectedTotalRequests);
            outputGlobalFinalStatsOnce();
        } else if (globalCount > expectedTotalRequests && !globalFinalStatsOutputted) {
            // 防止请求数超过预期时遗漏统计
            logger.warn("请求数{}超过预期{}，强制输出统计", globalCount, expectedTotalRequests);
            outputGlobalFinalStatsOnce();
        }
    }

    private synchronized void outputGlobalFinalStatsOnce() {
        if (!globalFinalStatsOutputted) {
            globalFinalStatsOutputted = true;
            outputGlobalFinalStats();
        }
    }

    private void outputGlobalFinalStats() {
        int total = globalTotalRequests.get();
        int success = globalSuccessRequests.get();

        if (total == 0) {
            logger.warn("没有接收到任何请求");
            return;
        }

        // 计算核心性能指标
        double actualProcessingTime;
        if (globalFirstResponseTime > 0 && globalLastResponseTime > globalFirstResponseTime) {
            actualProcessingTime = (globalLastResponseTime - globalFirstResponseTime) / 1000.0;
        } else {
            actualProcessingTime = (System.currentTimeMillis() - globalStartTime) / 1000.0;
        }

        double throughput = actualProcessingTime > 0 ? success / actualProcessingTime : 0.0;
        double avgLatency = success > 0 ? globalTotalLatency.get() / (double) success : 0.0;
        double avgWait = success > 0 ? globalTotalWaitTime.get() / (double) success : 0.0;
        double avgInference = success > 0 ? globalTotalInferenceTime.get() / (double) success : 0.0;
        double successRate = total > 0 ? (success * 100.0) / total : 0.0;

        // 计算GPU负载均衡指标
        double gpuLoadBalance = calculateLoadBalance();
        int actualBatches = calculateActualBatches();
        double avgBatchSize = actualBatches > 0 ? (double) success / actualBatches : 0.0;

        // 计算资源利用率
        double theoreticalMaxThroughput = parallelism * (1000.0 / (300 + batchSize * 50)); // 理论最大吞吐量
        double resourceUtilization = theoreticalMaxThroughput > 0 ? (throughput / theoreticalMaxThroughput) * 100 : 0.0;

        logger.info("================================================");
        logger.info("=== p×b联合优化最终统计 ===");
        logger.info("================================================");
        logger.info("实验配置: p={}, b={} ({})", parallelism, batchSize, experimentId);
        logger.info("------------------------------------------------");
        logger.info("🔢 请求统计:");
        logger.info("  总请求: {}", total);
        logger.info("  成功请求: {}", success);
        logger.info("  成功率: {}%", String.format("%.1f", successRate));
        logger.info("------------------------------------------------");
        logger.info("⚡ 性能指标:");
        logger.info("  吞吐量: {} req/s", String.format("%.2f", throughput));
        logger.info("  平均延迟: {}ms", Math.round(avgLatency));
        logger.info("  平均等待: {}ms", Math.round(avgWait));
        logger.info("  平均推理: {}ms", Math.round(avgInference));
        logger.info("  处理时间: {}s", String.format("%.1f", actualProcessingTime));
        logger.info("------------------------------------------------");
        logger.info("🔧 并行度分析:");
        logger.info("  并行度(p): {}", parallelism);
        logger.info("  批大小(b): {}", batchSize);
        logger.info("  实际批次数: {}", actualBatches);
        logger.info("  平均批大小: {}", String.format("%.1f", avgBatchSize));
        logger.info("  负载均衡: {}%", String.format("%.1f", gpuLoadBalance));
        logger.info("  资源利用率: {}%", String.format("%.1f", resourceUtilization));
        logger.info("------------------------------------------------");
        logger.info("📊 请求分布:");
        nodeRequestsCount.forEach((node, count) -> {
            double percentage = total > 0 ? (count.get() * 100.0) / total : 0.0;
            logger.info("  节点{}: {} 请求 ({}%)", node, count.get(), String.format("%.1f", percentage));
        });
        logger.info("------------------------------------------------");
        logger.info("📦 批大小分布:");
        batchSizeDistribution.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    double percentage = total > 0 ? (entry.getValue().get() * 100.0) / total : 0.0;
                    logger.info("  批大小{}: {} 请求 ({}%)",
                            entry.getKey(), entry.getValue().get(), String.format("%.1f", percentage));
                });
        logger.info("================================================");

        // 关键性能总结
        logger.info("🎯 关键指标总结:");
        logger.info("  配置: p{}b{}", parallelism, batchSize);
        logger.info("  吞吐量: {} req/s", String.format("%.2f", throughput));
        logger.info("  平均延迟: {}ms", Math.round(avgLatency));
        logger.info("  GPU利用率: {}%", String.format("%.1f", resourceUtilization));
        logger.info("================================================");
    }

    private double calculateLoadBalance() {
        if (nodeRequestsCount.isEmpty()) return 0.0;

        int total = globalTotalRequests.get();
        double idealRequestsPerGpu = (double) total / parallelism;

        double variance = 0.0;
        for (AtomicInteger count : nodeRequestsCount.values()) {
            double diff = count.get() - idealRequestsPerGpu;
            variance += diff * diff;
        }
        variance /= nodeRequestsCount.size();

        double standardDeviation = Math.sqrt(variance);
        double coefficientOfVariation = idealRequestsPerGpu > 0 ? standardDeviation / idealRequestsPerGpu : 0.0;

        // 负载均衡百分比：越接近100%表示负载越均衡
        return Math.max(0, Math.min(100, (1.0 - coefficientOfVariation) * 100));
    }

    private int calculateActualBatches() {
        // 估算实际批次数：假设大部分请求都在完整批次中
        int successRequests = globalSuccessRequests.get();
        return (int) Math.ceil((double) successRequests / batchSize);
    }
}