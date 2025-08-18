package com.infertuner.sinks;

import com.infertuner.models.InferenceResponse;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 统一性能统计汇聚器 - 支持多实例全局统计
 */
public class UnifiedPerformanceSink implements SinkFunction<InferenceResponse> {

    private static final Logger logger = LoggerFactory.getLogger(UnifiedPerformanceSink.class);

    public enum ExperimentType {
        GPU_SCALING,
        BATCH_ANALYSIS
    }

    private final ExperimentType experimentType;
    private final String experimentId;

    // 全局统计变量（跨所有Sink实例共享）
    private static final AtomicInteger globalTotalRequests = new AtomicInteger(0);
    private static final AtomicInteger globalSuccessRequests = new AtomicInteger(0);
    private static final AtomicLong globalTotalInferenceTime = new AtomicLong(0);
    private static final AtomicLong globalTotalWaitTime = new AtomicLong(0);
    private static final AtomicLong globalTotalLatency = new AtomicLong(0);
    private static volatile long globalStartTime = 0;
    private static volatile long globalFirstResponseTime = 0;
    private static volatile long globalLastResponseTime = 0;
    private static volatile boolean globalFinalStatsOutputted = false;

    // 实例配置
    private final AtomicInteger localRequests = new AtomicInteger(0);
    private final int totalExpectedRequests;
    private final int parallelism;

    public UnifiedPerformanceSink(ExperimentType experimentType, String experimentId) {
        this.experimentType = experimentType;
        this.experimentId = experimentId;

        this.totalExpectedRequests = parseTotalRequestsFromId(experimentId);
        this.parallelism = parseParallelismFromId(experimentId);

        // 重置全局统计
        synchronized (UnifiedPerformanceSink.class) {
            globalTotalRequests.set(0);
            globalSuccessRequests.set(0);
            globalTotalInferenceTime.set(0);
            globalTotalWaitTime.set(0);
            globalTotalLatency.set(0);
            globalStartTime = System.currentTimeMillis();
            globalFirstResponseTime = 0;
            globalLastResponseTime = 0;
            globalFinalStatsOutputted = false;
        }

        logger.info("统计配置: 实验类型={}, 总请求数={}, 并行度={}",
                experimentType, totalExpectedRequests, parallelism);
    }

    private int parseTotalRequestsFromId(String experimentId) {
        try {
            if (experimentId.contains("requests")) {
                String[] parts = experimentId.split("-");
                for (String part : parts) {
                    part = part.trim();
                    if (part.endsWith("requests")) {
                        return Integer.parseInt(part.replace("requests", "").strip());
                    }
                }
            }
        } catch (Exception e) {
            // 静默处理
        }
        return experimentType == ExperimentType.BATCH_ANALYSIS ? 32 : 50;
    }

    private int parseParallelismFromId(String experimentId) {
        try {
            String[] parts = experimentId.split("-");
            for (String part : parts) {
                part = part.trim();
                if (part.endsWith("nodes")) {
                    return Integer.parseInt(part.replace("nodes", ""));
                } else if (part.startsWith("batch")) {
                    return 1; // batch实验是单并行度
                }
            }
        } catch (Exception e) {
            // 静默处理
        }
        return 1;
    }

    @Override
    public void invoke(InferenceResponse response, Context context) throws Exception {
        long currentTime = System.currentTimeMillis();

        int globalCount = globalTotalRequests.incrementAndGet();
        int localCount = localRequests.incrementAndGet();

        // 更新全局时间跟踪
        if (globalFirstResponseTime == 0) {
            synchronized (UnifiedPerformanceSink.class) {
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

        logger.info("globalCount: {}, totalExpectedRequests: {}", globalCount, totalExpectedRequests);
        // 日志输出
        if (localCount <= 5 || localCount % 5 == 0) {
            logger.info("结果 #{}: {} | {}", globalCount, response.requestId,
                    response.success ? "✅" : "❌");
            outputGlobalFinalStats();
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

        // 计算吞吐量
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

        logger.info("================================================");
        logger.info("=== 最终统计 ({}) ===", experimentType);
        logger.info("================================================");
        logger.info("总请求: {}", total);
        logger.info("成功请求: {}", success);
        logger.info("吞吐量: {}req/s", String.format("%.2f", throughput));
        logger.info("平均延迟: {}ms", Math.round(avgLatency));
        logger.info("平均等待: {}ms", Math.round(avgWait));
        logger.info("平均推理: {}ms", Math.round(avgInference));
        logger.info("================================================");
    }
}