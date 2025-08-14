package com.infertuner.sinks;

import com.infertuner.models.InferenceResponse;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class GPUMonitorSink implements SinkFunction<InferenceResponse> {
    
    private static final Logger logger = LoggerFactory.getLogger(GPUMonitorSink.class);
    
    private final AtomicInteger totalCount = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicLong totalTime = new AtomicLong(0);
    
    private final Map<Integer, GPUStats> gpuStatsMap = new ConcurrentHashMap<>();
    
    private long startTime = System.currentTimeMillis();
    
    @Override
    public void invoke(InferenceResponse response, Context context) throws Exception {
        int count = totalCount.incrementAndGet();
        
        int gpuId = extractGPUId(response.modelName);
        
        if (response.success) {
            successCount.incrementAndGet();
            totalTime.addAndGet((long) response.inferenceTimeMs);
        }
        
        gpuStatsMap.computeIfAbsent(gpuId, k -> new GPUStats()).addResponse(response);
        
        if (count % 10 == 0) {
            outputStats();
        }
        
        if (count <= 5 || count % 20 == 0) {
            // 格式化延迟数值
            String latencyStr = String.format("%.1f", response.inferenceTimeMs);
            
            logger.info("结果 #{}: GPU-{} | {} | {}ms | {}", 
                       count, gpuId, 
                       response.success ? "✅" : "❌", 
                       latencyStr,
                       truncate(response.aiResponse, 60));
        }
        
        // 在作业结束时保存报告 - 只在最后几个请求时触发
        if (count >= 45) { // 当接近50个请求时开始检查
            saveMultiGPUReport();
        }
    }
    
    private void outputStats() {
        long currentTime = System.currentTimeMillis();
        double elapsedSeconds = (currentTime - startTime) / 1000.0;

        int total = totalCount.get();
        int success = successCount.get();
        double avgTime = success > 0 ? totalTime.get() / (double) success : 0.0;
        double throughput = total / elapsedSeconds;
        double successRate = total > 0 ? (double) success / total * 100 : 0.0;

        // 修复：提前格式化所有数值
        String successRateStr = String.format("%.1f", successRate);
        String avgTimeStr = String.format("%.1f", avgTime);
        String throughputStr = String.format("%.2f", throughput);

        logger.info("=== 整体统计 ===");
        logger.info("总请求: {} | 成功: {} ({}%) | 平均延迟: {}ms | 吞吐量: {} req/s",
                   total, success, successRateStr, avgTimeStr, throughputStr);

        logger.info("=== GPU分布统计 ===");
        for (Map.Entry<Integer, GPUStats> entry : gpuStatsMap.entrySet()) {
            int gpuId = entry.getKey();
            GPUStats stats = entry.getValue();

            double gpuThroughput = stats.count / elapsedSeconds;
            double gpuAvgTime = stats.successCount > 0 ? stats.totalTime / (double) stats.successCount : 0.0;
            double gpuSuccessRate = stats.count > 0 ? (double) stats.successCount / stats.count * 100 : 0.0;

            // 修复：提前格式化GPU统计数值
            String gpuSuccessRateStr = String.format("%.1f", gpuSuccessRate);
            String gpuAvgTimeStr = String.format("%.1f", gpuAvgTime);
            String gpuThroughputStr = String.format("%.2f", gpuThroughput);

            logger.info("GPU-{}: 请求={} | 成功率={}% | 平均延迟={}ms | 吞吐量={} req/s",
                       gpuId, stats.count, gpuSuccessRateStr, gpuAvgTimeStr, gpuThroughputStr);
        }
        logger.info("========================");
    }
    
    private int extractGPUId(String modelName) {
        if (modelName != null && modelName.contains("GPU-")) {
            try {
                int start = modelName.indexOf("GPU-") + 4;
                int end = modelName.indexOf(")", start);
                if (end > start) {
                    return Integer.parseInt(modelName.substring(start, end));
                }
            } catch (Exception e) {
                // 解析失败，返回默认值
            }
        }
        return 0;
    }
    
    private String truncate(String text, int maxLen) {
        if (text == null) return "null";
        return text.length() > maxLen ? text.substring(0, maxLen) + "..." : text;
    }
    
    public void saveMultiGPUReport() {
        try (FileWriter writer = new FileWriter("multi_gpu_performance.csv")) {
            writer.write("gpu_id,total_requests,success_requests,success_rate,avg_latency_ms,throughput_qps\n");
            
            long currentTime = System.currentTimeMillis();
            double elapsedSeconds = (currentTime - startTime) / 1000.0;
            
            for (Map.Entry<Integer, GPUStats> entry : gpuStatsMap.entrySet()) {
                int gpuId = entry.getKey();
                GPUStats stats = entry.getValue();
                
                double successRate = stats.count > 0 ? (double) stats.successCount / stats.count : 0.0;
                double avgLatency = stats.successCount > 0 ? stats.totalTime / (double) stats.successCount : 0.0;
                double throughput = stats.count / elapsedSeconds;
                
                writer.write(String.format("%d,%d,%d,%.4f,%.2f,%.2f\n",
                    gpuId, stats.count, stats.successCount, successRate, avgLatency, throughput));
            }
            
            logger.info("✅ 多GPU性能报告已保存到 multi_gpu_performance.csv");
            
        } catch (IOException e) {
            logger.error("保存多GPU报告失败", e);
        }
    }
    
    private static class GPUStats {
        volatile int count = 0;
        volatile int successCount = 0;
        volatile long totalTime = 0;
        
        void addResponse(InferenceResponse response) {
            count++;
            if (response.success) {
                successCount++;
                totalTime += (long) response.inferenceTimeMs;
            }
        }
    }
}