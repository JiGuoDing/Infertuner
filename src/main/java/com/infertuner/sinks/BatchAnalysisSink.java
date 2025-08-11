package com.infertuner.sinks;

import com.infertuner.models.InferenceResponse;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 批处理分析结果汇聚器
 * 用于验证不同batch_size下的延迟vs吞吐量权衡关系
 */
public class BatchAnalysisSink implements SinkFunction<InferenceResponse> {
    
    private static final Logger logger = LoggerFactory.getLogger(BatchAnalysisSink.class);
    
    // 统计计数器
    private final AtomicInteger responseCount = new AtomicInteger(0);
    private final List<ResponseRecord> records = new ArrayList<>();
    private final Map<Integer, AtomicInteger> batchSizeStats = new ConcurrentHashMap<>();
    
    // 时间跟踪
    private long startTime = System.currentTimeMillis();
    private long firstResponseTime = 0;
    private long lastResponseTime = 0;
    
    @Override
    public void invoke(InferenceResponse response, Context context) throws Exception {
        int count = responseCount.incrementAndGet();
        long currentTime = System.currentTimeMillis();
        
        // 记录时间范围用于吞吐量计算
        if (firstResponseTime == 0) {
            firstResponseTime = currentTime;
        }
        lastResponseTime = currentTime;
        
        // 记录响应详情
        ResponseRecord record = new ResponseRecord(
            count,
            response.requestId,
            response.batchSize,
            response.waitTimeMs,
            response.batchProcessTimeMs,
            response.totalLatencyMs,
            response.success,
            currentTime
        );
        
        synchronized (records) {
            records.add(record);
        }
        
        // 统计批大小分布
        batchSizeStats.computeIfAbsent(response.batchSize, k -> new AtomicInteger()).incrementAndGet();
        
        // 实时日志输出
        if (response.batchSize > 0) {
            logger.info("[{}] Batch-{} | 等待: {}ms | 处理: {}ms | 总计: {}ms", 
                       response.requestId,
                       response.batchSize,
                       response.waitTimeMs,
                       response.batchProcessTimeMs,
                       response.totalLatencyMs);
        }
        
        // 定期输出统计
        if (count % 5 == 0) {
            outputCurrentStats();
        }
        
        // 达到阈值时输出最终统计
        if (count >= 25) {
            saveBatchAnalysisReport();
            outputFinalStats();
        }
    }
    
    /**
     * 输出当前统计信息
     */
    private void outputCurrentStats() {
        long currentTime = System.currentTimeMillis();
        double elapsedSeconds = (currentTime - startTime) / 1000.0;
        
        synchronized (records) {
            if (records.isEmpty()) return;
            
            // 只统计成功的处理记录
            List<ResponseRecord> validRecords = new ArrayList<>();
            for (ResponseRecord r : records) {
                if (r.batchSize > 0 && r.success) {
                    validRecords.add(r);
                }
            }
            
            if (validRecords.isEmpty()) return;
            
            int totalProcessed = validRecords.size();
            double avgWaitTime = validRecords.stream().mapToDouble(r -> r.waitTimeMs).average().orElse(0);
            double avgProcessTime = validRecords.stream().mapToDouble(r -> r.batchProcessTimeMs).average().orElse(0);
            double avgTotalLatency = validRecords.stream().mapToDouble(r -> r.totalLatencyMs).average().orElse(0);
            double throughput = totalProcessed / elapsedSeconds;
            
            logger.info("=== 批处理分析中期统计 ===");
            logger.info("已处理: {} 请求 | 吞吐量: {:.2f} req/s", totalProcessed, throughput);
            logger.info("平均等待: {:.1f}ms | 平均处理: {:.1f}ms | 平均总延迟: {:.1f}ms", 
                       avgWaitTime, avgProcessTime, avgTotalLatency);
            
            // 批大小分布
            logger.info("批大小分布:");
            batchSizeStats.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    if (entry.getKey() > 0) {
                        logger.info("  Batch-{}: {} 个响应", entry.getKey(), entry.getValue().get());
                    }
                });
            logger.info("========================");
        }
    }
    
    /**
     * 输出最终统计信息（供脚本解析）
     */
    private void outputFinalStats() {
        synchronized (records) {
            List<ResponseRecord> validRecords = new ArrayList<>();
            for (ResponseRecord r : records) {
                if (r.batchSize > 0 && r.success) {
                    validRecords.add(r);
                }
            }
            
            if (validRecords.isEmpty()) return;
            
            int totalProcessed = validRecords.size();
            double avgWaitTime = validRecords.stream().mapToDouble(r -> r.waitTimeMs).average().orElse(0);
            double avgTotalLatency = validRecords.stream().mapToDouble(r -> r.totalLatencyMs).average().orElse(0);
            
            // 计算准确的吞吐量
            double actualThroughputSeconds = 0;
            if (firstResponseTime > 0 && lastResponseTime > firstResponseTime) {
                actualThroughputSeconds = (lastResponseTime - firstResponseTime) / 1000.0;
            }
            
            double throughput = 0;
            if (actualThroughputSeconds > 0) {
                throughput = totalProcessed / actualThroughputSeconds;
            }
            
            // 输出供脚本解析的格式化统计
            logger.info("=== 批处理分析最终统计 ===");
            logger.info("处理请求数: {}", totalProcessed);
            logger.info("等待={}ms", Math.round(avgWaitTime));
            logger.info("总延迟={}ms", Math.round(avgTotalLatency));
            logger.info("吞吐量={:.2f}req/s", throughput);
            logger.info("执行时间={:.2f}s", actualThroughputSeconds);
            logger.info("==============================");
        }
    }
    
    /**
     * 保存详细的分析报告
     */
    private void saveBatchAnalysisReport() {
        String fileName = "results/batch_analysis/batch_analysis_" + System.currentTimeMillis() + ".csv";
        
        try {
            // 确保目录存在
            new java.io.File("results/batch_analysis").mkdirs();
            
            try (FileWriter writer = new FileWriter(fileName)) {
                // CSV头部
                writer.write("response_id,request_id,batch_size,wait_time_ms,process_time_ms,total_latency_ms,success,timestamp\n");
                
                synchronized (records) {
                    for (ResponseRecord record : records) {
                        writer.write(String.format("%d,%s,%d,%d,%d,%d,%s,%d\n",
                            record.responseId,
                            record.requestId,
                            record.batchSize,
                            record.waitTimeMs,
                            record.batchProcessTimeMs,
                            record.totalLatencyMs,
                            record.success,
                            record.timestamp));
                    }
                }
            }
            
            logger.info("批处理分析报告已保存: {}", fileName);
            
        } catch (IOException e) {
            logger.error("保存批处理分析报告失败", e);
        }
    }
    
    /**
     * 响应记录数据结构
     */
    private static class ResponseRecord {
        final int responseId;
        final String requestId;
        final int batchSize;
        final long waitTimeMs;
        final long batchProcessTimeMs;
        final long totalLatencyMs;
        final boolean success;
        final long timestamp;
        
        ResponseRecord(int responseId, String requestId, int batchSize, 
                      long waitTimeMs, long batchProcessTimeMs, long totalLatencyMs,
                      boolean success, long timestamp) {
            this.responseId = responseId;
            this.requestId = requestId;
            this.batchSize = batchSize;
            this.waitTimeMs = waitTimeMs;
            this.batchProcessTimeMs = batchProcessTimeMs;
            this.totalLatencyMs = totalLatencyMs;
            this.success = success;
            this.timestamp = timestamp;
        }
    }
}