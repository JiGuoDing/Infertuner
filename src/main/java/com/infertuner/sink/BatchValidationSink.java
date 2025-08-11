package com.infertuner.sink;

import com.infertuner.model.InferenceResponse;
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

public class BatchValidationSink implements SinkFunction<InferenceResponse> {
    
    private static final Logger logger = LoggerFactory.getLogger(BatchValidationSink.class);
    
    private final AtomicInteger responseCount = new AtomicInteger(0);
    private final List<ResponseRecord> records = new ArrayList<>();
    private final Map<Integer, AtomicInteger> batchSizeStats = new ConcurrentHashMap<>();
    private long startTime = System.currentTimeMillis();
    private long firstResponseTime = 0;  // 记录第一个响应时间
    private long lastResponseTime = 0;   // 记录最后一个响应时间
    
    @Override
    public void invoke(InferenceResponse response, Context context) throws Exception {
        int count = responseCount.incrementAndGet();
        long currentTime = System.currentTimeMillis();
        
        // 记录第一个和最后一个响应时间，用于计算吞吐量
        if (firstResponseTime == 0) {
            firstResponseTime = currentTime;
        }
        lastResponseTime = currentTime;
        
        // 记录详细信息
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
        
        // 实时日志输出 - 增加关键信息用于脚本解析
        if (response.batchSize > 0) {  // 只记录真正处理的批次
            logger.info("✅ 响应 #{}: {} | 批大小={} | 等待={}ms | 处理={}ms | 总延迟={}ms", 
                       count,
                       response.requestId,
                       response.batchSize,
                       response.waitTimeMs,
                       response.batchProcessTimeMs,
                       response.totalLatencyMs);
        } else {
            logger.info("⏳ 响应 #{}: {} | 等待批处理中...", count, response.requestId);
        }
        
        // 每5个响应输出一次统计
        if (count % 5 == 0) {
            outputCurrentStats();
        }
        
        // 在结束时保存报告 - 调整触发条件
        if (count >= 25) {  // 适应更多的请求数量
            saveBatchValidationReport();
            outputFinalThroughputStats();
        }
    }
    
    private void outputCurrentStats() {
        long currentTime = System.currentTimeMillis();
        double elapsedSeconds = (currentTime - startTime) / 1000.0;
        
        synchronized (records) {
            if (records.isEmpty()) return;
            
            // 只统计真正处理的响应（batchSize > 0）
            List<ResponseRecord> processedRecords = new ArrayList<>();
            for (ResponseRecord r : records) {
                if (r.batchSize > 0 && r.success) {
                    processedRecords.add(r);
                }
            }
            
            if (processedRecords.isEmpty()) return;
            
            int totalProcessed = processedRecords.size();
            
            double avgWaitTime = processedRecords.stream().mapToDouble(r -> r.waitTimeMs).average().orElse(0);
            double avgProcessTime = processedRecords.stream().mapToDouble(r -> r.batchProcessTimeMs).average().orElse(0);
            double avgTotalLatency = processedRecords.stream().mapToDouble(r -> r.totalLatencyMs).average().orElse(0);
            
            double maxWaitTime = processedRecords.stream().mapToDouble(r -> r.waitTimeMs).max().orElse(0);
            double maxProcessTime = processedRecords.stream().mapToDouble(r -> r.batchProcessTimeMs).max().orElse(0);
            double maxTotalLatency = processedRecords.stream().mapToDouble(r -> r.totalLatencyMs).max().orElse(0);
            
            double throughput = totalProcessed / elapsedSeconds;
            
            logger.info("=== 批处理验证统计 ===");
            logger.info("已处理响应: {} | 吞吐量: {:.2f} req/s", totalProcessed, throughput);
            logger.info("延迟统计:");
            logger.info("  等待时间: 平均={:.1f}ms, 最大={:.1f}ms", avgWaitTime, maxWaitTime);
            logger.info("  处理时间: 平均={:.1f}ms, 最大={:.1f}ms", avgProcessTime, maxProcessTime);
            logger.info("  总延迟:   平均={:.1f}ms, 最大={:.1f}ms", avgTotalLatency, maxTotalLatency);
            
            // 批大小分布统计
            logger.info("批大小分布:");
            batchSizeStats.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    if (entry.getKey() > 0) {  // 只显示真正的批次
                        logger.info("  批大小 {}: {} 次", entry.getKey(), entry.getValue().get());
                    }
                });
            logger.info("=======================");
        }
    }
    
    /**
     * 输出最终的吞吐量统计 - 供脚本解析使用
     */
    private void outputFinalThroughputStats() {
        synchronized (records) {
            List<ResponseRecord> processedRecords = new ArrayList<>();
            for (ResponseRecord r : records) {
                if (r.batchSize > 0 && r.success) {
                    processedRecords.add(r);
                }
            }
            
            if (processedRecords.isEmpty()) return;
            
            int totalProcessed = processedRecords.size();
            double avgWaitTime = processedRecords.stream().mapToDouble(r -> r.waitTimeMs).average().orElse(0);
            double avgTotalLatency = processedRecords.stream().mapToDouble(r -> r.totalLatencyMs).average().orElse(0);
            
            // 计算基于实际处理时间的吞吐量
            double actualThroughputSeconds = 0;
            if (firstResponseTime > 0 && lastResponseTime > firstResponseTime) {
                actualThroughputSeconds = (lastResponseTime - firstResponseTime) / 1000.0;
            }
            
            double throughput = 0;
            if (actualThroughputSeconds > 0) {
                throughput = totalProcessed / actualThroughputSeconds;
            }
            
            // 输出供脚本解析的关键统计信息
            logger.info("🎯 === 最终权衡统计 (供脚本解析) ===");
            logger.info("处理请求数: {}", totalProcessed);
            logger.info("等待={:.0f}ms", avgWaitTime);
            logger.info("总延迟={:.0f}ms", avgTotalLatency);
            logger.info("吞吐量={:.2f}req/s", throughput);
            logger.info("执行时间={:.2f}s", actualThroughputSeconds);
            logger.info("=======================================");
        }
    }
    
    private void saveBatchValidationReport() {
        try (FileWriter writer = new FileWriter("batch_validation_report.csv")) {
            writer.write("response_id,request_id,batch_size,wait_time_ms,process_time_ms,total_latency_ms,success,timestamp\n");
            
            synchronized (records) {
                for (ResponseRecord record : records) {
                    writer.write(String.format("%d,%s,%d,%.2f,%.2f,%.2f,%s,%d\n",
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
            
            logger.info("✅ 批处理验证报告已保存到 batch_validation_report.csv");
            
        } catch (IOException e) {
            logger.error("保存验证报告失败", e);
        }
    }
    
    // 响应记录
    private static class ResponseRecord {
        final int responseId;
        final String requestId;
        final int batchSize;
        final double waitTimeMs;
        final double batchProcessTimeMs;
        final double totalLatencyMs;
        final boolean success;
        final long timestamp;
        
        ResponseRecord(int responseId, String requestId, int batchSize, 
                      double waitTimeMs, double batchProcessTimeMs, double totalLatencyMs,
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