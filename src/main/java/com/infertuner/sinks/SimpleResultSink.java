package com.infertuner.sinks;

import com.infertuner.models.InferenceResponse;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 简单结果输出汇聚器
 */
public class SimpleResultSink extends RichSinkFunction<InferenceResponse> {

    private static final Logger logger = LoggerFactory.getLogger(SimpleResultSink.class);

    private int totalCount = 0;
    private int successCount = 0;
    private double totalTime = 0.0;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void invoke(InferenceResponse response, Context context) throws Exception {
        totalCount++;

        // 输出单个结果
        logger.info("=== 结果 #{} ===", totalCount);
        logger.info("请求ID: {} | 用户: {}", response.requestId, response.userId);
        logger.info("问题: {}", truncate(response.userMessage, 100));
        logger.info("回答: {}", truncate(response.responseText, 200));

        String timeStr = String.format("%.1f", response.inferenceTimeMs);
        String statusStr = response.success ? "成功" : "失败";

        logger.info("状态: {} | 耗时: {}ms | 模型: {}", statusStr, timeStr, response.nodeIP);

        // 统计
        if (response.success) {
            successCount++;
            totalTime += response.inferenceTimeMs;
        }

        // 每5个输出统计
        if (totalCount % 5 == 0) {
            outputStats();
        }

        logger.info("================");
    }

    private void outputStats() {
        double avgTime = successCount > 0 ? totalTime / successCount : 0.0;
        double successRate = (double) successCount / totalCount * 100;

        // 格式化数值
        String avgTimeStr = String.format("%.1f", avgTime);
        String successRateStr = String.format("%.1f", successRate);

        logger.info("--- 统计 ---");
        logger.info("总数: {} | 成功: {} ({}%) | 平均耗时: {}ms",
                totalCount, successCount, successRateStr, avgTimeStr);
    }

    private String truncate(String text, int maxLen) {
        if (text == null)
            return "null";
        return text.length() > maxLen ? text.substring(0, maxLen) + "..." : text;
    }
}
