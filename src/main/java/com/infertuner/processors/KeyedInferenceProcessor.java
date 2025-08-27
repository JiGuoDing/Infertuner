package com.infertuner.processors;

import java.net.UnknownHostException;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.infertuner.models.InferenceRequest;
import com.infertuner.models.InferenceResponse;

public class KeyedInferenceProcessor extends KeyedProcessFunction<String, InferenceRequest, InferenceResponse> {
    private static final Logger logger = LoggerFactory.getLogger(KeyedInferenceProcessor.class);

    String nodeIP;

    @Override
    public void open(Configuration parameters) throws Exception {
        // TODO 启动对应模型的推理服务
        super.open(parameters);

        // 获取的当前节点IP
        try {
            nodeIP = java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.error("获取当前节点IP失败", e);
            nodeIP = "Unknown-hostIP";
        }
    }

    @Override
    public void processElement(InferenceRequest value,
            KeyedProcessFunction<String, InferenceRequest, InferenceResponse>.Context ctx,
            Collector<InferenceResponse> out) throws Exception {
        // TODO [攒批]推理
    }

    private class ResponseData {
        private String requestID;
        private String responseText;
        private boolean success;

        ResponseData() {
        };

        ResponseData(String requestID, String responseText, boolean success) {
            this.requestID = requestID;
            this.responseText = responseText;
            this.success = success;
        }

        public String getRequestID() {
            return requestID;
        }

        public void setRequestID(String requestID) {
            this.requestID = requestID;
        }

        public String getResponseText() {
            return responseText;
        }

        public void setResponseText(String responseText) {
            this.responseText = responseText;
        }

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }
    }
}
