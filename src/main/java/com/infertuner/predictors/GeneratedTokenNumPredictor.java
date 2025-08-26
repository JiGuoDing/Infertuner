package com.infertuner.predictors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 生成 Token 数预测器
 */
public class GeneratedTokenNumPredictor {

    private static final Logger logger = LoggerFactory.getLogger(GeneratedTokenNumPredictor.class);
    // 一次性对多少请求进行批次预处理
    private static final int batchSize = 4;

    // 代理模型
    private String proxyModelName;
    private Path proxyModelPath;

    private Path pcaModelPath;
    private Path predictorPath;

    public GeneratedTokenNumPredictor(String proxyModelName, Path pcaModelPath, Path predictorPath) {
        this.proxyModelName = proxyModelName;
        this.proxyModelPath =  Paths.get("/mnt/tidal-alsh01/usr/suqian/models", this.proxyModelName);
        this.pcaModelPath = pcaModelPath;
        this.predictorPath = predictorPath;
    }

    public String getProxyModelName() {
        return proxyModelName;
    }

    public void setProxyModelName(String proxyModelName) {
        this.proxyModelName = proxyModelName;
    }

    public Path getProxyModelPath() {
        return proxyModelPath;
    }

    public void setProxyModelPath(Path proxyModelPath) {
        this.proxyModelPath = proxyModelPath;
    }

    public Path getPredictorPath() {
        return predictorPath;
    }

    public void setPredictorPath(Path predictorPath) {
        this.predictorPath = predictorPath;
    }
}
