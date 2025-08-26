package com.infertuner.models;

/*
    支持的 LLM 模型枚举
 */
public enum LLMModel {
    // 枚举常量，指定模型名称和路径
    Falcon3("Falcon3-7B-Instruct", "/mnt/tidal-alsh01/usr/suqian/models/Falcon3-7B-Instruct"),
    Llama2("llama-2-13B", "/mnt/tidal-alsh01/usr/suqian/models/llama-2-13B"),
    Qwen3("Qwen3-30B-A3B-Instruct", "/mnt/tidal-alsh01/usr/suqian/models/Qwen3-30B-A3B-Instruct");

    private final String modelName;
    private final String modelPath;

    LLMModel(String modelName, String modelPath) {
        this.modelName = modelName;
        this.modelPath = modelPath;
    }

    public String getModelName() {
        return modelName;
    }

    public String getModelPath() {
        return modelPath;
    }

    public LLMModel fromModelName(String name) {
        for (LLMModel model : LLMModel.values()) {
            if (model.getModelName().equalsIgnoreCase(name)) {
                return model;
            }
        }
        // 未找到对应模型
        throw  new IllegalArgumentException("No LLMModel found for model name: " + name);
    }
}
