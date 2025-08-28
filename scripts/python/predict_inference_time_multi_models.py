"""
预测推理时长
"""
import os
import sys
import json
import math
import torch
import joblib
import logging

import pandas as pd
import numpy as np

from torch import nn
from transformers import AutoModel, AutoTokenizer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

os.environ["CUDA_VISIBLE_DEVICES"] = "0"

# llama-2-13B不自带chat模板，需要自定义
custom_chat_template = """
{% for message in messages %}
{{ '<|user|>' if message['role'] == 'user' else '<|assistant|>' }}
{{ message['content'] }}
{% endfor %}
<|assistant|>
"""



# 预测器网络结构
dropout = 0.3
class TokenPredictor(nn.Module):
    def __init__(self, input_dim):
        super(TokenPredictor, self).__init__()
        self.network = nn.Sequential(
            nn.Linear(input_dim, 1024),
            nn.BatchNorm1d(1024),
            nn.GELU(),
            nn.Dropout(dropout),

            nn.Linear(1024, 512),
            nn.BatchNorm1d(512),
            nn.GELU(),
            nn.Dropout(dropout),

            nn.Linear(512, 256),
            nn.BatchNorm1d(256),
            nn.GELU(),
            nn.Dropout(dropout),

            nn.Linear(256, 128),
            nn.BatchNorm1d(128),
            nn.GELU(),
            nn.Dropout(dropout),

            nn.Linear(128, 64),
            nn.BatchNorm1d(64),
            nn.GELU(),
            nn.Dropout(dropout),

            nn.Linear(64, 1)
        )

    def forward(self, x):
        return self.network(x)


class PredictInferenceTimeService():
    def __init__(self):
        """
        加载代理模型及其编码器、推理模型的编码器、预测器、PCA模型
        """
        logger.info(f"正在初始化预测推理时长服务")
        self.device = "cuda:0" if torch.cuda.is_available() else "cpu"
        logger.info(f"使用设备: {self.device}")

        # 预定义参数：
        self.temperature = {
            "Falcon3-7B-Instruct": 0.6,
            "llama-2-13B": 0.75,
            "Qwen3-30B-A3B-Instruct": 0.75
        }
        self.top_p = {
            "Falcon3-7B-Instruct": 0.65,
            "llama-2-13B": 0.85,
            "Qwen3-30B-A3B-Instruct": 0.85
        }
        self.top_k = 40
        self.repetition_penalty = 1.15
        self.max_new_tokens = {
            "Falcon3-7B-Instruct": 640,
            "llama-2-13B": 960,
            "Qwen3-30B-A3B-Instruct": 1280
        }

        # 加载代理模型及其编码器
        self.setup_proxy_tokenizer_model(device=self.device)

        # 加载 3 种推理模型的编码器
        self.inference_tokenizers = {}
        self.setup_inference_tokenizer()

        # 加载 token 数预测器
        self.token_num_predictors = {}
        self.setup_token_num_predictor(device=self.device)

        # 加载推理时间预测器
        self.inference_time_predictors = {}
        self.setup_inference_time_predictor()

        # 加载 PCA 模型
        self.pca_models = {}
        self.setup_pca_model()

    def setup_proxy_tokenizer_model(self, proxy_model_path: str = "/mnt/tidal-alsh01/usr/suqian/models/BGE-m3", device="cuda:0"):
        """
        加载代理模型
        :param proxy_model_path:
        :param device:
        :return:
        """

        logger.info(f"正在从 {proxy_model_path} 加载代理模型到设备 {device}")
        self.proxy_tokenizer = AutoTokenizer.from_pretrained(pretrained_model_name_or_path=proxy_model_path,
                                                             use_fast=True)
        self.proxy_model = AutoModel.from_pretrained(pretrained_model_name_or_path=proxy_model_path,
                                                     torch_dtype=torch.bfloat16)
        self.proxy_model.to(device).eval()

        logger.info(f"代理模型加载成功")

    def setup_token_num_predictor(self, predictor_dir_path: str = "/mnt/tidal-alsh01/usr/suqian/models/predict_models", device="cuda:0", pca_dim=64):
        """
        加载 token 数预测器
        :param pca_dim:
        :param device:
        :param predictor_dir_path:
        :return:
        """

        logger.info(f"正在从 {predictor_dir_path} 加载预测器到设备 {device}")

        self.token_num_predictors["Falcon3-7B-Instruct"] = TokenPredictor(2 * pca_dim + 5)
        self.token_num_predictors["llama-2-13B"] = TokenPredictor(2 * pca_dim + 5)
        self.token_num_predictors["Qwen3-30B-A3B-Instruct"] = TokenPredictor(2 * pca_dim + 5)

        falcon3_predictor_path = os.path.join(predictor_dir_path, f"best_mlp_model_Falcon3-7B-Instruct_pca{pca_dim}_ray.pth")
        llama2_predictor_path = os.path.join(predictor_dir_path, f"best_mlp_model_llama-2-13B_pca{pca_dim}_ray.pth")
        qwen3_predictor_path = os.path.join(predictor_dir_path, f"best_mlp_model_Qwen3-30B-A3B-Instruct_pca{pca_dim}_ray.pth")

        self.token_num_predictors["Falcon3-7B-Instruct"].load_state_dict(torch.load(falcon3_predictor_path, map_location=torch.device('cpu')))
        self.token_num_predictors["llama-2-13B"].load_state_dict(torch.load(llama2_predictor_path, map_location=torch.device('cpu')))
        self.token_num_predictors["Qwen3-30B-A3B-Instruct"].load_state_dict(torch.load(qwen3_predictor_path, map_location=torch.device('cpu')))

        self.token_num_predictors["Falcon3-7B-Instruct"].eval().to(device=device)
        self.token_num_predictors["llama-2-13B"].eval().to(device=device)
        self.token_num_predictors["Qwen3-30B-A3B-Instruct"].eval().to(device=device)

        logger.info("预测器加载成功")

    def setup_inference_time_predictor(self, predictor_dir_path: str = "/mnt/tidal-alsh01/usr/suqian/models/predict_time_models"):
        """
        加载推理时间预测器
        """
        model_scope = ["Falcon3-7B-Instruct", "llama-2-13B", "Qwen3-30B-A3B-Instruct"]
        for model in model_scope:
            self.inference_time_predictors[model] = {}

            # 为每种模型加载各个算子的独立的预测器
            self.inference_time_predictors[model]["embedding"] = joblib.load(os.path.join(predictor_dir_path), model, "embedding.joblib")
            self.inference_time_predictors[model]["rotary_embedding"] = joblib.load(os.path.join(predictor_dir_path), model, "rotary_embedding.joblib")
            self.inference_time_predictors[model]["attention"] = joblib.load(os.path.join(predictor_dir_path), model, "attention.joblib")
            self.inference_time_predictors[model]["mlp"] = joblib.load(os.path.join(predictor_dir_path), model, "mlp.joblib")
            self.inference_time_predictors[model]["input_layernorm"] = joblib.load(os.path.join(predictor_dir_path), model, "input_layernorm.joblib")
            self.inference_time_predictors[model]["post_attn_layernorm"] = joblib.load(os.path.join(predictor_dir_path), model, "post_attention_layernorm.joblib")
            self.inference_time_predictors[model]["rmsnorm"] = joblib.load(os.path.join(predictor_dir_path), model, "rmsnorm.joblib")
            self.inference_time_predictors[model]["lm_head"] = joblib.load(os.path.join(predictor_dir_path), model, "lm_head.joblib")
            self.inference_time_predictors[model]["cpu"] = joblib.load(os.path.join(predictor_dir_path), model, "cpu.joblib")


    def setup_inference_tokenizer(self):
        """
        加载三种推理模型的编码器
        :return:
        """

        logger.info(f"正在加载三种推理模型的编码器")

        self.inference_tokenizers["Falcon3-7B-Instruct"] = AutoTokenizer.from_pretrained(pretrained_model_name_or_path="/mnt/tidal-alsh01/usr/suqian/models/Falcon3-7B-Instruct", use_fast=True)

        self.inference_tokenizers["llama-2-13B"] = AutoTokenizer.from_pretrained(pretrained_model_name_or_path="/mnt/tidal-alsh01/usr/suqian/models/llama-2-13B", use_fast=True)
        self.inference_tokenizers.get("llama-2-13B").chat_template = custom_chat_template

        self.inference_tokenizers["Qwen3-30B-A3B-Instruct"] = AutoTokenizer.from_pretrained(pretrained_model_name_or_path="/mnt/tidal-alsh01/usr/suqian/models/Qwen3-30B-A3B-Instruct", use_fast=True)

        logger.info(f"三种推理模型的编码器加载成功")

    def setup_pca_model(self, pca_model_dir_path: str = "/mnt/tidal-alsh01/usr/suqian/models/predict_models/pca_models", pca_dim=64):
        """
        加载 PCA 模型
        :param pca_model_dir_path:
        :param pca_dim:
        :return:
        """

        logger.info(f"正在从 {pca_model_dir_path} 加载 PCA 模型")

        self.pca_models["Falcon3-7B-Instruct"] = {}
        self.pca_models["Falcon3-7B-Instruct"]["embedding"] = joblib.load(
            os.path.join(pca_model_dir_path, f"pca_embedding_{pca_dim}d_Falcon3-7B-Instruct_ray.pkl"))
        self.pca_models["Falcon3-7B-Instruct"]["pooling"] = joblib.load(
            os.path.join(pca_model_dir_path, f"pca_pooling_{pca_dim}d_Falcon3-7B-Instruct_ray.pkl"))

        self.pca_models["llama-2-13B"] = {}
        self.pca_models["llama-2-13B"]["embedding"] = joblib.load(
            os.path.join(pca_model_dir_path, f"pca_embedding_{pca_dim}d_llama-2-13B_ray.pkl"))
        self.pca_models["llama-2-13B"]["pooling"] = joblib.load(
                os.path.join(pca_model_dir_path, f"pca_pooling_{pca_dim}d_llama-2-13B_ray.pkl"))

        self.pca_models["Qwen3-30B-A3B-Instruct"] = {}
        self.pca_models["Qwen3-30B-A3B-Instruct"]["embedding"] = joblib.load(
            os.path.join(pca_model_dir_path, f"pca_embedding_{pca_dim}d_Qwen3-30B-A3B-Instruct_ray.pkl"))
        self.pca_models["Qwen3-30B-A3B-Instruct"]["pooling"] = joblib.load(
            os.path.join(pca_model_dir_path, f"pca_pooling_{pca_dim}d_Qwen3-30B-A3B-Instruct_ray.pkl"))

        logger.info(f"PCA 模型加载成功")

    def predicting_inference(self, request_list: list[dict]):
        """
        预测请求列表的推理时长
        :param request:
        :return:
        """

        """

        requestData结构：
        private class RequestData {
            String requestId;
            String userId;
            String userMessage;
            long predictedGeneratedTokenNum;
            double predictedInferenceTime;
        }
        """
        # 0. 对所有请求文本进行预处理，应用模板
        formatted_prompts = []
        for request in request_list:
            prompt = request.get("userMessage", None)
            llmModelName = request.get("llmModelName", None)
            try:
                messages = {"role: user", f"content: {prompt}"}
                tokenizer = self.inference_tokenizers.get(llmModelName)
                if tokenizer is None:
                    logger.error(f"未找到模型 {llmModelName} 对应的编码器")

                formatted_prompt = tokenizer.apply_chat_template(messages, tokenize=False)
                formatted_prompts.append(formatted_prompt)
            except Exception as e:
                logger.info(f"无法进行编码，错误:{e}")

        # 1. 使用代理模型获取请求文本 [CLS] 隐藏层状态和请求文本的平均池化向量
        inputs = self.proxy_tokenizer(formatted_prompts, return_tensors="pt", padding=True, truncation=True, return_length=True)
        # list[int]，每个 prompt 的 token 数
        prompt_token_nums = inputs["length"].tolist()

        with torch.no_grad():
            outputs = self.proxy_model(**inputs);
            hidden_states = outputs.last_hidden_state

            # [CLS] 对应的隐藏层状态
            cls_embeddings = hidden_states[:, 0, :]

            # 处 [CLS] 外 token 的隐藏层状态的平均池化向量，包含padding结果
            excluded_cls_embeddings = hidden_states[:, 1:, :]
            mean_pooling_excluded_cls_embeddings = excluded_cls_embeddings.mean(dim=1)
        
        for i, request in enumerate(request_list):
            # 2. 组成完整的输入特征
            llmModelName = request.get("llmModelName", None)
            
            cls_embd = cls_embeddings[i].cpu().numpy()
            mean_pooling_embd = mean_pooling_excluded_cls_embeddings[i].cpu().numpy()

            # 使用 PCA 对两个向量特征进行降维
            pca_models = self.pca_models.get(llmModelName)
            cls_embd = pca_models.get("embedding").transform(cls_embd).flatten()
            mean_pooling_embd = pca_models.get("pooling").transform(mean_pooling_embd).flatten()

            temperature = self.temperature.get(request.get("llmModelName"))
            top_p = self.top_p.get(request.get("llmModelName"))
            top_k = self.top_k
            repetition_penalty = self.repetition_penalty

            feature_vec = pd.DataFrame([list(cls_embd) + list(mean_pooling_embd) + [prompt_token_nums[i], temperature, top_p, top_k, repetition_penalty]])
            
            # 3. 将特征输入模型，获取每个请求对应的预测的生成 token 数
            token_num_predictor = self.token_num_predictors.get(llmModelName)
            try:
                with torch.no_grad():
                    predicted_generated_token_num = math.ceil(token_num_predictor.predict(feature_vec).cpu().numpy().flatten()[0])
            except Exception as e:
                logger.error(f"预测生成 token 数失败")

            # 4. 获取每个请求对应的预测的推理时间
            predicted_inference_time = self.predicting_inference_time(llmModelName=llmModelName, predicted_token_num=predicted_generated_token_num)

            # 将预测的token数和预测的推理时间加到请求字典中
            request.update(predictedGeneratedTokenNum=predicted_generated_token_num, predictedInferenceTime=predicted_inference_time)

        # 5. 返回添加了预测生成 token 数以及预测推理时间的请求列表(元素类型为 dict)
        return request_list

    def predicting_inference_time(self, llmModelName: str, predicted_token_num: int, prompt_token_num: int, layer_iter_num: int, batchsize: int):
        inference_time_predictor = self.inference_time_predictors[llmModelName]

        embedding_predictor = inference_time_predictor["embedding"]
        rotary_embedding_predictor = inference_time_predictor["rotary_embedding"]
        attention_predictor = inference_time_predictor["attention"]
        mlp_predictor = inference_time_predictor["mlp"]
        input_layernorm_predictor = inference_time_predictor["input_layernorm"]
        post_attn_layernorm_predictor = inference_time_predictor["post_attn_layernorm"]
        rmsnorm_predictor = inference_time_predictor["rmsnorm"]
        lm_head_predictor = inference_time_predictor["lm_head"]
        cpu_predictor = inference_time_predictor["cpu"]

        # ========= 构造 NumPy 特征 =========

        # embedding / rotary / lm_head
        embedding_features = []
        rotary_embedding_features = []
        lm_head_features = []

        # rmsnorm / attention / mlp
        input_layernorm_features = []
        attention_features = []
        post_attn_layernorm_features = []
        mlp_features = []
        rmsnorm_features = []

        for i in range(predicted_token_num):
            post_token_num = prompt_token_num + i - 1;
            if i == 0:
                post_token_num += 1
            embedding_features.append([batchsize, i, prompt_token_num, post_token_num])
            rotary_embedding_features.append([batchsize, i, prompt_token_num, post_token_num])
            lm_head_features.append([batchsize, i, prompt_token_num, post_token_num])
            input_layernorm_idx = 0
            post_attn_layernorm_idx = 1

            for j in range(layer_iter_num):
                input_layernorm_features.append([batchsize, i, input_layernorm_idx, prompt_token_num, post_token_num])
                input_layernorm_idx += 2
                post_attn_layernorm_features.append([batchsize, i, post_attn_layernorm_idx, prompt_token_num, post_token_num])
                post_attn_layernorm_idx += 2

            rmsnorm_features.append([batchsize, i, 56, prompt_token_num, post_token_num])

        cpu_features = [[batchsize, prompt_token_num, predicted_token_num]]

        embedding_df = pd.DataFrame(np.array(embedding_features, dtype=np.int32), columns=['batch_size', 'model_idx', 'prompt_token_num', 'post_token_num'])
        rotary_embedding_df = pd.DataFrame(np.array(rotary_embedding_features, dtype=np.int32), columns=['batch_size', 'model_idx', 'prompt_token_num', 'post_token_num'])
        lm_head_df = pd.DataFrame(np.array(lm_head_features, dtype=np.int32), columns=['batch_size', 'model_idx', 'prompt_token_num', 'post_token_num'])
        rmsnorm_df = pd.DataFrame(np.array(rmsnorm_features, dtype=np.int32), columns=['batch_size', 'model_idx', 'rmsnorm_idx', 'prompt_token_num', 'post_token_num'])
        attention_df = pd.DataFrame(np.array(attention_features, dtype=np.int32), columns=['batch_size', 'model_idx', 'layer_idx', 'prompt_token_num', 'post_token_num'])
        mlp_df = pd.DataFrame(np.array(mlp_features, dtype=np.int32), columns=['batch_size', 'model_idx', 'layer_idx', 'prompt_token_num', 'post_token_num'])
        post_df = pd.DataFrame(np.array(post_attn_layernorm_features, dtype=np.int32), columns=['batch_size', 'model_idx', 'rmsnorm_idx', 'prompt_token_num', 'post_token_num'])
        input_df = pd.DataFrame(np.array(input_layernorm_features, dtype=np.int32), columns=['batch_size', 'model_idx', 'rmsnorm_idx', 'prompt_token_num', 'post_token_num'])
        cpu_df = pd.DataFrame(np.array(cpu_features, dtype=np.int32), columns=['batch_size', 'prompt_token_num', 'generated_token_num'])

        total_gpu_time_us = (sum(embedding_predictor.predict(embedding_df)) + sum(rotary_embedding_predictor.predict(rotary_embedding_df)) + sum(lm_head_predictor.predict(lm_head_df)) + sum(rmsnorm_predictor.predict(rmsnorm_df)) + sum(attention_predictor.predict(attention_df)) + sum(mlp_predictor.predict(mlp_df)) + sum(post_attn_layernorm_features.predict(post_df)) + sum(input_layernorm_features.predict(input_df)))

        total_cpu_time_us = sum(cpu_predictor.predict(cpu_df))
        
        total_predicted_elapsed_time_us = total_gpu_time_us + total_cpu_time_us
        total_predicted_elapsed_time_ms = total_predicted_elapsed_time_us / 1e3

        return total_predicted_elapsed_time_ms


def main():
    if len(sys.argv) < 2:
        logger.error("Usage: python predict_inference_time.py <node_ip>")
        sys.exit(1)

    # 节点ip
    node_ip = sys.argv[1]

    # 初始化预测推理时长的服务
    service = PredictInferenceTimeService()

    # 输出服务就绪信号
    print("READY", flush=True)

    logger.info(f"推理时长预测服务初始化完成")

    # 接收主进程的输入
    for line_num, line in enumerate(sys.stdin, 1):
        line = line.strip()
        if not line:
            continue

        try:
            # 解析请求信息，预测推理时长
            request_list = json.loads(line)
            if not isinstance(request_list, list):
                logger.error(f"Expected list but got: {type(request_list)}")
                raise ValueError(f"Expected list but got: {type(request_list)}")

            predicted_request_list = service.predicting_inference(request_list)

            print(predicted_request_list, flush=True)


        except Exception as e:
            logger.error(f"Error processing line {line_num}: {e}")


if __name__ == "__main__":
    main()
