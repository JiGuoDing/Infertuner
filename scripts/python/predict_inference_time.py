"""
预测推理时长
"""
import os
import sys
import json
import torch
import joblib
import logging

from torch import nn
from transformers import AutoModel, AutoTokenizer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

os.environ["CUDA_VISIBLE_DEVICES"] = "0"


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

        # 加载预测器
        self.predictors = {}
        self.setup_predictor(device=self.device)

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

    def setup_predictor(self, predictor_dir_path: str = "/mnt/tidal-alsh01/usr/suqian/models/predict_models", device="cuda:0", pca_dim=64):
        """
        加载预测器
        :param pca_dim:
        :param device:
        :param predictor_dir_path:
        :return:
        """

        logger.info(f"正在从 {predictor_dir_path} 加载预测器到设备 {device}")

        self.predictors["Falcon3-7B-Instruct"] = TokenPredictor(2 * pca_dim + 5)
        self.predictors["llama-2-13B"] = TokenPredictor(2 * pca_dim + 5)
        self.predictors["Qwen3-30B-A3B-Instruct"] = TokenPredictor(2 * pca_dim + 5)

        falcon3_predictor_path = os.path.join(predictor_dir_path, f"best_mlp_model_Falcon3-7B-Instruct_pca{pca_dim}_ray.pth")
        llama2_predictor_path = os.path.join(predictor_dir_path, f"best_mlp_model_llama-2-13B_pca{pca_dim}_ray.pth")
        qwen3_predictor_path = os.path.join(predictor_dir_path, f"best_mlp_model_Qwen3-30B-A3B-Instruct_pca{pca_dim}_ray.pth")

        self.predictors["Falcon3-7B-Instruct"].load_state_dict(torch.load(falcon3_predictor_path, map_location=torch.device('cpu')))
        self.predictors["llama-2-13B"].load_state_dict(torch.load(llama2_predictor_path, map_location=torch.device('cpu')))
        self.predictors["Qwen3-30B-A3B-Instruct"].load_state_dict(torch.load(qwen3_predictor_path, map_location=torch.device('cpu')))

        self.predictors["Falcon3-7B-Instruct"].eval().to(device=device)
        self.predictors["llama-2-13B"].eval().to(device=device)
        self.predictors["Qwen3-30B-A3B-Instruct"].eval().to(device=device)

        logger.info("预测器加载成功")

    def setup_inference_tokenizer(self):
        """
        加载三种推理模型的编码器
        :return:
        """

        logger.info(f"正在加载三种推理模型的编码器")

        self.inference_tokenizers["Falcon3-7B-Instruct"] = AutoTokenizer.from_pretrained(pretrained_model_name_or_path="/mnt/tidal-alsh01/usr/suqian/models/Falcon3-7B-Instruct", use_fast=True)
        self.inference_tokenizers["llama-2-13B"] = AutoTokenizer.from_pretrained(pretrained_model_name_or_path="/mnt/tidal-alsh01/usr/suqian/models/llama-2-13B", use_fast=True)
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

    def predict_inference_time(self, request_list):
        """
        预测请求列表的推理时长
        :param request:
        :return:
        """

        """
        request结构：
        private static class SingleRequestData {
            public String userId;
            public String user_message;
            public int max_tokens;
            public String request_id;
        }
        """
        # 0. 对请求文本进行预处理，应用模板
        

        # 1. 使用代理模型获取请求文本 [CLS] 隐藏层状态和请求文本的平均池化向量



def main():
    if len(sys.argv) < 2:
        logger.error("Usage: python predict_inference_time.py <node_ip>")
        sys.exit(1)

    # 节点ip
    node_ip = sys.argv[1]

    # 初始化预测推理时长的服务
    service = PredictInferenceTimeService()

    # 输出服务就绪信号
    print("READY")

    logger.info(f"推理时长预测服务初始化完成")

    # 接收主进程的输入
    for line_num, line in enumerate(sys.stdin, 1):
        line = line.strip()
        if not line:
            continue

        try:
            # 解析请求信息，预测推理时长
            request_data = json.loads(line)
            request_list = request_data.get("requests", [])

            predicted_inference_time_list = service.predict_inference_time(request_list)

        except Exception as e:
            logger.error(f"Error processing line {line_num}: {e}")


if __name__ == "__main__":
    main()
