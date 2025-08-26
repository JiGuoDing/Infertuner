"""
预测一次推理生成的 token 数量
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

dropout = 0.3
# 预测器网络结构
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

def setup_predictor(predictor_path, device, pca_dim=16):
    """
    加载预测器
    :param pca_dim:
    :param device:
    :param predictor_path:
    :return:
    """

    logger.info(f"Loading predictor from {predictor_path} to device {device}")
    predictor = TokenPredictor(2*pca_dim + 5)
    predictor.load_state_dict(torch.load(predictor_path, map_location=torch.device('cpu')))
    predictor.eval().to(device=device)
    logger.info("Predictor loaded successfully")

    return predictor

def setup_proxy_model(proxy_model_name, device):
    """
    加载代理模型
    :param proxy_model_name:
    :param device:
    :return:
    """

    logger.info(f"Loading proxy model {proxy_model_name} to device {device}")
    proxy_model_path = os.path.join("/mnt/tidal-alsh01/usr/suqian/models", proxy_model_name)
    proxy_tokenizer = AutoTokenizer.from_pretrained(pretrained_model_name_or_path=proxy_model_path, use_fast=True)
    proxy_model = AutoModel.from_pretrained(pretrained_model_name_or_path=proxy_model_path, torch_dtype=torch.bfloat16)
    proxy_model.to(device).eval()

    logger.info(f"Proxy model loaded to device {device} successfully")

    return proxy_tokenizer, proxy_model

def setup_pca_model(pca_model_path, inference_model_name, pca_dim=16):
    """
    加载 PCA 模型
    :param pca_model_path:
    :param inference_model_name:
    :param pca_dim:
    :return:
    """

    try:
        logger.info(f"Loading PCA model from {pca_model_path}")
        pca_embedding_model = joblib.load(os.path.join(pca_model_path, f"pca_{inference_model_name}_{pca_dim}d_embedding.pkl"))
        pca_pooling_model = joblib.load(os.path.join(pca_model_path, f"pca_{inference_model_name}_{pca_dim}d_pooling.pkl"))
    except Exception as e:
        logger.error(f"Failed to load PCA model from {pca_model_path}: {e}")
        sys.exit(1)
    logger.info(f"PCA model loaded successfully")

    return pca_embedding_model, pca_pooling_model


def predict_generated_token_num(proxy_tokenizer, proxy_model, pca_embedding_model, pca_pooling_model, predictor, request_list):
    """
    预测生成 token 数
    :param proxy_tokenizer:
    :param proxy_model:
    :param pca_embedding_model:
    :param pca_pooling_model:
    :param predictor:
    :param request_list:
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


def main():
    if len(sys.argv) < 6:
        logger.error("Usage: python predict_generated_token_num.py <node_ip> <proxy_model_name> <inference_model_name> <predictor_path> <pca_model_path>")
        sys.exit(1)

    # 节点ip
    node_ip = sys.argv[1]
    # 代理模型名称
    proxy_model_name = sys.argv[2]
    # 推理模型名称
    inference_model_name = sys.argv[3]
    # 预测器路径
    predictor_path = sys.argv[4]
    # pca模型路径
    pca_model_path = sys.argv[5]

    logger.info(f"Inference preprocessing at {node_ip}, using proxy model {proxy_model_name}")

    device = "cuda:0" if torch.cuda.is_available() else "cpu"
    logger.info(f"Using device: {device}")

    # 获取代理模型及其编码器
    proxy_tokenizer, proxy_model = setup_proxy_model(proxy_model_name=proxy_model_name, device=device)

    # 获取 PCA 模型
    pca_embedding_model, pca_pooling_model = setup_pca_model(pca_model_path=pca_model_path, inference_model_name=inference_model_name)

    # 获取预测器
    predictor = setup_predictor(predictor_path=predictor_path, device=device)

    logger.info(f"推理token数预测服务初始化完成")

    # 预测推理 token 数
    for line_num, line in enumerate(sys.stdin, 1):
        line = line.strip()
        if not line:
            continue

        try:
            # 获取 prompt 的嵌入向量
            request_data = json.loads(line)
            request_list = request_data.get("requests", [])

            # TODO BGE-m3是否可以直接应用chat_template
            predicted_token_num_list = predict_generated_token_num(proxy_tokenizer, proxy_model, pca_embedding_model, pca_pooling_model, predictor, request_list)

        except Exception as e:
            logger.error(f"Error processing line {line_num}: {e}")


if __name__ == "__main__":
    main()
