"""
预测一次推理生成的 token 数量
"""
import os
import sys
import json

import joblib
import torch
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

def setup_predictor(predictor_path):
    """
    加载预测器
    :param predictor_path:
    :return:
    """

    return predcitor

def setup_proxy_model(proxy_model_name, device):
    """
    加载代理模型
    :param proxy_model:
    :param device:
    :return:
    """

    logger.info(f"Loading proxy model from {proxy_model_path} to device {device}")
    proxy_tokenizer = AutoTokenizer.from_pretrained(pretrained_model_name_or_path=proxy_model_path, use_fast=True)
    proxy_model = AutoModel.from_pretrained(pretrained_model_name_or_path=proxy_model_path, torch_dtype=torch.bfloat16)
    proxy_model.to(device).eval()

    logger.info(f"Proxy model loaded to device {device} successfully")

    return proxy_tokenizer, proxy_model

def setup_pca_model(pca_model_path, inference_model_name, pca_dim=32):
    """
    加载 PCA 模型
    :param pca_model_path:
    :return:
    """

    try:
        logger.info(f"Loading PCA model from {pca_model_path}")
        pca_embedding_model = joblib.load(os.path.join(pca_model_path, f"pca_{inference_model_name_{pca_dim}d_embedding.pkl"))
        pca_pooling_model = joblib.load(os.path.join(pca_model_path, f"pca_{inference_model_name}_{pca_dim}d_pooling.pkl"))
    except Exception as e:
        logger.error(f"Failed to load PCA model from {pca_model_path}: {e}")
        sys.exit(1)
    logger.info(f"PCA model loaded successfully")

    return pca_embedding_model, pca_pooling_model


def predict_generated_token_num():
    """
    预测推理生成的 token 数量
    :return:
    """


def main():
    if len(sys.argv) < 5:
        logger.error("Usage: python predict_generated_token_num.py <node_ip> <proxy_model_name> <inference_model_name> <predictor_path> <pca_model_path>")
        sys.exit(1)

    node_ip = sys.argv[1]
    proxy_model_name = sys.argv[2]
    predictor_path = sys.argv[3]
    pca_model_path = sys.argv[4]

    logger.info(f"Inference preprocessing at {node_ip}, using proxy model {proxy_model_name}")

    device = "cuda:0" if torch.cuda.is_available() else "cpu"
    logger.info(f"Using device: {device}")

    proxy_tokenizer, proxy_model = setup_proxy_model(proxy_model_name=proxy_model_name, device=device)

if __name__ == "__main__":
    main()
