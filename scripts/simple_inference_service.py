#!/opt/conda/envs/vllm-env/bin/python3

import torch
import json
import sys
import time
import logging
from pathlib import Path
from transformers import AutoTokenizer, AutoModelForCausalLM

# 配置日志输出到stderr，避免与stdout数据流冲突
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

class SimpleInferenceService:
    """精简版推理服务 - GPU加速版本"""
    
    def __init__(self, node_ip: str, model_path: str, gpu_id: int = 0):
        """
            node_ip指明推理进程所在节点的IP地址
        """

        self.node_ip = node_ip
        self.model_path = model_path
        self.gpu_id = gpu_id
        
        # GPU设备配置
        if torch.cuda.is_available():
            self.device = torch.device(f'cuda:{gpu_id}')
            logger.info(f"使用GPU: {torch.cuda.get_device_name(gpu_id)}")
        else:
            self.device = torch.device('cpu')
            logger.warning("CUDA不可用，回退到CPU模式")
        
        # 加载模型
        self.tokenizer, self.model = self._load_model()
        
        # 简化统计
        self.stats = {
            "total_requests": 0,
            "total_inference_time": 0.0,
            "errors": 0,
            "start_time": time.time()
        }
    
    def _load_model(self):
        """加载模型到GPU"""
        try:
            logger.info(f"节点 {self.node_ip} 从 {self.model_path} 加载模型到 {self.device}")
            
            # 加载tokenizer
            tokenizer = AutoTokenizer.from_pretrained(
                self.model_path,
                trust_remote_code=True
            )
            
            # 加载模型到GPU
            model = AutoModelForCausalLM.from_pretrained(
                self.model_path,
                torch_dtype=torch.bfloat16,  # GPU使用float16节省显存
                device_map="auto"
                trust_remote_code=True,
                low_cpu_mem_usage=True
            )
            
            model.eval()
            
            # 显示GPU显存使用情况
            if torch.cuda.is_available():
                gpu_memory = torch.cuda.get_device_properties(self.device).total_memory / 1e9
                allocated = torch.cuda.memory_allocated(self.device) / 1e9
                logger.info(f"节点 {self.node_ip} 的GPU显存: {allocated:.1f}GB / {gpu_memory:.1f}GB")
            
            logger.info("✅ 模型加载成功")
            return tokenizer, model
            
        except Exception as e:
            logger.error(f"❌ 模型加载失败: {e}")
            raise
    
    def generate_response(self, request_data):
        """生成回复"""
        start_time = time.time()
        self.stats["total_requests"] += 1
        
        try:
            # 解析请求参数
            user_message = request_data.get('user_message', '')
            max_tokens = request_data.get('max_tokens', 640)
            request_id = request_data.get('request_id', 'unknown')
            batch_size = request_data.get('batch_size', 1)  # 支持批处理参数
            
            if not user_message:
                raise ValueError("user_message不能为空")
            
            # 构建对话消息
            messages = [{"role": "user", "content": user_message}]
            
            # 生成回复
            response_text = self._generate_text(messages, max_tokens)
            
            # 计算推理时间
            inference_time = (time.time() - start_time) * 1000
            self.stats["total_inference_time"] += inference_time
            
            # 构建响应
            result = {
                "response": response_text,
                "inference_time_ms": round(inference_time, 2),
                "model_name": "Qwen1.5-1.8B-Chat",
                "request_id": request_id,
                "batch_size": batch_size,
                "success": True,
                "timestamp": int(time.time() * 1000)
            }
            
            return result
            
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"推理失败 - request_id: {request_data.get('request_id', 'unknown')}, error: {e}")
            
            return {
                "error": str(e),
                "response": "抱歉，处理请求时出现错误",
                "inference_time_ms": 0.0,
                "model_name": "Error",
                "request_id": request_data.get('request_id', 'unknown'),
                "batch_size": request_data.get('batch_size', 1),
                "success": False,
                "timestamp": int(time.time() * 1000)
            }
    
    def _generate_text(self, messages, max_tokens):
        """生成文本回复"""
        try:
            # 应用chat模板
            text = self.tokenizer.apply_chat_template(
                messages,
                tokenize=False,
                add_generation_prompt=True
            )
            
            # 编码输入到GPU
            inputs = self.tokenizer([text], return_tensors="pt").to(self.device)
            
            # 生成配置
            generation_config = {
                "max_new_tokens": max_tokens,
                "do_sample": True,
                "temperature": 0.7,
                "top_p": 0.9,
                "pad_token_id": self.tokenizer.eos_token_id,
            }
            
            # 执行生成
            with torch.no_grad():
                outputs = self.model.generate(**inputs, **generation_config)
            
            # 解码回复（只取新生成的部分）
            generated_ids = outputs[0][inputs['input_ids'].shape[1]:]
            response = self.tokenizer.decode(generated_ids, skip_special_tokens=True)
            
            return response.strip()
            
        except Exception as e:
            logger.error(f"文本生成失败: {e}")
            raise
    
    def get_stats(self):
        """获取服务统计信息"""
        current_time = time.time()
        uptime = current_time - self.stats["start_time"]
        
        avg_inference_time = (
            self.stats["total_inference_time"] / self.stats["total_requests"]
            if self.stats["total_requests"] > 0 else 0.0
        )
        
        throughput = (
            self.stats["total_requests"] / uptime
            if uptime > 0 else 0.0
        )
        
        return {
            "total_requests": self.stats["total_requests"],
            "avg_inference_time_ms": round(avg_inference_time, 2),
            "total_inference_time_ms": round(self.stats["total_inference_time"], 2),
            "errors": self.stats["errors"],
            "error_rate": round(self.stats["errors"] / max(self.stats["total_requests"], 1), 3),
            "uptime_seconds": round(uptime, 1),
            "throughput_qps": round(throughput, 3),
            "device": str(self.device),
            "gpu_memory_allocated_gb": torch.cuda.memory_allocated(self.device) / 1e9 if torch.cuda.is_available() else 0,
            "gpu_memory_cached_gb": torch.cuda.memory_reserved(self.device) / 1e9 if torch.cuda.is_available() else 0
        }

def main():
    """主函数"""
    if len(sys.argv) < 3:
        logger.error("用法: python3 simple_inference_service.py <node_ip> <model_path> <gpu_id>")
        sys.exit(1)

    node_ip = sys.argv[1]
    model_path = sys.argv[2]
    gpu_id = int(sys.argv[3]) if len(sys.argv) > 3 else 0
    
    # 检查模型路径
    if not Path(model_path).exists():
        logger.error(f"模型路径不存在: {model_path}")
        sys.exit(1)
    
    # 检查GPU可用性
    if not torch.cuda.is_available():
        logger.error("CUDA不可用！请检查GPU驱动和CUDA安装")
        sys.exit(1)
    
    if gpu_id >= torch.cuda.device_count():
        logger.error(f"GPU {gpu_id} 不存在！可用GPU数量: {torch.cuda.device_count()}")
        sys.exit(1)
    
    # 初始化服务
    try:
        service = SimpleInferenceService(node_ip, model_path, gpu_id)
        logger.info("输入格式: JSON包含'user_message', 'max_tokens', 'request_id'等字段")
        logger.info("特殊命令: {'command': 'stats'} 获取统计, {'command': 'shutdown'} 关闭服务")
    except Exception as e:
        logger.error(f"❌ 服务启动失败: {e}")
        sys.exit(1)
    
    # 处理请求循环
    try:
        # 在没有外部干预的情况下，该循环是一个无限循环，因为sys.stdin是一个流式输入，Python 会持续读取直到流关闭或发生特定事件。
        for line_num, line in enumerate(sys.stdin, 1):
            line = line.strip()
            if not line:
                continue
            
            try:
                # 解析JSON请求
                request_data = json.loads(line)
                
                # 处理特殊命令
                if request_data.get("command") == "stats":
                    stats = service.get_stats()
                    print(json.dumps({"stats": stats}))
                    sys.stdout.flush()
                    continue
                
                if request_data.get("command") == "shutdown":
                    logger.info("收到关闭命令，正在退出...")
                    break
                
                # 处理推理请求
                result = service.generate_response(request_data)
                
                # 输出结果到stdout
                print(json.dumps(result))
                sys.stdout.flush()
                
                # 定期输出统计到stderr
                if line_num % 10 == 0:
                    stats = service.get_stats()
                    logger.info(f"处理了 {line_num} 个请求，统计: {stats}")
                
            except json.JSONDecodeError as e:
                error_result = {
                    "error": f"JSON解析错误: {e}",
                    "response": "请求格式错误",
                    "success": False,
                    "request_id": "json_error",
                    "timestamp": int(time.time() * 1000)
                }
                print(json.dumps(error_result))
                sys.stdout.flush()
                
            except Exception as e:
                logger.error(f"处理请求时出错: {e}")
                error_result = {
                    "error": f"处理错误: {e}",
                    "response": "服务器内部错误",
                    "success": False,
                    "request_id": "processing_error",
                    "timestamp": int(time.time() * 1000)
                }
                print(json.dumps(error_result))
                sys.stdout.flush()
    
    except KeyboardInterrupt:
        logger.info("服务被用户中断")
    except Exception as e:
        logger.error(f"服务运行出错: {e}")
        sys.exit(1)
    finally:
        # 输出最终统计
        try:
            final_stats = service.get_stats()
            logger.info(f"最终统计结果: {final_stats}")
        except Exception as e:
            logger.error("获取最终统计时出错")
        finally:
            logger.info("🛑 推理服务已停止")

if __name__ == "__main__":
    main()
