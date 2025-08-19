"""
有服务启动开销的批量推理服务
每次批处理调用都有固定的启动开销，攒批可以分摊这个开销
"""

import os
import sys
import json
import time
import torch
import logging
from pathlib import Path
from transformers import AutoTokenizer, AutoModelForCausalLM

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

class BatchInferenceServiceWithStartupCost:
    """有服务启动开销的批量推理服务"""

    def __init__(self, model_path: str, gpu_id: int = 0):
        self.model_path = model_path
        self.gpu_id = gpu_id

        # GPU设备配置
        if torch.cuda.is_available():
            self.device = torch.device(f'cuda:{gpu_id}')
            logger.info(f"使用GPU: {torch.cuda.get_device_name(gpu_id)}")
        else:
            self.device = torch.device('cpu')
            logger.warning("CUDA不可用，回退到CPU模式")

        # 加载真实模型
        self.tokenizer, self.model = self._load_model()

        # 统计信息
        self.stats = {
            "total_batches": 0,
            "total_requests": 0,
            "total_startup_cost": 0.0,
            "total_inference_time": 0.0,
            "start_time": time.time()
        }

        logger.info("BatchInferenceServiceWithStartupCost初始化完成")

    def _load_model(self):
        """加载真实模型"""
        try:
            logger.info(f"加载模型: {self.model_path}")

            tokenizer = AutoTokenizer.from_pretrained(
                self.model_path,
                trust_remote_code=True
            )

            model = AutoModelForCausalLM.from_pretrained(
                self.model_path,
                torch_dtype=torch.bfloat16,
                device_map="auto",
                trust_remote_code=True,
                low_cpu_mem_usage=True
            )

            model.eval()

            if torch.cuda.is_available():
                gpu_memory = torch.cuda.get_device_properties(self.device).total_memory / 1e9
                allocated = torch.cuda.memory_allocated(self.device) / 1e9
                logger.info(f"GPU显存: {allocated:.1f}GB / {gpu_memory:.1f}GB")

            logger.info("✅ 模型加载成功")
            return tokenizer, model

        except Exception as e:
            logger.error(f"❌ 模型加载失败: {e}")
            raise

    def batch_generate_response(self, batch_request_data):
        """处理批量请求 - 关键：有固定的服务启动开销"""
        batch_start_time = time.time()
        self.stats["total_batches"] += 1

        try:
            requests = batch_request_data.get('requests', [])
            batch_size = len(requests)
            batch_id = batch_request_data.get('batch_id', 'unknown')

            if not requests:
                raise ValueError("批量请求不能为空")

            self.stats["total_requests"] += batch_size

            logger.info(f"🚀 收到批量请求: batch_id={batch_id}, size={batch_size}")

            # 🔧 关键：模拟每次批处理调用的固定启动开销
            startup_cost_ms = 300  # 每次调用固定300ms的启动开销
            logger.info(f"⏳ 服务启动开销: {startup_cost_ms}ms (每次批处理调用都有这个开销)")
            time.sleep(startup_cost_ms / 1000.0)
            self.stats["total_startup_cost"] += startup_cost_ms

            # 然后串行处理每个请求
            logger.info(f"📝 启动开销完成，开始串行处理 {batch_size} 个请求...")

            batch_responses = []
            total_inference_time = 0

            for i, req in enumerate(requests):
                logger.info(f"  处理第 {i+1}/{batch_size} 个请求: {req.get('request_id', 'unknown')}")

                # 单个请求的推理
                single_start = time.time()
                response_text = self._generate_real_text(req)
                single_end = time.time()

                single_inference_time = (single_end - single_start) * 1000
                total_inference_time += single_inference_time

                single_response = {
                    "response": response_text,
                    "inference_time_ms": round(single_inference_time, 2),
                    "request_id": req.get('request_id', f'req_{i}'),
                    "success": True,
                    "timestamp": int(time.time() * 1000)
                }
                batch_responses.append(single_response)

                logger.info(f"  ✅ 第 {i+1} 个请求完成: {single_inference_time:.1f}ms")

            batch_end_time = time.time()
            total_batch_time = (batch_end_time - batch_start_time) * 1000

            result = {
                "responses": batch_responses,
                "batch_size": batch_size,
                "batch_id": batch_id,
                "total_inference_time_ms": round(total_batch_time, 2),
                "success": True,
                "timestamp": int(time.time() * 1000),
                "error": "No error"
            }

            logger.info(f"✅ 批量处理完成: batch_id={batch_id}")
            logger.info(f"📊 总耗时={total_batch_time:.1f}ms (启动:{startup_cost_ms}ms + 推理:{total_inference_time:.1f}ms)")
            logger.info(f"💡 攒批效果: {batch_size}个请求共享{startup_cost_ms}ms启动开销 = 平均{startup_cost_ms/batch_size:.1f}ms/req")

            return result

        except Exception as e:
            logger.error(f"批量推理失败: {e}")
            return {
                "error": str(e),
                "responses": [],
                "batch_size": 0,
                "batch_id": batch_request_data.get('batch_id', 'unknown'),
                "total_inference_time_ms": 0.0,
                "success": False,
                "timestamp": int(time.time() * 1000)
            }

    def _generate_real_text(self, request):
        """真实模型推理"""
        try:
            user_message = request.get('user_message', '')
            max_tokens = request.get('max_tokens', 100)

            if not user_message:
                raise ValueError("user_message不能为空")

            messages = [{"role": "user", "content": user_message}]

            text = self.tokenizer.apply_chat_template(
                messages,
                tokenize=False,
                add_generation_prompt=True
            )

            inputs = self.tokenizer([text], return_tensors="pt").to(self.device)

            generation_config = {
                "max_new_tokens": max_tokens,
                "do_sample": True,
                "temperature": 0.7,
                "top_p": 0.9,
                "pad_token_id": self.tokenizer.eos_token_id,
            }

            with torch.no_grad():
                outputs = self.model.generate(**inputs, **generation_config)

            generated_ids = outputs[0][inputs['input_ids'].shape[1]:]
            response = self.tokenizer.decode(generated_ids, skip_special_tokens=True)

            return response.strip()

        except Exception as e:
            logger.error(f"推理失败: {e}")
            raise

    def get_stats(self):
        """获取统计信息"""
        uptime = time.time() - self.stats["start_time"]
        avg_startup_cost = (
            self.stats["total_startup_cost"] / self.stats["total_batches"]
            if self.stats["total_batches"] > 0 else 0.0
        )

        return {
            "total_batches": self.stats["total_batches"],
            "total_requests": self.stats["total_requests"],
            "avg_startup_cost_ms": round(avg_startup_cost, 2),
            "total_startup_cost_ms": round(self.stats["total_startup_cost"], 2),
            "uptime_seconds": round(uptime, 1),
            "gpu_id": self.gpu_id
        }

def main():
    """主函数"""
    if len(sys.argv) < 3:
        logger.error("用法: python3 batch_inference_service.py <node_ip> <model_path> [gpu_id]")
        sys.exit(1)

    node_ip = sys.argv[1]
    model_path = sys.argv[2]
    gpu_id = int(sys.argv[3]) if len(sys.argv) > 3 else 0

    os.environ["CUDA_VISIBLE_DEVICES"] = str(gpu_id)

    # 检查环境
    if not Path(model_path).exists():
        logger.error(f"模型路径不存在: {model_path}")
        sys.exit(1)

    if not torch.cuda.is_available():
        logger.error("CUDA不可用！")
        sys.exit(1)

    if gpu_id >= torch.cuda.device_count():
        logger.error(f"GPU {gpu_id} 不存在！")
        sys.exit(1)

    try:
        service = BatchInferenceServiceWithStartupCost(model_path, gpu_id)
        logger.info("🚀 有启动开销的推理服务启动成功！")
        logger.info("💡 攒批优势: 多个请求共享固定的300ms服务启动开销")

        """
            private static class BatchRequestData {
                public List<SingleRequestData> requests;
                public int batch_size;
                public String batch_id;
            }

            private static class SingleRequestData {
                public String user_message;
                public int max_tokens;
                public String request_id;
            }
        """

        for line_num, line in enumerate(sys.stdin, 1):
            line = line.strip()
            if not line:
                continue

            try:
                batch_request_data = json.loads(line)

#                 command = batch_request_data.get("command")
#
#                 if command == "stats":
#                     stats = service.get_stats()
#                     print(json.dumps({"stats": stats}))
#                     sys.stdout.flush()
#                     continue
#
#                 if command == "shutdown":
#                     logger.info("收到关闭命令")
#                     break

                # 处理攒批请求
                result = service.batch_generate_response(batch_request_data)

                print(json.dumps(result))
                sys.stdout.flush()

            except json.JSONDecodeError as e:
                error_result = {
                    "error": f"JSON解析错误: {e}",
                    "responses": [],
                    "success": False,
                    "batch_id": "json_error",
                    "timestamp": int(time.time() * 1000)
                }
                print(json.dumps(error_result))
                sys.stdout.flush()

            except Exception as e:
                logger.error(f"处理请求出错: {e}")
                error_result = {
                    "error": f"处理错误: {e}",
                    "responses": [],
                    "success": False,
                    "batch_id": "processing_error",
                    "timestamp": int(time.time() * 1000)
                }
                print(json.dumps(error_result))
                sys.stdout.flush()

    except Exception as e:
        logger.error(f"服务启动失败: {e}")
        sys.exit(1)

    logger.info("🛑 推理服务停止")

if __name__ == "__main__":
    main()