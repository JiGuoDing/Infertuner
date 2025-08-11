#!/usr/bin/env python3
"""
测试推理服务的功能
"""

import subprocess
import json
import time
import threading
import queue

def test_inference_service():
    model_path = "/workspace/models/Qwen1.5-1.8B-Chat"
    service_script = "/workspace/infertuner-simple/scripts/simple_inference_service.py"
    
    print("🧪 启动推理服务测试...")
    
    # 启动推理服务进程
    try:
        process = subprocess.Popen(
            ["python3", service_script, model_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )
        
        print("✅ 推理服务进程启动")
        
        # 等待服务初始化
        print("⏳ 等待服务初始化...")
        time.sleep(8)
        
        # 测试请求
        test_requests = [
            {
                "user_message": "你好，请简单介绍一下机器学习",
                "max_tokens": 50,
                "request_id": "test_001"
            },
            {
                "user_message": "什么是深度学习？",
                "max_tokens": 60,
                "request_id": "test_002"
            },
            {
                "user_message": "能解释一下transformer吗？",
                "max_tokens": 80,
                "request_id": "test_003"
            }
        ]
        
        print(f"📤 发送 {len(test_requests)} 个测试请求...\n")
        
        # 发送测试请求
        for i, req in enumerate(test_requests, 1):
            print(f"--- 测试请求 {i}/{len(test_requests)} ---")
            print(f"发送: {req['user_message']}")
            
            # 发送请求
            request_json = json.dumps(req) + "\n"
            process.stdin.write(request_json)
            process.stdin.flush()
            
            # 读取响应
            response_line = process.stdout.readline()
            if response_line:
                try:
                    response = json.loads(response_line.strip())
                    
                    if response.get("success"):
                        print(f"✅ 成功: {response['response'][:100]}...")
                        print(f"推理时间: {response['inference_time_ms']:.2f}ms")
                    else:
                        print(f"❌ 失败: {response.get('error', '未知错误')}")
                        
                except json.JSONDecodeError as e:
                    print(f"❌ 响应解析失败: {e}")
                    print(f"原始响应: {response_line}")
            else:
                print("❌ 未收到响应")
            
            print()
        
        # 获取统计信息
        print("📊 获取服务统计信息...")
        stats_request = json.dumps({"command": "stats"}) + "\n"
        process.stdin.write(stats_request)
        process.stdin.flush()
        
        stats_response = process.stdout.readline()
        if stats_response:
            try:
                stats_data = json.loads(stats_response.strip())
                print("统计信息:")
                for key, value in stats_data.get("stats", {}).items():
                    print(f"  {key}: {value}")
            except json.JSONDecodeError:
                print(f"统计信息解析失败: {stats_response}")
        
        # 关闭服务
        print("\n🛑 关闭推理服务...")
        shutdown_request = json.dumps({"command": "shutdown"}) + "\n"
        process.stdin.write(shutdown_request)
        process.stdin.flush()
        
        # 等待进程结束
        process.wait(timeout=10)
        print("✅ 推理服务测试完成")
        
    except subprocess.TimeoutExpired:
        print("⚠️ 服务关闭超时，强制终止")
        process.kill()
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        if process.poll() is None:
            process.terminate()
        raise

if __name__ == "__main__":
    test_inference_service()
