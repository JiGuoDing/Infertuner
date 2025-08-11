#!/bin/bash

echo "🔍 调试Python推理服务启动问题"
echo ""

MODEL_PATH="/workspace/models/Qwen1.5-1.8B-Chat"
SCRIPT_PATH="/workspace/infertuner-simple/scripts/simple_inference_service.py"

# 1. 检查文件是否存在
echo "1. 检查文件存在性:"
if [ -f "$SCRIPT_PATH" ]; then
    echo "✅ Python脚本存在: $SCRIPT_PATH"
else
    echo "❌ Python脚本不存在: $SCRIPT_PATH"
    exit 1
fi

if [ -d "$MODEL_PATH" ]; then
    echo "✅ 模型目录存在: $MODEL_PATH"
else
    echo "❌ 模型目录不存在: $MODEL_PATH"
    echo "可用的模型目录:"
    ls -la /workspace/models/ 2>/dev/null || echo "无法访问 /workspace/models/"
    exit 1
fi

echo ""

# 2. 检查Python环境
echo "2. 检查Python环境:"
python3 --version
echo "检查依赖包:"
python3 -c "import torch; print('PyTorch版本:', torch.__version__)"
python3 -c "import transformers; print('Transformers版本:', transformers.__version__)"

echo ""

# 3. 尝试启动Python服务（短时间测试）
echo "3. 测试Python服务启动:"
echo "启动命令: python3 $SCRIPT_PATH $MODEL_PATH"
echo ""

# 使用timeout防止长时间等待
timeout 15s python3 "$SCRIPT_PATH" "$MODEL_PATH" &
PYTHON_PID=$!

echo "Python进程PID: $PYTHON_PID"

# 等待几秒看看进程是否还活着
sleep 3

if ps -p $PYTHON_PID > /dev/null 2>&1; then
    echo "✅ Python进程运行中"
    
    # 杀掉进程
    kill $PYTHON_PID 2>/dev/null
    echo "已停止测试进程"
else
    echo "❌ Python进程已退出"
    echo ""
    echo "尝试获取错误信息:"
    # 直接运行看错误输出
    timeout 5s python3 "$SCRIPT_PATH" "$MODEL_PATH" 2>&1 | head -20
fi

echo ""
echo "4. 建议:"
echo "如果Python服务无法启动，可能的原因:"
echo "- 模型文件损坏或不完整"
echo "- 显存不足"
echo "- 缺少依赖包"
echo "- CUDA环境问题"