#!/bin/bash

# InferTuner GPU扩展性验证脚本
# 验证多GPU并行扩展的线性关系

set -e

cd /workspace/infertuner
FLINK_HOME="/opt/flink-1.17.2/"
SCRIPT_NAME="GPU扩展性验证"

echo "🚀 === ${SCRIPT_NAME} ==="
echo "验证目标: 多GPU并行扩展的线性关系"
echo ""

# =============================================================================
# 📋 实验配置参数
# =============================================================================
GPU_SCALING_REQUEST_COUNT=50
GPU_SCALING_REQUEST_INTERVAL=500  # ms
GPU_SCALING_PARALLELISM_CONFIGS=(1 2 4)

# 每次实验发送的推理请求数量（50），两次请求之间等待500ms
echo "📋 实验配置:"
echo "  请求数量: ${GPU_SCALING_REQUEST_COUNT}"
echo "  请求间隔: ${GPU_SCALING_REQUEST_INTERVAL}ms"  
echo "  并行度测试: ${GPU_SCALING_PARALLELISM_CONFIGS[*]}"
echo ""

# 创建结果目录
RESULTS_DIR="/mnt/tidal-alsh01/usr/suqian/results/gpu_scaling"
LOGS_DIR="/mnt/tidal-alsh01/usr/suqian/logs/gpu_scaling"
mkdir -p "$RESULTS_DIR" "$LOGS_DIR"

# 结果文件
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_FILE="${RESULTS_DIR}/gpu_scaling_${TIMESTAMP}.csv"
echo "parallelism,total_throughput,scale_factor,gpu_distribution,execution_time" > "$RESULT_FILE"

# 基准吞吐量
BASELINE_THROUGHPUT=0

for parallelism in "${GPU_SCALING_PARALLELISM_CONFIGS[@]}"; do
    echo "🔧 测试 ${parallelism}GPU 配置..."
    
    # 1. 重启Flink集群
    echo "  🔄 重启Flink集群..."
    $FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1 || true
    sleep 3
    
    # 清理日志
    find $FLINK_HOME/log/ -name "*.log" -exec rm -f {} \; 2>/dev/null || true
    
    $FLINK_HOME/bin/start-cluster.sh >/dev/null 2>&1
    sleep 5
    
#    # 2. 编译项目
#    echo "  🔨 编译项目..."
#    mvn clean package -q > /dev/null 2>&1
#    if [ $? -ne 0 ]; then
#        echo "  ❌ 编译失败"
#        continue
#    fi
    
    # 3. 运行实验
    echo "  🚀 运行GPU扩展性测试..."
    START_TIME=$(date +%s)
    
    $FLINK_HOME/bin/flink run -c com.infertuner.jobs.GPUScalingJob \
        target/infertuner-1.0.0.jar \
        $parallelism $GPU_SCALING_REQUEST_COUNT $GPU_SCALING_REQUEST_INTERVAL >/dev/null 2>&1
    
    # 4. 等待完成
    echo "  ⏳ 等待实验完成..."
    sleep 10
    
    WAIT_COUNT=0
    while $FLINK_HOME/bin/flink list 2>/dev/null | grep -q "RUNNING"; do
        sleep 5
        WAIT_COUNT=$((WAIT_COUNT + 1))
        if [ $WAIT_COUNT -gt 30 ]; then
            echo "    ⚠️ 超时，强制停止"
            $FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1 || true
            break
        fi
    done
    
    END_TIME=$(date +%s)
    EXECUTION_TIME=$((END_TIME - START_TIME))
    
    # 5. 提取并分析结果
    echo "  📊 分析实验结果..."
    LOG_FILE="${LOGS_DIR}/gpu_scaling_${parallelism}gpu_${TIMESTAMP}.log"
    
    # 复制TaskExecutor日志
    TASK_LOG=$(find $FLINK_HOME/log/ -name "*taskexecutor*.log" -type f | head -1)
    
    if [ -f "$TASK_LOG" ]; then
        cp "$TASK_LOG" "$LOG_FILE"
        
        # 从日志提取GPU统计信息
        GPU_STATS=$(grep -E "GPU-[0-9]+:.*吞吐量=" "$LOG_FILE" | tail -$parallelism)
        
        if [ ! -z "$GPU_STATS" ]; then
            TOTAL_THROUGHPUT=0
            GPU_DISTRIBUTION=""
            
            # 解析GPU统计
            while read -r line; do
                if [[ $line =~ GPU-([0-9]+):.*请求=([0-9]+).*吞吐量=([0-9.]+) ]]; then
                    gpu_id=${BASH_REMATCH[1]}
                    requests=${BASH_REMATCH[2]}
                    throughput=${BASH_REMATCH[3]}
                    
                    echo "    GPU-${gpu_id}: ${requests}请求, ${throughput}req/s"
                    
                    # 累加总吞吐量
                    TOTAL_THROUGHPUT=$(awk "BEGIN {print $TOTAL_THROUGHPUT + $throughput}")
                    
                    # 构建分布字符串
                    if [ -z "$GPU_DISTRIBUTION" ]; then
                        GPU_DISTRIBUTION="GPU${gpu_id}:${requests}"
                    else
                        GPU_DISTRIBUTION="${GPU_DISTRIBUTION};GPU${gpu_id}:${requests}"
                    fi
                fi
            done <<< "$GPU_STATS"
            
            # 计算扩展倍数
            if [ $parallelism -eq 1 ]; then
                BASELINE_THROUGHPUT=$TOTAL_THROUGHPUT
                SCALE_FACTOR="1.00"
            else
                if [ $(awk "BEGIN {print ($BASELINE_THROUGHPUT > 0)}") -eq 1 ]; then
                    SCALE_FACTOR=$(awk "BEGIN {printf \"%.2f\", $TOTAL_THROUGHPUT / $BASELINE_THROUGHPUT}")
                else
                    SCALE_FACTOR="N/A"
                fi
            fi
            
            echo "    📈 总吞吐量: ${TOTAL_THROUGHPUT} req/s"
            echo "    📊 扩展倍数: ${SCALE_FACTOR}x"
            echo "    ⏱️  执行时间: ${EXECUTION_TIME}s"
            
            # 保存到结果文件
            echo "${parallelism},${TOTAL_THROUGHPUT},${SCALE_FACTOR},${GPU_DISTRIBUTION},${EXECUTION_TIME}" >> "$RESULT_FILE"
            
        else
            echo "    ❌ 未找到GPU统计信息"
            echo "${parallelism},0,0,无数据,${EXECUTION_TIME}" >> "$RESULT_FILE"
        fi
        
    else
        echo "    ❌ 未找到日志文件"
        echo "${parallelism},0,0,日志缺失,${EXECUTION_TIME}" >> "$RESULT_FILE"
    fi
    
    echo "  ✅ ${parallelism}GPU测试完成"
    echo ""
done

# 停止Flink
echo "🛑 停止Flink集群..."
$FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1 || true

echo ""
echo "🎉 === GPU扩展性验证完成 ==="
echo ""

# 显示结果分析
echo "📊 === 扩展性分析结果 ==="
printf "%-12s %-15s %-12s %-30s %-12s\n" "配置" "总吞吐量" "扩展倍数" "GPU负载分布" "执行时间"
printf "%-12s %-15s %-12s %-30s %-12s\n" "----------" "-------------" "----------" "----------------------------" "----------"

tail -n +2 "$RESULT_FILE" | while IFS=, read parallelism throughput scale_factor distribution exec_time; do
    printf "%-12s %-15s %-12s %-30s %-12s\n" \
        "${parallelism}GPU" \
        "${throughput}req/s" \
        "${scale_factor}x" \
        "$distribution" \
        "${exec_time}s"
done

echo ""

# 扩展效率分析
if [ -f "$RESULT_FILE" ] && [ $(wc -l < "$RESULT_FILE") -gt 2 ]; then
    echo "🔍 === 扩展效率分析 ==="
    
    # 分析2GPU和4GPU的扩展效率
    DUAL_GPU_SCALE=$(grep "^2," "$RESULT_FILE" | cut -d',' -f3)
    QUAD_GPU_SCALE=$(grep "^4," "$RESULT_FILE" | cut -d',' -f3)
    
    if [[ $DUAL_GPU_SCALE =~ ^[0-9.]+$ ]]; then
        DUAL_EFFICIENCY=$(awk "BEGIN {printf \"%.1f\", $DUAL_GPU_SCALE / 2 * 100}")
        echo "📊 2GPU扩展效率: ${DUAL_EFFICIENCY}% (理想值100%)"
        
        if [ $(awk "BEGIN {print ($DUAL_EFFICIENCY >= 80)}") -eq 1 ]; then
            echo "    ✅ 扩展效果优秀"
        elif [ $(awk "BEGIN {print ($DUAL_EFFICIENCY >= 60)}") -eq 1 ]; then
            echo "    ⚠️  扩展效果良好"  
        else
            echo "    ❌ 扩展效果需要优化"
        fi
    fi
    
    if [[ $QUAD_GPU_SCALE =~ ^[0-9.]+$ ]]; then
        QUAD_EFFICIENCY=$(awk "BEGIN {printf \"%.1f\", $QUAD_GPU_SCALE / 4 * 100}")
        echo "📊 4GPU扩展效率: ${QUAD_EFFICIENCY}% (理想值100%)"
        
        if [ $(awk "BEGIN {print ($QUAD_EFFICIENCY >= 70)}") -eq 1 ]; then
            echo "    ✅ 4GPU扩展效果优秀"
        elif [ $(awk "BEGIN {print ($QUAD_EFFICIENCY >= 50)}") -eq 1 ]; then
            echo "    ⚠️  4GPU扩展效果良好"
        else
            echo "    ❌ 4GPU扩展效果需要优化"
        fi
    fi
fi

echo ""
echo "📁 输出文件:"
echo "  📈 结果数据: $RESULT_FILE"
echo "  📋 详细日志: $LOGS_DIR/gpu_scaling_*gpu_${TIMESTAMP}.log"
echo ""

echo "💡 理论验证:"
echo "  ✅ 线性扩展: 2GPU ≈ 2x, 4GPU ≈ 4x"
echo "  ⚠️  亚线性扩展: 由于通信开销和资源竞争"
echo "  📊 负载均衡: 各GPU请求分布应相对均匀"
echo ""

echo "🚀 下一步: 运行批处理权衡验证实验"
echo "   ./scripts/run_batch_analysis.sh"