#!/bin/bash

cd /workspace/infertuner-simple

FLINK_HOME="/workspace/flink-setup/flink"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "🚀 === 多GPU扩展性测试 ==="
echo ""

# =============================================================================
# 🔧 参数配置
# =============================================================================
REQUEST_COUNT=50
REQUEST_INTERVAL=500  # ms
PARALLELISM_CONFIGS=(1 2 3 4)

echo "📋 测试配置:"
echo "  请求数量: ${REQUEST_COUNT}"
echo "  请求间隔: ${REQUEST_INTERVAL}ms"  
echo "  并行度: ${PARALLELISM_CONFIGS[*]}"
echo ""

# 创建输出目录
LOGS_DIR="${SCRIPT_DIR}/logs"
mkdir -p "$LOGS_DIR"

# 结果文件
RESULT_FILE="${SCRIPT_DIR}/gpu_scaling_results.csv"
echo "配置,总吞吐量,扩展倍数,GPU详情" > "$RESULT_FILE"

# 存储基准性能
BASELINE_THROUGHPUT=0

for parallelism in "${PARALLELISM_CONFIGS[@]}"; do
    echo "🔧 测试 ${parallelism}GPU 配置..."
    
    # 1. 重启Flink
    echo "  🔄 重启Flink集群..."
    $FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1 || true
    sleep 3
    
    # 清理旧日志
    find $FLINK_HOME/log/ -name "*.log" -exec rm -f {} \; 2>/dev/null || true
    
    $FLINK_HOME/bin/start-cluster.sh >/dev/null 2>&1
    sleep 5
    
    # 2. 编译运行
    echo "  🔨 编译..."
    mvn clean package -q > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "  ❌ 编译失败"
        continue
    fi
    
    echo "  🚀 运行实验..."
    
    # 记录开始时间
    START_TIME=$(date +%s)
    
    $FLINK_HOME/bin/flink run -c com.infertuner.MultiGPUInferenceJob \
        target/infertuner-simple-1.0.0.jar \
        $parallelism $REQUEST_COUNT $REQUEST_INTERVAL >/dev/null 2>&1
    
    # 3. 等待完成
    echo "  ⏳ 等待完成..."
    sleep 10
    
    WAIT_COUNT=0
    while $FLINK_HOME/bin/flink list 2>/dev/null | grep -q "RUNNING"; do
        sleep 5
        WAIT_COUNT=$((WAIT_COUNT + 1))
        if [ $WAIT_COUNT -gt 30 ]; then
            echo "    ⚠️ 超时，强制停止"
            $FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1 || true
            sleep 3
            break
        fi
    done
    
    END_TIME=$(date +%s)
    TOTAL_TIME=$((END_TIME - START_TIME))
    
    # 4. 复制日志并提取数据
    echo "  📋 复制日志..."
    LOG_FILE="${LOGS_DIR}/multi_gpu_${parallelism}gpu.log"
    
    # 找到TaskExecutor日志
    TASK_LOG=$(find $FLINK_HOME/log/ -name "*taskexecutor*.log" -type f | head -1)
    
    if [ -f "$TASK_LOG" ]; then
        cp "$TASK_LOG" "$LOG_FILE"
        echo "    日志已复制: $(basename $TASK_LOG) → $(basename $LOG_FILE)"
        
        # 5. 从日志提取统计信息
        echo "  📊 分析日志数据..."
        
        # 提取GPU统计信息（从最后的统计输出）
        GPU_STATS=$(grep -E "GPU-[0-9]+: 请求=" "$LOG_FILE" | tail -$parallelism)
        
        if [ ! -z "$GPU_STATS" ]; then
            echo "    GPU分布详情:"
            
            TOTAL_THROUGHPUT=0
            GPU_DETAILS=""
            
            while read -r line; do
                if [[ $line =~ GPU-([0-9]+):.*请求=([0-9]+).*吞吐量=([0-9.]+) ]]; then
                    gpu_id=${BASH_REMATCH[1]}
                    requests=${BASH_REMATCH[2]}
                    throughput=${BASH_REMATCH[3]}
                    
                    echo "      GPU-${gpu_id}: ${requests}请求, ${throughput}req/s"
                    
                    # 累加吞吐量
                    TOTAL_THROUGHPUT=$(awk "BEGIN {print $TOTAL_THROUGHPUT + $throughput}")
                    
                    # 构建详情字符串
                    if [ -z "$GPU_DETAILS" ]; then
                        GPU_DETAILS="GPU${gpu_id}:${requests}"
                    else
                        GPU_DETAILS="${GPU_DETAILS},GPU${gpu_id}:${requests}"
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
            echo "    ⏱️  执行时间: ${TOTAL_TIME}秒"
            
            # 保存结果
            echo "${parallelism}GPU,${TOTAL_THROUGHPUT},${SCALE_FACTOR},${GPU_DETAILS}" >> "$RESULT_FILE"
            
        else
            echo "    ❌ 未找到GPU统计信息"
            
            # 尝试从整体统计提取
            OVERALL_STATS=$(grep "总请求:" "$LOG_FILE" | tail -1)
            if [ ! -z "$OVERALL_STATS" ]; then
                # 提取吞吐量（简化处理）
                if [[ $OVERALL_STATS =~ 吞吐量:.*([0-9.]+).*req/s ]]; then
                    TOTAL_THROUGHPUT=${BASH_REMATCH[1]}
                    echo "    📈 从整体统计提取吞吐量: ${TOTAL_THROUGHPUT} req/s"
                    echo "${parallelism}GPU,${TOTAL_THROUGHPUT},估算,统计数据不完整" >> "$RESULT_FILE"
                else
                    echo "${parallelism}GPU,0,0,日志解析失败" >> "$RESULT_FILE"
                fi
            else
                echo "${parallelism}GPU,0,0,无统计数据" >> "$RESULT_FILE"
            fi
        fi
        
    else
        echo "    ❌ 未找到TaskExecutor日志文件"
        echo "${parallelism}GPU,0,0,日志文件缺失" >> "$RESULT_FILE"
    fi
    
    echo "  ✅ 完成"
    echo ""
done

# 停止Flink
echo "🛑 停止Flink集群..."
$FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1 || true

echo ""
echo "🎉 === 多GPU扩展性测试完成 ==="
echo ""

# 输出最终结果表格
echo "📊 === 扩展性对比结果 ==="
printf "%-8s %-15s %-10s %-30s\n" "配置" "总吞吐量" "扩展倍数" "GPU负载分布"
printf "%-8s %-15s %-10s %-30s\n" "------" "-------------" "--------" "----------------------------"

tail -n +2 "$RESULT_FILE" | while IFS=, read config throughput scale_factor details; do
    printf "%-8s %-15s %-10s %-30s\n" "$config" "${throughput}req/s" "${scale_factor}x" "$details"
done

echo ""

# 扩展性分析
if [ -f "$RESULT_FILE" ] && [ $(wc -l < "$RESULT_FILE") -gt 1 ]; then
    echo "🔍 === 扩展性分析 ==="
    
    # 分析扩展效率
    DUAL_GPU_LINE=$(grep "2GPU," "$RESULT_FILE" 2>/dev/null || echo "")
    QUAD_GPU_LINE=$(grep "4GPU," "$RESULT_FILE" 2>/dev/null || echo "")
    
    if [ ! -z "$DUAL_GPU_LINE" ]; then
        DUAL_SCALE=$(echo "$DUAL_GPU_LINE" | cut -d',' -f3)
        if [[ $DUAL_SCALE =~ ^[0-9.]+$ ]]; then
            DUAL_EFFICIENCY=$(awk "BEGIN {printf \"%.1f\", $DUAL_SCALE / 2 * 100}")
            echo "📊 2GPU扩展效率: ${DUAL_EFFICIENCY}% (理想值100%)"
            
            if [ $(awk "BEGIN {print ($DUAL_EFFICIENCY >= 80)}") -eq 1 ]; then
                echo "    ✅ 扩展效果优秀"
            elif [ $(awk "BEGIN {print ($DUAL_EFFICIENCY >= 60)}") -eq 1 ]; then
                echo "    ⚠️  扩展效果良好"  
            else
                echo "    ❌ 扩展效果较差，需优化"
            fi
        fi
    fi
    
    if [ ! -z "$QUAD_GPU_LINE" ]; then
        QUAD_SCALE=$(echo "$QUAD_GPU_LINE" | cut -d',' -f3)
        if [[ $QUAD_SCALE =~ ^[0-9.]+$ ]]; then
            QUAD_EFFICIENCY=$(awk "BEGIN {printf \"%.1f\", $QUAD_SCALE / 4 * 100}")
            echo "📊 4GPU扩展效率: ${QUAD_EFFICIENCY}% (理想值100%)"
            
            if [ $(awk "BEGIN {print ($QUAD_EFFICIENCY >= 80)}") -eq 1 ]; then
                echo "    ✅ 扩展效果优秀"
            elif [ $(awk "BEGIN {print ($QUAD_EFFICIENCY >= 60)}") -eq 1 ]; then
                echo "    ⚠️  扩展效果良好"
            else
                echo "    ❌ 扩展效果较差，需优化"
            fi
        fi
    fi
fi

echo ""
echo "📁 输出文件:"
echo "  汇总结果: $RESULT_FILE"
echo "  详细日志: $LOGS_DIR/multi_gpu_*gpu.log"
echo ""

echo "💡 下一步:"
echo "  ✅ 如果扩展效果良好: 进入batch_size维度测试"
echo "  🔧 如果扩展效果不佳: 检查负载均衡和通信开销"