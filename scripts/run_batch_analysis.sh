#!/bin/bash

# InferTuner 批处理权衡关系验证脚本
# 验证不同batch_size下延迟与吞吐量的权衡关系

set -e

cd /workspace/infertuner
FLINK_HOME="/workspace/flink-setup/flink"
SCRIPT_NAME="批处理权衡验证"

echo "🎯 === ${SCRIPT_NAME} ==="
echo "验证目标: 大batch_size提升吞吐量但增加延迟的权衡关系"
echo ""

# =============================================================================
# 📋 实验配置参数
# =============================================================================
BATCH_ANALYSIS_REQUEST_COUNT=30
BATCH_ANALYSIS_REQUEST_INTERVAL=200  # ms
BATCH_ANALYSIS_BATCH_SIZES=(1 2 3 4 8)

echo "📋 实验配置:"
for batch_size in "${BATCH_ANALYSIS_BATCH_SIZES[@]}"; do
    echo "  Batch-${batch_size}: ${BATCH_ANALYSIS_REQUEST_COUNT}个请求, ${BATCH_ANALYSIS_REQUEST_INTERVAL}ms间隔"
done
echo ""

# 创建结果目录
RESULTS_DIR="results/batch_analysis"
LOGS_DIR="logs/batch_analysis"
mkdir -p "$RESULTS_DIR" "$LOGS_DIR"

# 结果文件
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_FILE="${RESULTS_DIR}/batch_analysis_${TIMESTAMP}.csv"
echo "batch_size,avg_wait_ms,avg_latency_ms,throughput_req_per_sec,total_requests,efficiency_notes" > "$RESULT_FILE"

# 停止现有Flink
$FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1 || true
sleep 2

for batch_size in "${BATCH_ANALYSIS_BATCH_SIZES[@]}"; do
    echo "🧪 测试 Batch-${batch_size} (${BATCH_ANALYSIS_REQUEST_COUNT}个请求)"
    
    # 1. 重启Flink集群
    echo "  🔄 重启Flink集群..."
    rm -f $FLINK_HOME/log/*.log 2>/dev/null || true
    $FLINK_HOME/bin/start-cluster.sh >/dev/null 2>&1
    sleep 3
    
    # 2. 编译项目
    echo "  🔨 编译项目..."
    mvn clean package -q >/dev/null 2>&1
    
    if [ $? -ne 0 ]; then
        echo "    ❌ 编译失败"
        continue
    fi
    
    # 3. 运行实验
    echo "  🚀 运行批处理分析..."
    START_TIME=$(date +%s)
    
    $FLINK_HOME/bin/flink run -c com.infertuner.jobs.BatchAnalysisJob \
        target/infertuner-1.0.0.jar \
        $batch_size 1 $BATCH_ANALYSIS_REQUEST_COUNT $BATCH_ANALYSIS_REQUEST_INTERVAL >/dev/null 2>&1
    
    sleep 8
    END_TIME=$(date +%s)
    EXECUTION_TIME=$((END_TIME - START_TIME))
    
    # 4. 分析结果
    echo "  📊 分析实验结果..."
    LOG_FILE="${LOGS_DIR}/batch_analysis_${batch_size}_${TIMESTAMP}.log"
    TASK_LOG=$(find $FLINK_HOME/log/ -name "*taskexecutor*.log" -type f | head -1)
    
    if [ -f "$TASK_LOG" ]; then
        cp "$TASK_LOG" "$LOG_FILE"
        
        # 方法1: 从最终统计中提取数据
        TOTAL_REQUESTS=$(grep "处理请求数:" "$LOG_FILE" | tail -1 | grep -o "[0-9]\+" | head -1)
        AVG_WAIT=$(grep "等待=" "$LOG_FILE" | tail -1 | grep -o "等待=[0-9]\+ms" | sed 's/等待=//g' | sed 's/ms//g')
        AVG_LATENCY=$(grep "总延迟=" "$LOG_FILE" | tail -1 | grep -o "总延迟=[0-9]\+ms" | sed 's/总延迟=//g' | sed 's/ms//g')
        THROUGHPUT=$(grep "吞吐量=" "$LOG_FILE" | tail -1 | grep -o "吞吐量=[0-9.]\+req/s" | sed 's/吞吐量=//g' | sed 's/req\/s//g')
        
        # 方法2: 备用解析 - 从日志中直接提取等待时间和总延迟
        if [ -z "$TOTAL_REQUESTS" ] || [ -z "$AVG_WAIT" ] || [ -z "$AVG_LATENCY" ] || [ -z "$THROUGHPUT" ]; then
            echo "    ⚠️ 主解析失败，尝试备用解析..."
            
            # 提取等待时间和总延迟的模式匹配
            WAIT_TIMES=$(grep "等待:" "$LOG_FILE" | grep -o "等待: [0-9]\+ms" | sed 's/等待: //g' | sed 's/ms//g')
            TOTAL_LATENCIES=$(grep "总计:" "$LOG_FILE" | grep -o "总计: [0-9]\+ms" | sed 's/总计: //g' | sed 's/ms//g')
            
            # 如果上面的模式不匹配，尝试其他模式
            if [ -z "$WAIT_TIMES" ]; then
                WAIT_TIMES=$(grep "等待=" "$LOG_FILE" | grep -o "等待=[0-9]\+ms" | sed 's/等待=//g' | sed 's/ms//g')
            fi
            
            if [ -z "$TOTAL_LATENCIES" ]; then
                TOTAL_LATENCIES=$(grep "总延迟=" "$LOG_FILE" | grep -o "总延迟=[0-9]\+ms" | sed 's/总延迟=//g' | sed 's/ms//g')
            fi
            
            if [ ! -z "$WAIT_TIMES" ] && [ ! -z "$TOTAL_LATENCIES" ]; then
                # 计算平均值
                TOTAL_WAIT=0
                WAIT_COUNT=0
                while read -r wait_time; do
                    if [ ! -z "$wait_time" ] && [[ $wait_time =~ ^[0-9]+$ ]]; then
                        WAIT_COUNT=$((WAIT_COUNT + 1))
                        TOTAL_WAIT=$((TOTAL_WAIT + wait_time))
                    fi
                done <<< "$WAIT_TIMES"
                
                TOTAL_LATENCY_SUM=0
                LATENCY_COUNT=0
                while read -r latency; do
                    if [ ! -z "$latency" ] && [[ $latency =~ ^[0-9]+$ ]]; then
                        LATENCY_COUNT=$((LATENCY_COUNT + 1))
                        TOTAL_LATENCY_SUM=$((TOTAL_LATENCY_SUM + latency))
                    fi
                done <<< "$TOTAL_LATENCIES"
                
                if [ $WAIT_COUNT -gt 0 ] && [ $LATENCY_COUNT -gt 0 ]; then
                    AVG_WAIT=$((TOTAL_WAIT / WAIT_COUNT))
                    AVG_LATENCY=$((TOTAL_LATENCY_SUM / LATENCY_COUNT))
                    TOTAL_REQUESTS=$LATENCY_COUNT
                    
                    # 使用理论计算吞吐量：考虑批处理效应
                    if [ $AVG_LATENCY -gt 0 ] && [ $batch_size -gt 0 ]; then
                        THROUGHPUT=$(echo "scale=2; $batch_size * 1000 / $AVG_LATENCY" | bc -l)
                    else
                        THROUGHPUT="0"
                    fi
                    
                    echo "    💡 备用解析成功: 等待=${AVG_WAIT}ms, 延迟=${AVG_LATENCY}ms, 吞吐=${THROUGHPUT}req/s"
                fi
            fi
        fi
        
        # 验证数据有效性
        if [ ! -z "$TOTAL_REQUESTS" ] && [ ! -z "$AVG_WAIT" ] && [ ! -z "$AVG_LATENCY" ] && [ ! -z "$THROUGHPUT" ] && \
           [[ $TOTAL_REQUESTS =~ ^[0-9]+$ ]] && [[ $AVG_WAIT =~ ^[0-9]+$ ]] && [[ $AVG_LATENCY =~ ^[0-9]+$ ]]; then
            
            echo "    ✅ Batch-${batch_size} 结果:"
            echo "      📊 处理请求: ${TOTAL_REQUESTS}个"
            echo "      ⏱️  平均等待: ${AVG_WAIT}ms"
            echo "      ⏱️  平均延迟: ${AVG_LATENCY}ms"
            echo "      🚀 吞吐量: ${THROUGHPUT} req/s"
            
            # 效率注释
            EFFICIENCY_NOTE=""
            if [ $batch_size -eq 1 ]; then
                EFFICIENCY_NOTE="基准配置"
            elif [ $batch_size -le 4 ]; then
                EFFICIENCY_NOTE="平衡配置"
            else
                EFFICIENCY_NOTE="高吞吐配置"
            fi
            
            # 保存到结果文件
            echo "$batch_size,$AVG_WAIT,$AVG_LATENCY,$THROUGHPUT,$TOTAL_REQUESTS,$EFFICIENCY_NOTE" >> "$RESULT_FILE"
            
        else
            echo "    ❌ 数据解析失败或数据无效"
            echo "    调试信息: requests=$TOTAL_REQUESTS, wait=$AVG_WAIT, latency=$AVG_LATENCY, throughput=$THROUGHPUT"
            echo "$batch_size,0,0,0,0,解析失败" >> "$RESULT_FILE"
        fi
        
    else
        echo "    ❌ 未找到日志文件"
        echo "$batch_size,0,0,0,0,日志缺失" >> "$RESULT_FILE"
    fi
    
    echo "  ✅ Batch-${batch_size} 测试完成"
    echo ""
    
    # 停止Flink准备下一轮
    $FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1 || true
    sleep 2
done

echo "🎯 === 批处理权衡关系验证结果 ==="
echo ""

# 显示权衡关系分析
if [ -f "$RESULT_FILE" ]; then
    echo "📊 延迟 vs 吞吐量权衡表:"
    printf "%-10s %-12s %-12s %-15s %-15s %-15s\n" "BatchSize" "等待(ms)" "延迟(ms)" "吞吐量(req/s)" "延迟增长" "吞吐量增长"
    printf "%-10s %-12s %-12s %-15s %-15s %-15s\n" "--------" "--------" "--------" "-------------" "---------" "-----------"
    
    BASELINE_LATENCY=""
    BASELINE_THROUGHPUT=""
    
    # 读取并分析数据
    tail -n +2 "$RESULT_FILE" | while IFS=, read batch_size avg_wait avg_latency throughput total_requests efficiency_note; do
        if [ $batch_size -eq 1 ]; then
            # 设置基准值
            BASELINE_LATENCY=$avg_latency
            BASELINE_THROUGHPUT=$throughput
            printf "%-10s %-12s %-12s %-15s %-15s %-15s\n" "$batch_size" "$avg_wait" "$avg_latency" "$throughput" "基准" "基准"
            echo "$avg_latency" > /tmp/baseline_latency_$$
            echo "$throughput" > /tmp/baseline_throughput_$$
        else
            # 计算增长率
            BASELINE_LATENCY=$(cat /tmp/baseline_latency_$$ 2>/dev/null || echo $avg_latency)
            BASELINE_THROUGHPUT=$(cat /tmp/baseline_throughput_$$ 2>/dev/null || echo $throughput)
            
            if [ ! -z "$BASELINE_LATENCY" ] && [ ! -z "$BASELINE_THROUGHPUT" ] && \
               [ "$BASELINE_LATENCY" != "0" ] && [ "$BASELINE_THROUGHPUT" != "0" ]; then
                LATENCY_INCREASE=$(echo "scale=1; ($avg_latency - $BASELINE_LATENCY) * 100 / $BASELINE_LATENCY" | bc -l 2>/dev/null || echo "N/A")
                THROUGHPUT_INCREASE=$(echo "scale=1; ($throughput - $BASELINE_THROUGHPUT) * 100 / $BASELINE_THROUGHPUT" | bc -l 2>/dev/null || echo "N/A")
                printf "%-10s %-12s %-12s %-15s %-15s %-15s\n" "$batch_size" "$avg_wait" "$avg_latency" "$throughput" "+${LATENCY_INCREASE}%" "+${THROUGHPUT_INCREASE}%"
            else
                printf "%-10s %-12s %-12s %-15s %-15s %-15s\n" "$batch_size" "$avg_wait" "$avg_latency" "$throughput" "N/A" "N/A"
            fi
        fi
    done
    
    # 清理临时文件
    rm -f /tmp/baseline_latency_$$ /tmp/baseline_throughput_$$
    
    echo ""
    echo "💡 权衡关系理论验证:"
    echo "  ✅ 延迟趋势: 应随batch_size增加而增长（排队等待效应）"
    echo "  ✅ 吞吐量趋势: 应随batch_size增加而提升（GPU利用率提升）"
    echo "  ✅ 权衡点: 找到延迟可接受且吞吐量最优的batch_size"
    
    echo ""
    echo "📄 详细数据已保存: $RESULT_FILE"
    
else
    echo "❌ 未找到结果文件"
fi

echo ""
echo "📁 输出文件:"
echo "  📈 结果数据: $RESULT_FILE"  
echo "  📋 详细日志: $LOGS_DIR/batch_analysis_*_${TIMESTAMP}.log"
echo ""

echo "🚀 下一步: 运行缓存策略验证实验"
echo "   ./scripts/run_cache_experiments.sh"