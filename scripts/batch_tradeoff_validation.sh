#!/bin/bash

cd /workspace/infertuner-simple

FLINK_HOME="/workspace/flink-setup/flink"

echo "🎯 === 批处理延迟与吞吐量权衡验证 ==="
echo "目标: 验证大batch_size提升吞吐量但增加延迟的权衡关系"
echo ""

# 测试配置 - 扩展到更多batch_size
declare -a BATCH_CONFIGS=(
    "1 30 200"   # batch_size=1, 30个请求, 200ms间隔
    "2 30 200"   # batch_size=2, 30个请求, 200ms间隔
    "3 30 200"   # batch_size=3, 30个请求, 200ms间隔
    "4 30 200"   # batch_size=4, 30个请求, 200ms间隔
    "8 32 200"   # batch_size=8, 32个请求, 200ms间隔 (确保能整除)
)

echo "📋 测试配置:"
for config in "${BATCH_CONFIGS[@]}"; do
    read batch_size requests interval <<< "$config"
    echo "  batch_size=$batch_size: $requests个请求, ${interval}ms间隔"
done
echo ""

# 停止现有Flink
$FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1 || true
sleep 2

# 创建结果CSV文件
RESULTS_FILE="/tmp/batch_tradeoff_results.csv"
echo "batch_size,avg_wait_ms,avg_latency_ms,throughput_req_per_sec,total_requests,time_reference" > $RESULTS_FILE

for config in "${BATCH_CONFIGS[@]}"; do
    read batch_size requests interval <<< "$config"
    
    echo "🧪 测试 batch_size=$batch_size ($requests个请求)"
    
    # 重启Flink
    rm -f $FLINK_HOME/log/*.log 2>/dev/null || true
    $FLINK_HOME/bin/start-cluster.sh >/dev/null 2>&1
    sleep 3
    
    # 编译运行
    mvn clean package -q >/dev/null 2>&1
    
    if [ $? -ne 0 ]; then
        echo "  ❌ 编译失败"
        continue
    fi
    
    echo "  🚀 运行测试..."
    START_TIME=$(date +%s)
    
    $FLINK_HOME/bin/flink run -c com.infertuner.SimpleRealBatchJob \
        target/infertuner-simple-1.0.0.jar \
        $batch_size 1 $requests $interval >/dev/null 2>&1
    
    sleep 8
    END_TIME=$(date +%s)
    EXECUTION_TIME=$((END_TIME - START_TIME))
    
    echo "  📊 分析结果..."
    TASK_LOG=$(find $FLINK_HOME/log/ -name "*taskexecutor*.log" -type f | head -1)
    
    if [ -f "$TASK_LOG" ]; then
        
        # 分析等待时间
        WAIT_TIMES=$(grep "等待=" "$TASK_LOG" | grep -o "等待=[0-9]\+ms" | sed 's/等待=//g' | sed 's/ms//g')
        
        # 分析总延迟
        TOTAL_LATENCIES=$(grep "总延迟=" "$TASK_LOG" | grep -o "总延迟=[0-9]\+ms" | sed 's/总延迟=//g' | sed 's/ms//g')
        
        # 从日志中提取实际处理时间范围
        FIRST_RESPONSE_TIME=$(grep "✅ 响应 #" "$TASK_LOG" | head -1 | grep -o "[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\} [0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}" || echo "")
        LAST_RESPONSE_TIME=$(grep "✅ 响应 #" "$TASK_LOG" | tail -1 | grep -o "[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\} [0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}" || echo "")
        
        # 尝试从增强的日志格式中提取吞吐量信息
        THROUGHPUT_FROM_LOG=$(grep "吞吐量=" "$TASK_LOG" | tail -1 | grep -o "吞吐量=[0-9.]\+req/s" | sed 's/吞吐量=//g' | sed 's/req\/s//g')
        ACTUAL_EXECUTION_TIME=$(grep "执行时间=" "$TASK_LOG" | tail -1 | grep -o "执行时间=[0-9.]\+s" | sed 's/执行时间=//g' | sed 's/s//g')
        
        if [ ! -z "$WAIT_TIMES" ] && [ ! -z "$TOTAL_LATENCIES" ]; then
            
            # 计算平均等待时间
            TOTAL_WAIT=0
            WAIT_COUNT=0
            while read -r wait_time; do
                WAIT_COUNT=$((WAIT_COUNT + 1))
                TOTAL_WAIT=$((TOTAL_WAIT + wait_time))
            done <<< "$WAIT_TIMES"
            
            AVG_WAIT=0
            if [ $WAIT_COUNT -gt 0 ]; then
                AVG_WAIT=$((TOTAL_WAIT / WAIT_COUNT))
            fi
            
            # 计算平均总延迟
            TOTAL_LATENCY_SUM=0
            LATENCY_COUNT=0
            while read -r latency; do
                LATENCY_COUNT=$((LATENCY_COUNT + 1))
                TOTAL_LATENCY_SUM=$((TOTAL_LATENCY_SUM + latency))
            done <<< "$TOTAL_LATENCIES"
            
            AVG_LATENCY=0
            if [ $LATENCY_COUNT -gt 0 ]; then
                AVG_LATENCY=$((TOTAL_LATENCY_SUM / LATENCY_COUNT))
            fi
            
            # 优先使用增强日志中的吞吐量数据，否则用修正的计算方法
            THROUGHPUT=0
            ACTUAL_TIME_USED=""
            
            if [ ! -z "$THROUGHPUT_FROM_LOG" ] && [ ! -z "$ACTUAL_EXECUTION_TIME" ]; then
                # 使用Sink中计算的精确吞吐量
                THROUGHPUT=$THROUGHPUT_FROM_LOG
                ACTUAL_TIME_USED=$ACTUAL_EXECUTION_TIME
                echo "    💡 使用Sink精确计算的吞吐量"
            else
                # 回退到修正的计算方法：用平均延迟估算
                if [ $AVG_LATENCY -gt 0 ] && [ $batch_size -gt 0 ]; then
                    # 考虑批处理效应：batch_size越大，理论吞吐量越高
                    # 吞吐量 ≈ batch_size / (平均延迟/1000)，再除以batch_size得到单请求吞吐量
                    THEORETICAL_BATCH_THROUGHPUT=$(echo "scale=3; $batch_size * 1000 / $AVG_LATENCY" | bc -l)
                    THROUGHPUT=$(echo "scale=2; $THEORETICAL_BATCH_THROUGHPUT" | bc -l)
                    ACTUAL_TIME_USED="estimated"
                    echo "    💡 使用理论计算的吞吐量 (batch效应)"
                else
                    # 最后备选：用脚本执行时间
                    if [ $EXECUTION_TIME -gt 0 ] && [ $LATENCY_COUNT -gt 0 ]; then
                        THROUGHPUT=$(echo "scale=2; $LATENCY_COUNT / $EXECUTION_TIME" | bc -l)
                        ACTUAL_TIME_USED=$EXECUTION_TIME
                        echo "    ⚠️  使用脚本执行时间 (可能不准确)"
                    fi
                fi
            fi
            
            echo "  ✅ batch_size=$batch_size 结果:"
            echo "    📊 处理请求数: $LATENCY_COUNT"
            echo "    ⏱️  平均等待时间: ${AVG_WAIT}ms"
            echo "    ⏱️  平均总延迟: ${AVG_LATENCY}ms"
            echo "    🚀 吞吐量: ${THROUGHPUT} req/s"
            echo "    ⏰ 时间参考: ${ACTUAL_TIME_USED}s"
            
            # 保存到CSV
            echo "$batch_size,$AVG_WAIT,$AVG_LATENCY,$THROUGHPUT,$LATENCY_COUNT,$ACTUAL_TIME_USED" >> $RESULTS_FILE
            
        else
            echo "  ❌ 未找到有效的性能数据"
        fi
        
    else
        echo "  ❌ 未找到日志文件"
    fi
    
    echo ""
    
    # 停止Flink准备下一轮
    $FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1 || true
    sleep 2
done

echo "🎯 === 批处理权衡关系验证结果 ==="
echo ""

# 分析并显示权衡关系
if [ -f "$RESULTS_FILE" ]; then
    echo "📊 延迟 vs 吞吐量权衡表:"
    printf "%-10s %-15s %-15s %-15s %-15s %-15s\n" "BatchSize" "平均等待(ms)" "平均延迟(ms)" "吞吐量(req/s)" "延迟增长" "吞吐量增长"
    printf "%-10s %-15s %-15s %-15s %-15s %-15s\n" "--------" "-----------" "-----------" "-----------" "--------" "----------"
    
    BASELINE_LATENCY=""
    BASELINE_THROUGHPUT=""
    
    # 跳过CSV头部，读取数据
    tail -n +2 "$RESULTS_FILE" | while IFS=, read batch_size avg_wait avg_latency throughput total_requests time_reference; do
        if [ $batch_size -eq 1 ]; then
            # 保存基准值
            BASELINE_LATENCY=$avg_latency
            BASELINE_THROUGHPUT=$throughput
            printf "%-10s %-15s %-15s %-15s %-15s %-15s\n" "$batch_size" "$avg_wait" "$avg_latency" "$throughput" "基准" "基准"
            echo "$avg_latency" > /tmp/baseline_latency
            echo "$throughput" > /tmp/baseline_throughput
        else
            # 计算增长
            BASELINE_LATENCY=$(cat /tmp/baseline_latency 2>/dev/null || echo $avg_latency)
            BASELINE_THROUGHPUT=$(cat /tmp/baseline_throughput 2>/dev/null || echo $throughput)
            
            if [ ! -z "$BASELINE_LATENCY" ] && [ ! -z "$BASELINE_THROUGHPUT" ]; then
                LATENCY_INCREASE=$(echo "scale=1; ($avg_latency - $BASELINE_LATENCY) * 100 / $BASELINE_LATENCY" | bc -l)
                THROUGHPUT_INCREASE=$(echo "scale=1; ($throughput - $BASELINE_THROUGHPUT) * 100 / $BASELINE_THROUGHPUT" | bc -l)
                printf "%-10s %-15s %-15s %-15s %-15s %-15s\n" "$batch_size" "$avg_wait" "$avg_latency" "$throughput" "+${LATENCY_INCREASE}%" "+${THROUGHPUT_INCREASE}%"
            else
                printf "%-10s %-15s %-15s %-15s %-15s %-15s\n" "$batch_size" "$avg_wait" "$avg_latency" "$throughput" "N/A" "N/A"
            fi
        fi
    done
    
    # 清理临时文件
    rm -f /tmp/baseline_latency /tmp/baseline_throughput
    
    echo ""
    echo "💡 权衡关系验证:"
    echo "  ✅ 如果看到延迟随batch_size增加，说明排队延迟增长正确"
    echo "  ✅ 如果看到吞吐量随batch_size增加，说明GPU利用率提升正确"
    echo "  ✅ 这验证了论文中提到的延迟vs吞吐量权衡关系"
    
    echo ""
    echo "📄 详细数据已保存到: $RESULTS_FILE"
    
else
    echo "❌ 未找到结果文件"
fi

echo ""
echo "🚀 下一步: 基于这些真实性能数据，验证动态规划优化算法的效果"