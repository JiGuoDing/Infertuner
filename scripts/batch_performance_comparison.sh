#!/bin/bash

cd /workspace/infertuner-simple

FLINK_HOME="/workspace/flink-setup/flink"

echo "🎯 === 简化的真正攒批验证测试 ==="
echo "逻辑: 模拟攒批的等待时间效果"
echo ""

# 测试配置
declare -a SIMPLE_CONFIGS=(
    "1 6 200"   # batch_size=1, 6个请求, 200ms间隔 (无等待基准)
    "2 6 200"   # batch_size=2, 6个请求, 200ms间隔 
    "3 6 200"   # batch_size=3, 6个请求, 200ms间隔
)

echo "📋 测试配置:"
for config in "${SIMPLE_CONFIGS[@]}"; do
    read batch_size requests interval <<< "$config"
    echo "  batch_size=$batch_size: $requests个请求, ${interval}ms间隔"
    
    if [ $batch_size -eq 1 ]; then
        echo "    模拟: 无等待时间(基准)"
    else
        echo "    模拟: 第1个请求等待$((($batch_size-1)*200))ms, 第${batch_size}个请求等待0ms"
    fi
done
echo ""

# 停止现有Flink
$FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1 || true
sleep 2

for config in "${SIMPLE_CONFIGS[@]}"; do
    read batch_size requests interval <<< "$config"
    
    echo "🧪 测试 batch_size=$batch_size"
    
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
    
    $FLINK_HOME/bin/flink run -c com.infertuner.SimpleRealBatchJob \
        target/infertuner-simple-1.0.0.jar \
        $batch_size 1 $requests $interval >/dev/null 2>&1
    
    sleep 10
    
    echo "  📊 结果分析:"
    TASK_LOG=$(find $FLINK_HOME/log/ -name "*taskexecutor*.log" -type f | head -1)
    
    if [ -f "$TASK_LOG" ]; then
        
        echo ""
        echo "    === 等待时间分析 ==="
        WAIT_TIMES=$(grep "等待=" "$TASK_LOG" | grep -o "等待=[0-9]\+ms" | sed 's/等待=//g' | sed 's/ms//g')
        
        if [ ! -z "$WAIT_TIMES" ]; then
            echo "    各请求等待时间:"
            COUNT=0
            TOTAL_WAIT=0
            MAX_WAIT=0
            MIN_WAIT=999999
            
            while read -r wait_time; do
                COUNT=$((COUNT + 1))
                TOTAL_WAIT=$((TOTAL_WAIT + wait_time))
                
                if [ $wait_time -gt $MAX_WAIT ]; then
                    MAX_WAIT=$wait_time
                fi
                
                if [ $wait_time -lt $MIN_WAIT ]; then
                    MIN_WAIT=$wait_time
                fi
                
                echo "      请求$COUNT: ${wait_time}ms"
            done <<< "$WAIT_TIMES"
            
            if [ $COUNT -gt 0 ]; then
                AVG_WAIT=$((TOTAL_WAIT / COUNT))
                echo "    统计: 平均=${AVG_WAIT}ms, 最大=${MAX_WAIT}ms, 最小=${MIN_WAIT}ms"
                
                # 验证结果
                if [ $batch_size -eq 1 ]; then
                    if [ $MAX_WAIT -eq 0 ]; then
                        echo "    ✅ batch_size=1: 无等待时间，符合预期"
                    else
                        echo "    ⚠️ batch_size=1: 但有等待时间${MAX_WAIT}ms"
                    fi
                else
                    EXPECTED_MAX=$(( ($batch_size - 1) * 200 ))
                    if [ $MAX_WAIT -eq $EXPECTED_MAX ]; then
                        echo "    ✅ 最大等待时间${MAX_WAIT}ms符合预期${EXPECTED_MAX}ms"
                    else
                        echo "    ⚠️ 最大等待时间${MAX_WAIT}ms不符合预期${EXPECTED_MAX}ms"
                    fi
                    
                    if [ $MIN_WAIT -eq 0 ]; then
                        echo "    ✅ 最小等待时间${MIN_WAIT}ms符合预期"
                    else
                        echo "    ⚠️ 最小等待时间${MIN_WAIT}ms不符合预期"
                    fi
                fi
            fi
        else
            echo "    ❌ 未找到等待时间数据"
        fi
        
        echo ""
        echo "    === 总延迟对比 ==="
        TOTAL_LATENCIES=$(grep "总延迟=" "$TASK_LOG" | grep -o "总延迟=[0-9]\+ms" | sed 's/总延迟=//g' | sed 's/ms//g')
        
        if [ ! -z "$TOTAL_LATENCIES" ]; then
            TOTAL_SUM=0
            LATENCY_COUNT=0
            
            while read -r latency; do
                LATENCY_COUNT=$((LATENCY_COUNT + 1))
                TOTAL_SUM=$((TOTAL_SUM + latency))
            done <<< "$TOTAL_LATENCIES"
            
            if [ $LATENCY_COUNT -gt 0 ]; then
                AVG_TOTAL_LATENCY=$((TOTAL_SUM / LATENCY_COUNT))
                echo "    平均总延迟: ${AVG_TOTAL_LATENCY}ms"
                
                # 保存结果用于对比
                echo "$batch_size,$AVG_WAIT,$AVG_TOTAL_LATENCY" >> /tmp/batch_comparison.csv
            fi
        fi
        
    else
        echo "    ❌ 未找到日志文件"
    fi
    
    echo "  ✅ 完成"
    echo ""
    
    # 停止Flink准备下一轮
    $FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1 || true
    sleep 2
done

echo "🎯 === 攒批效果对比 ==="
echo ""

# 显示对比结果
if [ -f "/tmp/batch_comparison.csv" ]; then
    echo "📊 延迟对比结果:"
    printf "%-10s %-15s %-15s %-15s\n" "BatchSize" "平均等待(ms)" "平均总延迟(ms)" "延迟增长"
    printf "%-10s %-15s %-15s %-15s\n" "--------" "-------------" "-------------" "----------"
    
    BASELINE_LATENCY=""
    while IFS=, read batch_size avg_wait avg_total; do
        if [ $batch_size -eq 1 ]; then
            BASELINE_LATENCY=$avg_total
            printf "%-10s %-15s %-15s %-15s\n" "$batch_size" "$avg_wait" "$avg_total" "基准"
        else
            if [ ! -z "$BASELINE_LATENCY" ]; then
                INCREASE=$((avg_total - BASELINE_LATENCY))
                printf "%-10s %-15s %-15s %-15s\n" "$batch_size" "$avg_wait" "$avg_total" "+${INCREASE}ms"
            else
                printf "%-10s %-15s %-15s %-15s\n" "$batch_size" "$avg_wait" "$avg_total" "N/A"
            fi
        fi
    done < /tmp/batch_comparison.csv
    
    rm -f /tmp/batch_comparison.csv
fi

echo ""
echo "💡 验证结论:"
echo "  ✅ 如果看到等待时间随batch_size增加，说明攒批逻辑正确"
echo "  ✅ 如果总延迟 = 等待时间 + 推理时间，说明延迟计算正确"
echo "  ✅ 这样就模拟了真实攒批的延迟vs批大小权衡"
echo ""
echo "🚀 下一步: 进入完整的性能对比实验，验证不同batch_size的性能权衡"