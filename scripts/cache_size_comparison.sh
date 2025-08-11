#!/bin/bash

cd /workspace/infertuner-simple

FLINK_HOME="/workspace/flink-setup/flink"

echo "🧪 === 缓存大小对高负载影响实验 ==="
echo ""
echo "实验场景："
echo "  📈 负载变化: 5用户 → 15用户 → 30用户 → 50用户 → 8用户"
echo "  📊 请求总数: 80个"
echo "  🔍 测试缓存: 5, 15, 25, 40个条目"
echo ""

# 不同缓存大小
CACHE_SIZES=(5 15 25 40)
REQUEST_COUNT=80
BASE_INTERVAL=400

# 创建日志目录
mkdir -p logs

# 结果文件
RESULT_FILE="cache_size_comparison_results.txt"
echo "缓存大小,总请求,缓存命中,命中率(%),平均延迟(ms)" > $RESULT_FILE

for cache_size in "${CACHE_SIZES[@]}"; do
    echo "🔧 测试缓存大小: $cache_size 个条目"
    
    # 1. 重启Flink集群（确保干净环境）
    echo "🔄 重启Flink集群..."
    $FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1 || true
    sleep 3
    
    # 清空Flink日志
    find $FLINK_HOME/log/ -name "*.log" -exec truncate -s 0 {} \; 2>/dev/null || true
    
    # 启动Flink
    $FLINK_HOME/bin/start-cluster.sh >/dev/null 2>&1
    sleep 5
    
    # 2. 修改代码中的缓存大小
    sed -i "s/private static final int LOCAL_CACHE_SIZE = [0-9]*;/private static final int LOCAL_CACHE_SIZE = $cache_size;/" \
        src/main/java/com/infertuner/processor/CacheEnabledInferenceProcessor.java
    
    # 3. 重新编译
    echo "🔨 编译中..."
    mvn clean package -q > /dev/null 2>&1
    
    if [ $? -ne 0 ]; then
        echo "❌ 编译失败"
        continue
    fi
    
    # 4. 运行实验
    echo "🚀 运行实验 (请求数=$REQUEST_COUNT)..."
    
    $FLINK_HOME/bin/flink run target/infertuner-simple-1.0.0.jar \
        --class com.infertuner.CacheExperimentJob \
        $REQUEST_COUNT $BASE_INTERVAL true >/dev/null 2>&1
    
    # 5. 等待完成
    echo "⏳ 等待实验完成..."
    sleep 5
    
    # 确保作业真正完成
    while $FLINK_HOME/bin/flink list 2>/dev/null | grep -q "RUNNING"; do
        echo "   作业仍在运行，继续等待..."
        sleep 5
    done
    
    # 6. 复制日志文件
    TASK_LOG_DEST="logs/cache_${cache_size}_taskexecutor.log"
    
    # 查找TaskExecutor日志文件
    TASK_LOG_SOURCE=$(find $FLINK_HOME/log/ -name "*taskexecutor*gpu02.log" -type f | head -1)
    
    if [ -f "$TASK_LOG_SOURCE" ]; then
        cp "$TASK_LOG_SOURCE" "$TASK_LOG_DEST"
        echo "📋 日志已复制: $(basename $TASK_LOG_SOURCE) → $TASK_LOG_DEST"
    else
        echo "❌ 未找到TaskExecutor日志文件"
        echo "🔍 可用日志文件:"
        ls -la $FLINK_HOME/log/*.log 2>/dev/null || echo "  无日志文件"
        continue
    fi
    
    # 7. 提取最终结果
    if [ -f "$TASK_LOG_DEST" ]; then
        echo "🔍 提取实验结果..."
        
        # 提取最终统计信息
        TOTAL_REQ=$(grep "总请求:" "$TASK_LOG_DEST" | tail -1 | sed 's/.*总请求: \([0-9]*\).*/\1/')
        HITS=$(grep "缓存命中:" "$TASK_LOG_DEST" | tail -1 | sed 's/.*缓存命中: \([0-9]*\).*/\1/')
        AVG_LATENCY=$(grep "平均延迟:" "$TASK_LOG_DEST" | tail -1 | sed 's/.*平均延迟: \([0-9.]*\)ms.*/\1/')
        
        if [ ! -z "$TOTAL_REQ" ] && [ ! -z "$HITS" ] && [ ! -z "$AVG_LATENCY" ] && [ "$TOTAL_REQ" -gt 0 ]; then
            HIT_RATE=$(echo "scale=1; $HITS * 100 / $TOTAL_REQ" | bc -l 2>/dev/null || echo "0")
            
            echo "📊 结果: 总请求=$TOTAL_REQ, 命中=$HITS, 命中率=$HIT_RATE%, 平均延迟=${AVG_LATENCY}ms"
            
            # 保存到结果文件
            echo "$cache_size,$TOTAL_REQ,$HITS,$HIT_RATE,$AVG_LATENCY" >> $RESULT_FILE
            
            # 显示一些关键的命中/未命中日志
            echo "🔍 部分命中/未命中记录:"
            grep -E "(命中:|未命中:)" "$TASK_LOG_DEST" | head -5 | sed 's/.*INFO.*- /  /'
            echo "  ..."
            grep -E "(命中:|未命中:)" "$TASK_LOG_DEST" | tail -3 | sed 's/.*INFO.*- /  /'
            
        else
            echo "❌ 未能提取有效统计数据"
            echo "$cache_size,0,0,0,0" >> $RESULT_FILE
        fi
    else
        echo "❌ 日志文件不存在"
        echo "$cache_size,0,0,0,0" >> $RESULT_FILE
    fi
    
    echo ""
    echo "=================================="
    echo ""
done

echo "🎉 所有实验完成！"
echo ""
echo "📈 === 最终对比结果 ==="
echo ""

# 美化输出结果表格
printf "%-10s %-8s %-8s %-12s %-15s\n" "缓存大小" "总请求" "命中数" "命中率(%)" "平均延迟(ms)"
printf "%-10s %-8s %-8s %-12s %-15s\n" "--------" "------" "------" "--------" "------------"

tail -n +2 $RESULT_FILE | while IFS=, read cache_size total_req hits hit_rate avg_latency; do
    printf "%-10s %-8s %-8s %-12s %-15s\n" "$cache_size" "$total_req" "$hits" "$hit_rate" "$avg_latency"
done

echo ""


echo ""
echo "📁 文件输出:"
echo "  详细结果: $RESULT_FILE"
echo "  日志文件: logs/cache_*_taskexecutor.log"
echo ""

# 停止Flink
echo "🛑 停止Flink集群..."
$FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1 || true