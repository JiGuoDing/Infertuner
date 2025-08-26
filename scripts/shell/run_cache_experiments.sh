#!/bin/bash

# InferTuner 缓存策略验证脚本
# 验证自适应缓存vs静态缓存的优势

set -e

cd /workspace/infertuner
FLINK_HOME="/workspace/flink-setup/flink"
SCRIPT_NAME="缓存策略验证"

echo "🧪 === ${SCRIPT_NAME} ==="
echo "验证目标: 自适应缓存策略优于静态缓存策略"
echo ""

# =============================================================================
# 📋 实验配置参数
# =============================================================================
CACHE_REQUEST_COUNT=80
CACHE_BASE_INTERVAL=300
CACHE_ENABLE_LOAD_VARIATION=true

# 测试策略列表
CACHE_STRATEGIES=("STATIC" "FLUID" "FREQUENCY")

# 策略参数配置
STATIC_CACHE_SIZE=5
FLUID_INITIAL_SIZE=5
FLUID_TIME_WINDOW=3000
FLUID_EXPAND_THRESHOLD=1.35
FLUID_SHRINK_THRESHOLD=0.65
FLUID_EXPAND_FACTOR=1.25
FLUID_SHRINK_FACTOR=1.15
FLUID_ADJUSTMENT_INTERVAL=15
FLUID_MIN_SIZE=5
FLUID_MAX_SIZE=50

FREQUENCY_INITIAL_SIZE=5
FREQUENCY_BUCKETS=200
FREQUENCY_TARGET_HIT_RATE=0.85
FREQUENCY_ADJUSTMENT_INTERVAL=15
FREQUENCY_MIN_SIZE=5
FREQUENCY_MAX_SIZE=50

echo "📋 实验配置:"
echo "  请求数量: ${CACHE_REQUEST_COUNT}"
echo "  基础间隔: ${CACHE_BASE_INTERVAL}ms"
echo "  负载变化: ${CACHE_ENABLE_LOAD_VARIATION}"
echo "  测试策略: ${CACHE_STRATEGIES[*]}"
echo ""

echo "🔧 策略参数:"
echo "  STATIC: 固定大小=${STATIC_CACHE_SIZE}"
echo "  FLUID: 初始=${FLUID_INITIAL_SIZE}, 扩容阈值=${FLUID_EXPAND_THRESHOLD}, 时间窗口=${FLUID_TIME_WINDOW}ms"
echo "  FREQUENCY: 初始=${FREQUENCY_INITIAL_SIZE}, 目标命中率=${FREQUENCY_TARGET_HIT_RATE}, Bucket数=${FREQUENCY_BUCKETS}"
echo ""

# 创建结果目录
RESULTS_DIR="results/cache_experiments"
LOGS_DIR="logs/cache_experiments"
mkdir -p "$RESULTS_DIR" "$LOGS_DIR"

# 结果文件
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_FILE="${RESULTS_DIR}/cache_experiments_${TIMESTAMP}.csv"
echo "strategy,total_requests,cache_hits,hit_rate_percent,avg_latency_ms,strategy_config" > "$RESULT_FILE"

for strategy in "${CACHE_STRATEGIES[@]}"; do
    echo "🔧 测试缓存策略: $strategy"
    
    # 1. 重启Flink集群
    echo "  🔄 重启Flink集群..."
    $FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1 || true
    sleep 2
    
    # 清空Flink日志
    find $FLINK_HOME/log/ -name "*.log" -exec truncate -s 0 {} \; 2>/dev/null || true
    
    # 启动Flink
    $FLINK_HOME/bin/start-cluster.sh >/dev/null 2>&1
    sleep 4
    
    # 2. 配置策略参数
    echo "  🔧 配置策略参数..."
    
    # 处理器文件路径
    PROCESSOR_FILE="src/main/java/com/infertuner/processors/CacheEnabledInferenceProcessor.java"
    
    # 设置缓存策略
    sed -i "s/private static final CacheStrategy CACHE_STRATEGY = CacheStrategy\.[A-Z]*;/private static final CacheStrategy CACHE_STRATEGY = CacheStrategy.$strategy;/" "$PROCESSOR_FILE"
    
    # 根据策略设置具体参数
    case $strategy in
        "STATIC")
            sed -i "s/private static final int STATIC_CACHE_SIZE = [0-9]*;/private static final int STATIC_CACHE_SIZE = ${STATIC_CACHE_SIZE};/" "$PROCESSOR_FILE"
            STRATEGY_CONFIG="固定大小=${STATIC_CACHE_SIZE}"
            ;;
            
        "FLUID")
            sed -i "s/private static final int INITIAL_CACHE_SIZE = [0-9]*;/private static final int INITIAL_CACHE_SIZE = ${FLUID_INITIAL_SIZE};/" "$PROCESSOR_FILE"
            sed -i "s/private static final long TIME_WINDOW_MS = [0-9]*L;/private static final long TIME_WINDOW_MS = ${FLUID_TIME_WINDOW}L;/" "$PROCESSOR_FILE"
            sed -i "s/private static final double EXPAND_THRESHOLD = [0-9.]*;/private static final double EXPAND_THRESHOLD = ${FLUID_EXPAND_THRESHOLD};/" "$PROCESSOR_FILE"
            sed -i "s/private static final double SHRINK_THRESHOLD = [0-9.]*;/private static final double SHRINK_THRESHOLD = ${FLUID_SHRINK_THRESHOLD};/" "$PROCESSOR_FILE"
            sed -i "s/private static final double EXPAND_FACTOR = [0-9.]*;/private static final double EXPAND_FACTOR = ${FLUID_EXPAND_FACTOR};/" "$PROCESSOR_FILE"
            sed -i "s/private static final double SHRINK_FACTOR = [0-9.]*;/private static final double SHRINK_FACTOR = ${FLUID_SHRINK_FACTOR};/" "$PROCESSOR_FILE"
            sed -i "s/private static final int ADJUSTMENT_INTERVAL = [0-9]*;/private static final int ADJUSTMENT_INTERVAL = ${FLUID_ADJUSTMENT_INTERVAL};/" "$PROCESSOR_FILE"
            sed -i "s/private static final int MIN_CACHE_SIZE = [0-9]*;/private static final int MIN_CACHE_SIZE = ${FLUID_MIN_SIZE};/" "$PROCESSOR_FILE"
            sed -i "s/private static final int MAX_CACHE_SIZE = [0-9]*;/private static final int MAX_CACHE_SIZE = ${FLUID_MAX_SIZE};/" "$PROCESSOR_FILE"
            
            STRATEGY_CONFIG="初始=${FLUID_INITIAL_SIZE},窗口=${FLUID_TIME_WINDOW}ms,阈值${FLUID_EXPAND_THRESHOLD}/${FLUID_SHRINK_THRESHOLD}"
            ;;
            
        "FREQUENCY")
            sed -i "s/private static final int INITIAL_CACHE_SIZE = [0-9]*;/private static final int INITIAL_CACHE_SIZE = ${FREQUENCY_INITIAL_SIZE};/" "$PROCESSOR_FILE"
            sed -i "s/private static final int FREQUENCY_BUCKETS = [0-9]*;/private static final int FREQUENCY_BUCKETS = ${FREQUENCY_BUCKETS};/" "$PROCESSOR_FILE"
            sed -i "s/private static final double TARGET_HIT_RATE = [0-9.]*;/private static final double TARGET_HIT_RATE = ${FREQUENCY_TARGET_HIT_RATE};/" "$PROCESSOR_FILE"
            sed -i "s/private static final int ADJUSTMENT_INTERVAL = [0-9]*;/private static final int ADJUSTMENT_INTERVAL = ${FREQUENCY_ADJUSTMENT_INTERVAL};/" "$PROCESSOR_FILE"
            sed -i "s/private static final int MIN_CACHE_SIZE = [0-9]*;/private static final int MIN_CACHE_SIZE = ${FREQUENCY_MIN_SIZE};/" "$PROCESSOR_FILE"
            sed -i "s/private static final int MAX_CACHE_SIZE = [0-9]*;/private static final int MAX_CACHE_SIZE = ${FREQUENCY_MAX_SIZE};/" "$PROCESSOR_FILE"
            
            STRATEGY_CONFIG="初始=${FREQUENCY_INITIAL_SIZE},目标命中率=${FREQUENCY_TARGET_HIT_RATE},Bucket=${FREQUENCY_BUCKETS}"
            ;;
    esac
    
    # 3. 重新编译
    echo "  🔨 编译项目..."
    mvn clean package -q > /dev/null 2>&1
    
    if [ $? -ne 0 ]; then
        echo "    ❌ 编译失败"
        continue
    fi
    
    # 4. 运行实验
    echo "  🚀 运行缓存实验..."
    
    $FLINK_HOME/bin/flink run -c com.infertuner.jobs.CacheExperimentJob \
        target/infertuner-1.0.0.jar \
        $CACHE_REQUEST_COUNT $CACHE_BASE_INTERVAL $CACHE_ENABLE_LOAD_VARIATION >/dev/null 2>&1
    
    # 5. 等待完成
    echo "  ⏳ 等待实验完成..."
    sleep 5
    
    # 确保作业完成
    WAIT_COUNT=0
    while $FLINK_HOME/bin/flink list 2>/dev/null | grep -q "RUNNING"; do
        sleep 3
        WAIT_COUNT=$((WAIT_COUNT + 1))
        if [ $WAIT_COUNT -gt 40 ]; then
            echo "    ⚠️ 超时，强制停止"
            $FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1 || true
            break
        fi
    done
    
    # 6. 复制日志文件
    LOG_FILE="${LOGS_DIR}/cache_${strategy}_${TIMESTAMP}.log"
    
    # 查找TaskExecutor日志文件
    TASK_LOG=$(find $FLINK_HOME/log/ -name "*taskexecutor*.log" -type f | head -1)
    
    if [ -f "$TASK_LOG" ]; then
        cp "$TASK_LOG" "$LOG_FILE"
        echo "    📋 日志已复制: $(basename $TASK_LOG) → $(basename $LOG_FILE)"
    else
        echo "    ❌ 未找到TaskExecutor日志文件"
        echo "    🔍 可用日志文件:"
        ls -la $FLINK_HOME/log/*.log 2>/dev/null || echo "      无日志文件"
        continue
    fi
    
    # 7. 提取实验结果
    echo "  🔍 分析实验结果..."
    
    if [ -f "$LOG_FILE" ]; then
        # 提取最终统计信息
        TOTAL_REQ=$(grep "总请求:" "$LOG_FILE" | tail -1 | grep -o "总请求: [0-9]*" | grep -o "[0-9]*")
        HITS=$(grep "缓存命中:" "$LOG_FILE" | tail -1 | grep -o "缓存命中: [0-9]*" | grep -o "[0-9]*")
        AVG_LATENCY=$(grep "平均延迟:" "$LOG_FILE" | tail -1 | grep -o "平均延迟: [0-9.]*ms" | grep -o "[0-9.]*")
        
        # 备用解析方法
        if [ -z "$TOTAL_REQ" ]; then
            TOTAL_REQ=$(grep "总请求" "$LOG_FILE" | tail -1 | grep -o "[0-9]\+" | head -1)
        fi
        
        if [ -z "$HITS" ]; then
            HITS=$(grep "命中" "$LOG_FILE" | tail -1 | grep -o "[0-9]\+" | head -1)
        fi
        
        if [ -z "$AVG_LATENCY" ]; then
            AVG_LATENCY=$(grep "延迟" "$LOG_FILE" | tail -1 | grep -o "[0-9.]\+ms" | sed 's/ms//g' | head -1)
        fi
        
        if [ ! -z "$TOTAL_REQ" ] && [ ! -z "$HITS" ] && [ ! -z "$AVG_LATENCY" ] && [ "$TOTAL_REQ" -gt 0 ]; then
            HIT_RATE=$(echo "scale=1; $HITS * 100 / $TOTAL_REQ" | bc -l 2>/dev/null || echo "0")
            
            echo "    ✅ $strategy 策略结果:"
            echo "      📊 总请求: ${TOTAL_REQ}个"
            echo "      🎯 缓存命中: ${HITS}个"
            echo "      📈 命中率: ${HIT_RATE}%"
            echo "      ⏱️  平均延迟: ${AVG_LATENCY}ms"
            
            # 保存到结果文件
            echo "$strategy,$TOTAL_REQ,$HITS,$HIT_RATE,$AVG_LATENCY,$STRATEGY_CONFIG" >> "$RESULT_FILE"
            
            # 显示策略特有的调整记录
            case $strategy in
                "FLUID")
                    echo "    🌊 FLUID策略调整记录:"
                    FLUID_LOGS=$(grep -E "(FLUID扩容|FLUID缩容|FLUID保持)" "$LOG_FILE" | head -5)
                    if [ ! -z "$FLUID_LOGS" ]; then
                        echo "$FLUID_LOGS" | sed 's/.*INFO.*- /      /'
                        [ $(grep -c -E "(FLUID扩容|FLUID缩容)" "$LOG_FILE") -gt 5 ] && echo "      ..."
                    else
                        echo "      (未发现FLUID调整记录)"
                    fi
                    ;;
                "FREQUENCY")
                    echo "    📊 FREQUENCY策略调整记录:"
                    FREQ_LOGS=$(grep -E "(FREQUENCY计算|缓存大小调整)" "$LOG_FILE" | head -5)
                    if [ ! -z "$FREQ_LOGS" ]; then
                        echo "$FREQ_LOGS" | sed 's/.*INFO.*- /      /'
                        [ $(grep -c -E "FREQUENCY计算" "$LOG_FILE") -gt 5 ] && echo "      ..."
                    else
                        echo "      (未发现FREQUENCY调整记录)"
                    fi
                    ;;
                "STATIC")
                    echo "    🔒 STATIC策略 (固定大小: ${STATIC_CACHE_SIZE})"
                    ;;
            esac
            
        else
            echo "    ❌ 未能提取有效统计数据"
            echo "    调试信息: total=$TOTAL_REQ, hits=$HITS, latency=$AVG_LATENCY"
            echo "$strategy,0,0,0,0,$STRATEGY_CONFIG" >> "$RESULT_FILE"
        fi
    else
        echo "    ❌ 日志文件不存在"
        echo "$strategy,0,0,0,0,$STRATEGY_CONFIG" >> "$RESULT_FILE"
    fi
    
    echo "  ✅ $strategy 策略测试完成"
    echo ""
done

echo "🎉 === 缓存策略验证完成 ==="
echo ""

# 显示对比结果
echo "📊 === 策略对比结果 ==="
printf "%-12s %-10s %-10s %-12s %-15s %-40s\n" "策略" "总请求" "命中数" "命中率(%)" "平均延迟(ms)" "策略配置"
printf "%-12s %-10s %-10s %-12s %-15s %-40s\n" "----------" "--------" "--------" "----------" "-------------" "----------------------------------------"

tail -n +2 "$RESULT_FILE" | while IFS=, read strategy total_req hits hit_rate avg_latency config; do
    printf "%-12s %-10s %-10s %-12s %-15s %-40s\n" "$strategy" "$total_req" "$hits" "$hit_rate" "$avg_latency" "$config"
done

echo ""

# 性能排名分析
if [ $(wc -l < "$RESULT_FILE") -gt 1 ]; then
    echo "🏆 === 性能排名分析 ==="
    
    echo "🎯 按命中率排序:"
    tail -n +2 "$RESULT_FILE" | sort -t, -k4 -nr | head -1 | while IFS=, read strategy total_req hits hit_rate avg_latency config; do
        echo "  🥇 最高命中率: $strategy (${hit_rate}%)"
    done

    echo "⚡ 按延迟排序:"
    tail -n +2 "$RESULT_FILE" | sort -t, -k5 -n | head -1 | while IFS=, read strategy total_req hits hit_rate avg_latency config; do
        echo "  🥇 最低延迟: $strategy (${avg_latency}ms)"
    done
    
    echo ""
    echo "💡 理论验证:"
    echo "  ✅ 自适应策略命中率应高于静态策略"
    echo "  ✅ 缓存命中减少延迟，提升用户体验"
    echo "  ✅ 负载变化时自适应策略优势更明显"
fi

echo ""
echo "📁 输出文件:"
echo "  📈 结果数据: $RESULT_FILE"
echo "  📋 详细日志: $LOGS_DIR/cache_*_${TIMESTAMP}.log"
echo ""

echo "💡 缓存策略调优建议:"
echo "🔧 FLUID策略: 调整时间窗口和阈值以适应负载变化"
echo "📊 FREQUENCY策略: 根据访问模式调整目标命中率"
echo "🔒 STATIC策略: 适用于负载稳定的场景"