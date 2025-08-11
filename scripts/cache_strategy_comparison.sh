#!/bin/bash

cd /workspace/infertuner-simple

FLINK_HOME="/workspace/flink-setup/flink"

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "🧪 === 缓存策略对比实验 ==="
echo ""
echo "实验场景："
echo "  🔍 测试策略: STATIC, FLUID, FREQUENCY"
echo ""

# =============================================================================
# 🔧 参数配置区域 - 在这里修改参数即可生效
# =============================================================================

# 实验基础参数
# REQUEST_COUNT=80
# BASE_INTERVAL=400

REQUEST_COUNT=100
BASE_INTERVAL=300

# 测试的策略（可以注释掉不需要的策略）
STRATEGIES=("STATIC" "FLUID" "FREQUENCY")

# STATIC策略参数
STATIC_CACHE_SIZE=5                    # 固定缓存大小

# FLUID策略参数
FLUID_INITIAL_SIZE=5                   # 初始缓存大小
FLUID_TIME_WINDOW=3000                  # 时间窗口(ms)：3秒
FLUID_EXPAND_THRESHOLD=1.35             # 扩容阈值：15%上升
FLUID_SHRINK_THRESHOLD=0.65             # 缩容阈值：15%下降
FLUID_EXPAND_FACTOR=1.15                 # 扩容因子：1.3倍
FLUID_SHRINK_FACTOR=1.08                 # 缩容因子：除以1.2
FLUID_ADJUSTMENT_INTERVAL=20             # 调整检查间隔：每20个请求
FLUID_MIN_SIZE=5                        # 最小缓存大小
FLUID_MAX_SIZE=60                       # 最大缓存大小

# FREQUENCY策略参数
FREQUENCY_INITIAL_SIZE=5              # 初始缓存大小
FREQUENCY_BUCKETS=500                   # 频率捕获bucket数量
FREQUENCY_TARGET_HIT_RATE=0.9          # 目标命中率
FREQUENCY_ADJUSTMENT_INTERVAL=20        # 调整检查间隔：每20个请求
FREQUENCY_MIN_SIZE=5                    # 最小缓存大小
FREQUENCY_MAX_SIZE=60                   # 最大缓存大小

# =============================================================================

echo "📋 策略参数配置:"
echo "  STATIC固定大小: ${STATIC_CACHE_SIZE}"
echo ""
echo "  FLUID初始大小: ${FLUID_INITIAL_SIZE}"
echo "  FLUID时间窗口: ${FLUID_TIME_WINDOW}ms"
echo "  FLUID扩容阈值: ${FLUID_EXPAND_THRESHOLD}倍"
echo "  FLUID缩容阈值: ${FLUID_SHRINK_THRESHOLD}倍"
echo "  FLUID扩容因子: ${FLUID_EXPAND_FACTOR}倍"
echo "  FLUID缩容因子: ${FLUID_SHRINK_FACTOR}倍"
echo "  FLUID调整间隔: 每${FLUID_ADJUSTMENT_INTERVAL}个请求"
echo "  FLUID大小范围: ${FLUID_MIN_SIZE} - ${FLUID_MAX_SIZE}"
echo ""
echo "  FREQUENCY初始大小: ${FREQUENCY_INITIAL_SIZE}"
echo "  FREQUENCY Bucket数: ${FREQUENCY_BUCKETS}"
echo "  FREQUENCY目标命中率: ${FREQUENCY_TARGET_HIT_RATE}"
echo "  FREQUENCY调整间隔: 每${FREQUENCY_ADJUSTMENT_INTERVAL}个请求"
echo "  FREQUENCY大小范围: ${FREQUENCY_MIN_SIZE} - ${FREQUENCY_MAX_SIZE}"
echo ""

# 创建日志目录（在脚本目录下）
LOGS_DIR="${SCRIPT_DIR}/logs"
mkdir -p "$LOGS_DIR"

# 结果文件（在脚本目录下）
RESULT_FILE="${SCRIPT_DIR}/cache_strategy_comparison_results.csv"
echo "策略,总请求,缓存命中,命中率(%),平均延迟(ms),策略配置" > "$RESULT_FILE"

for strategy in "${STRATEGIES[@]}"; do
    echo "🔧 测试缓存策略: $strategy"
    
    # 1. 重启Flink集群
    echo "🔄 重启Flink集群..."
    $FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1 || true
    sleep 2
    
    # 清空Flink日志
    find $FLINK_HOME/log/ -name "*.log" -exec truncate -s 0 {} \; 2>/dev/null || true
    
    # 启动Flink
    $FLINK_HOME/bin/start-cluster.sh >/dev/null 2>&1
    sleep 4
    
    # 2. 配置策略参数
    echo "🔧 配置策略: $strategy"
    
    # 设置缓存策略
    sed -i "s/private static final CacheStrategy CACHE_STRATEGY = CacheStrategy\.[A-Z]*;/private static final CacheStrategy CACHE_STRATEGY = CacheStrategy.$strategy;/" \
        src/main/java/com/infertuner/processor/CacheEnabledInferenceProcessor.java
    
    # 根据策略设置具体参数
    case $strategy in
        "STATIC")
            # STATIC策略参数
            sed -i "s/private static final int STATIC_CACHE_SIZE = [0-9]*;/private static final int STATIC_CACHE_SIZE = ${STATIC_CACHE_SIZE};/" \
                src/main/java/com/infertuner/processor/CacheEnabledInferenceProcessor.java
            STRATEGY_CONFIG="固定大小=${STATIC_CACHE_SIZE}"
            ;;
            
        "FLUID")
            # FLUID策略所有参数
            sed -i "s/private static final int INITIAL_CACHE_SIZE = [0-9]*;/private static final int INITIAL_CACHE_SIZE = ${FLUID_INITIAL_SIZE};/" \
                src/main/java/com/infertuner/processor/CacheEnabledInferenceProcessor.java
            
            sed -i "s/private static final long TIME_WINDOW_MS = [0-9]*L;/private static final long TIME_WINDOW_MS = ${FLUID_TIME_WINDOW}L;/" \
                src/main/java/com/infertuner/processor/CacheEnabledInferenceProcessor.java
            
            sed -i "s/private static final double EXPAND_THRESHOLD = [0-9.]*;/private static final double EXPAND_THRESHOLD = ${FLUID_EXPAND_THRESHOLD};/" \
                src/main/java/com/infertuner/processor/CacheEnabledInferenceProcessor.java
                
            sed -i "s/private static final double SHRINK_THRESHOLD = [0-9.]*;/private static final double SHRINK_THRESHOLD = ${FLUID_SHRINK_THRESHOLD};/" \
                src/main/java/com/infertuner/processor/CacheEnabledInferenceProcessor.java
                
            sed -i "s/private static final double EXPAND_FACTOR = [0-9.]*;/private static final double EXPAND_FACTOR = ${FLUID_EXPAND_FACTOR};/" \
                src/main/java/com/infertuner/processor/CacheEnabledInferenceProcessor.java
                
            sed -i "s/private static final double SHRINK_FACTOR = [0-9.]*;/private static final double SHRINK_FACTOR = ${FLUID_SHRINK_FACTOR};/" \
                src/main/java/com/infertuner/processor/CacheEnabledInferenceProcessor.java
            
            # FLUID策略专用参数
            sed -i "s/private static final int ADJUSTMENT_INTERVAL = [0-9]*;/private static final int ADJUSTMENT_INTERVAL = ${FLUID_ADJUSTMENT_INTERVAL};/" \
                src/main/java/com/infertuner/processor/CacheEnabledInferenceProcessor.java
            
            sed -i "s/private static final int MIN_CACHE_SIZE = [0-9]*;/private static final int MIN_CACHE_SIZE = ${FLUID_MIN_SIZE};/" \
                src/main/java/com/infertuner/processor/CacheEnabledInferenceProcessor.java
            
            sed -i "s/private static final int MAX_CACHE_SIZE = [0-9]*;/private static final int MAX_CACHE_SIZE = ${FLUID_MAX_SIZE};/" \
                src/main/java/com/infertuner/processor/CacheEnabledInferenceProcessor.java
            
            STRATEGY_CONFIG="初始=${FLUID_INITIAL_SIZE},窗口=${FLUID_TIME_WINDOW}ms,阈值${FLUID_EXPAND_THRESHOLD}/${FLUID_SHRINK_THRESHOLD},间隔=${FLUID_ADJUSTMENT_INTERVAL}"
            ;;
            
        "FREQUENCY")
            # FREQUENCY策略参数 - 注意：FREQUENCY策略有自己独立的参数
            sed -i "s/private static final int INITIAL_CACHE_SIZE = [0-9]*;/private static final int INITIAL_CACHE_SIZE = ${FREQUENCY_INITIAL_SIZE};/" \
                src/main/java/com/infertuner/processor/CacheEnabledInferenceProcessor.java
            
            sed -i "s/private static final int FREQUENCY_BUCKETS = [0-9]*;/private static final int FREQUENCY_BUCKETS = ${FREQUENCY_BUCKETS};/" \
                src/main/java/com/infertuner/processor/CacheEnabledInferenceProcessor.java
            
            sed -i "s/private static final double TARGET_HIT_RATE = [0-9.]*;/private static final double TARGET_HIT_RATE = ${FREQUENCY_TARGET_HIT_RATE};/" \
                src/main/java/com/infertuner/processor/CacheEnabledInferenceProcessor.java
            
            # FREQUENCY策略专用参数
            sed -i "s/private static final int ADJUSTMENT_INTERVAL = [0-9]*;/private static final int ADJUSTMENT_INTERVAL = ${FREQUENCY_ADJUSTMENT_INTERVAL};/" \
                src/main/java/com/infertuner/processor/CacheEnabledInferenceProcessor.java
            
            sed -i "s/private static final int MIN_CACHE_SIZE = [0-9]*;/private static final int MIN_CACHE_SIZE = ${FREQUENCY_MIN_SIZE};/" \
                src/main/java/com/infertuner/processor/CacheEnabledInferenceProcessor.java
            
            sed -i "s/private static final int MAX_CACHE_SIZE = [0-9]*;/private static final int MAX_CACHE_SIZE = ${FREQUENCY_MAX_SIZE};/" \
                src/main/java/com/infertuner/processor/CacheEnabledInferenceProcessor.java
            
            STRATEGY_CONFIG="初始=${FREQUENCY_INITIAL_SIZE},Bucket=${FREQUENCY_BUCKETS},目标命中率=${FREQUENCY_TARGET_HIT_RATE},间隔=${FREQUENCY_ADJUSTMENT_INTERVAL}"
            ;;
    esac
    
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
    sleep 3
    
    # 确保作业完成
    while $FLINK_HOME/bin/flink list 2>/dev/null | grep -q "RUNNING"; do
        echo "   作业仍在运行，继续等待..."
        sleep 3
    done
    
    # 6. 复制日志文件
    TASK_LOG_DEST="${LOGS_DIR}/strategy_${strategy}_taskexecutor.log"
    
    # 查找TaskExecutor日志文件
    TASK_LOG_SOURCE=$(find $FLINK_HOME/log/ -name "*taskexecutor*.log" -type f | head -1)
    
    if [ -f "$TASK_LOG_SOURCE" ]; then
        cp "$TASK_LOG_SOURCE" "$TASK_LOG_DEST"
        echo "📋 日志已复制: $(basename $TASK_LOG_SOURCE) → $TASK_LOG_DEST"
    else
        echo "❌ 未找到TaskExecutor日志文件"
        echo "🔍 可用日志文件:"
        ls -la $FLINK_HOME/log/*.log 2>/dev/null || echo "  无日志文件"
        continue
    fi
    
    # 7. 提取实验结果
    if [ -f "$TASK_LOG_DEST" ]; then
        echo "🔍 提取实验结果..."
        
        # 提取最终统计信息
        TOTAL_REQ=$(grep "总请求:" "$TASK_LOG_DEST" | tail -1 | sed 's/.*总请求: \([0-9]*\).*/\1/')
        HITS=$(grep "缓存命中:" "$TASK_LOG_DEST" | tail -1 | sed 's/.*缓存命中: \([0-9]*\).*/\1/')
        
        # 修复平均延迟提取
        AVG_LATENCY=$(grep "平均延迟:" "$TASK_LOG_DEST" | tail -1 | sed 's/.*平均延迟: \([0-9.]*\)ms.*/\1/')
        
        if [ ! -z "$TOTAL_REQ" ] && [ ! -z "$HITS" ] && [ ! -z "$AVG_LATENCY" ] && [ "$TOTAL_REQ" -gt 0 ]; then
            HIT_RATE=$(echo "scale=1; $HITS * 100 / $TOTAL_REQ" | bc -l 2>/dev/null || echo "0")
            
            echo "📊 结果: 总请求=$TOTAL_REQ, 命中=$HITS, 命中率=$HIT_RATE%, 平均延迟=${AVG_LATENCY}ms"
            
            # 保存到结果文件
            echo "$strategy,$TOTAL_REQ,$HITS,$HIT_RATE,$AVG_LATENCY,$STRATEGY_CONFIG" >> "$RESULT_FILE"
            
            # 显示策略特有的调整记录
            case $strategy in
                "FLUID")
                    echo "🌊 FLUID策略调整记录:"
                    FLUID_LOGS=$(grep -E "(FLUID扩容|FLUID缩容|FLUID保持)" "$TASK_LOG_DEST" | head -8)
                    if [ ! -z "$FLUID_LOGS" ]; then
                        echo "$FLUID_LOGS" | sed 's/.*INFO.*- /  /'
                        [ $(grep -c -E "(FLUID扩容|FLUID缩容)" "$TASK_LOG_DEST") -gt 8 ] && echo "  ..."
                    else
                        echo "  (未发现FLUID调整记录，可能参数需要调优)"
                    fi
                    ;;
                "FREQUENCY")
                    echo "📊 FREQUENCY策略调整记录:"
                    FREQ_LOGS=$(grep -E "(FREQUENCY计算|缓存大小调整)" "$TASK_LOG_DEST" | head -8)
                    if [ ! -z "$FREQ_LOGS" ]; then
                        echo "$FREQ_LOGS" | sed 's/.*INFO.*- /  /'
                        [ $(grep -c -E "FREQUENCY计算" "$TASK_LOG_DEST") -gt 8 ] && echo "  ..."
                    else
                        echo "  (未发现FREQUENCY调整记录，可能参数需要调优)"
                    fi
                    ;;
                "STATIC")
                    echo "🔒 STATIC策略执行记录:"
                    grep -E "(策略=STATIC|STATIC固定)" "$TASK_LOG_DEST" | head -3 | sed 's/.*INFO.*- /  /' || echo "  固定大小策略执行"
                    ;;
            esac
            
        else
            echo "❌ 未能提取有效统计数据"
            echo "$strategy,0,0,0,0,$STRATEGY_CONFIG" >> "$RESULT_FILE"
        fi
    else
        echo "❌ 日志文件不存在"
        echo "$strategy,0,0,0,0,$STRATEGY_CONFIG" >> "$RESULT_FILE"
    fi
    
    echo ""
    echo "=================================="
    echo ""
done

echo "🎉 所有策略实验完成！"
echo ""
echo "📈 === 策略对比结果 ==="
echo ""

# 美化输出结果表格
printf "%-12s %-8s %-8s %-12s %-15s %-50s\n" "策略" "总请求" "命中数" "命中率(%)" "平均延迟(ms)" "策略配置"
printf "%-12s %-8s %-8s %-12s %-15s %-50s\n" "----------" "------" "------" "--------" "------------" "--------------------------------------------------"

tail -n +2 "$RESULT_FILE" | while IFS=, read strategy total_req hits hit_rate avg_latency config; do
    printf "%-12s %-8s %-8s %-12s %-15s %-50s\n" "$strategy" "$total_req" "$hits" "$hit_rate" "$avg_latency" "$config"
done

echo ""
echo "🔍 === 策略分析建议 ==="
echo "📊 STATIC: 固定大小策略，性能稳定但无法适应负载变化"
echo "🌊 FLUID: 动态调整策略，根据请求速率自适应调整缓存大小"  
echo "📊 FREQUENCY: 频率分析策略，根据访问模式计算最优缓存大小"
echo ""

# 找出最佳策略
if [ $(wc -l < "$RESULT_FILE") -gt 1 ]; then
    echo "🏆 性能排名分析:"
    echo "按命中率排序:"
    tail -n +2 "$RESULT_FILE" | sort -t, -k4 -nr | head -1 | while IFS=, read strategy total_req hits hit_rate avg_latency config; do
        echo "  🥇 最高命中率: $strategy ($hit_rate%)"
    done

    echo "按延迟排序:"
    tail -n +2 "$RESULT_FILE" | sort -t, -k5 -n | head -1 | while IFS=, read strategy total_req hits hit_rate avg_latency config; do
        echo "  ⚡ 最低延迟: $strategy (${avg_latency}ms)"
    done
fi

echo ""
echo "📁 文件输出:"
echo "  详细结果: $RESULT_FILE"
echo "  日志目录: $LOGS_DIR"
echo "  日志文件: $LOGS_DIR/strategy_*_taskexecutor.log"
echo ""

# 停止Flink
echo "🛑 停止Flink集群..."
$FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1 || true

echo ""
echo "💡 参数调优建议:"
echo "🔧 FLUID策略调优:"
echo "  📏 时间窗口: 太小(如100ms)→速率不准确, 太大(如10s)→响应慢"
echo "  🎯 阈值设置: 降低→更敏感, 提高→更稳定"
echo "  📊 缓存范围: 根据最大用户数设置上限"
echo "  ⚡ 调整间隔: 5-10个请求为佳，太频繁会震荡"
echo ""
echo "🔧 FREQUENCY策略调优:"
echo "  📏 Bucket数量: 太少→估算不准, 太多→内存开销大"
echo "  🎯 目标命中率: 根据性能要求设置"
echo "  ⚡ 调整间隔: 需要足够数据才能准确估算"
echo ""