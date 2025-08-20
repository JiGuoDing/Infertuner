#!/bin/bash

# InferTuner性能数据收集脚本
# 目标：在不同请求速率下测试所有(p,b)配置，构建完整的性能矩阵

set -e

cd /workspace/infertuner
FLINK_HOME="/workspace/flink-setup/flink"

# 参数配置
RUNS_PER_CONFIG=${1:-2}  # 每配置运行次数，默认2次

echo "🎯 === InferTuner性能数据收集 ==="
echo "每配置运行次数: ${RUNS_PER_CONFIG}"
echo "目标: 构建(p,b,rate) -> (latency,throughput)性能映射"
echo ""

# 实验参数矩阵
PARALLELISM_VALUES=(1 2 4)                                    # 并行度
BATCH_SIZE_VALUES=(1 2 4 8)                                   # 批大小
REQUEST_RATES=(0.5 1.0 1.5 2.0 2.5 3.0 4.0 5.0 6.0 8.0)     # 请求速率 req/s

# 固定实验参数
BATCHES_PER_GPU=15      # 每GPU处理批次数（确保足够数据）
MAX_RUNTIME=1200        # 最大运行时间（秒）

# 创建结果目录
RESULTS_DIR="data/performance_profiling"
LOGS_DIR="logs/performance_profiling"
mkdir -p "$RESULTS_DIR" "$LOGS_DIR"

# 结果文件
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_FILE="${RESULTS_DIR}/performance_matrix_${TIMESTAMP}.csv"
PROGRESS_FILE="${RESULTS_DIR}/progress_${TIMESTAMP}.txt"

# 生成实验配置列表并计算总数
EXPERIMENT_CONFIGS=()
for p in "${PARALLELISM_VALUES[@]}"; do
    for b in "${BATCH_SIZE_VALUES[@]}"; do
        for rate in "${REQUEST_RATES[@]}"; do
            # 过滤不合理的配置：避免严重过载
            max_reasonable_rate=$(echo "$p * 6.0" | bc -l)
            if (( $(echo "$rate <= $max_reasonable_rate" | bc -l) )); then
                EXPERIMENT_CONFIGS+=("${p},${b},${rate}")
            fi
        done
    done
done

TOTAL_EXPERIMENTS=$((${#EXPERIMENT_CONFIGS[@]} * RUNS_PER_CONFIG))
echo "📊 实验配置统计:"
echo "  并行度范围: ${PARALLELISM_VALUES[*]}"
echo "  批大小范围: ${BATCH_SIZE_VALUES[*]}"
echo "  请求速率范围: ${REQUEST_RATES[*]} req/s"
echo "  有效配置数: ${#EXPERIMENT_CONFIGS[@]}"
echo "  总实验次数: ${TOTAL_EXPERIMENTS}"
echo ""

# CSV表头
echo "p,b,target_rate,run,actual_throughput,avg_latency,avg_wait,success_rate,gpu_load_balance,experiment_id,timestamp" > "$RESULT_FILE"

# 初始化进度跟踪
echo "开始时间: $(date)" > "$PROGRESS_FILE"
echo "总实验数: ${TOTAL_EXPERIMENTS}" >> "$PROGRESS_FILE"
echo "" >> "$PROGRESS_FILE"

# 停止现有Flink
$FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1 || true
sleep 2

# 执行实验矩阵
EXPERIMENT_COUNT=0
SUCCESS_COUNT=0

echo "🚀 开始性能数据收集..."
echo ""

for config in "${EXPERIMENT_CONFIGS[@]}"; do
    IFS=',' read -r p b rate <<< "$config"

    # 计算请求间隔（毫秒）
    REQUEST_INTERVAL=$(echo "1000 / $rate" | bc -l | xargs printf "%.0f")
    TOTAL_REQUESTS=$((p * b * BATCHES_PER_GPU))

    echo "配置: p=${p}, b=${b}, rate=${rate}req/s (间隔=${REQUEST_INTERVAL}ms, 总请求=${TOTAL_REQUESTS})"

    for run in $(seq 1 $RUNS_PER_CONFIG); do
        EXPERIMENT_COUNT=$((EXPERIMENT_COUNT + 1))
        EXPERIMENT_ID="p${p}b${b}r${rate}_run${run}"

        echo "  [${EXPERIMENT_COUNT}/${TOTAL_EXPERIMENTS}] 运行 ${run}/${RUNS_PER_CONFIG} - ${EXPERIMENT_ID}"
        echo "进度: ${EXPERIMENT_COUNT}/${TOTAL_EXPERIMENTS} - ${EXPERIMENT_ID}" >> "$PROGRESS_FILE"

        # 重启Flink集群
        $FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1 || true
        sleep 3
        find $FLINK_HOME/log/ -name "*.log" -exec rm -f {} \; 2>/dev/null || true
        $FLINK_HOME/bin/start-cluster.sh >/dev/null 2>&1
        sleep 5

        # 编译（只在第一次运行时编译）
        if [ $EXPERIMENT_COUNT -eq 1 ]; then
            echo "    📦 编译项目..."
            mvn clean package -q > /dev/null 2>&1
            if [ $? -ne 0 ]; then
                echo "    ❌ 编译失败，终止实验"
                exit 1
            fi
            echo "    ✅ 编译完成"
        fi

        # 运行Flink实验
        START_TIME=$(date +%s)
        timeout ${MAX_RUNTIME}s $FLINK_HOME/bin/flink run -c com.infertuner.jobs.JointOptimizationJob \
            target/infertuner-1.0.0.jar $p $b $BATCHES_PER_GPU $REQUEST_INTERVAL >/dev/null 2>&1

        EXIT_CODE=$?

        # 等待作业完成
        sleep 5
        WAIT_COUNT=0
        while [ $WAIT_COUNT -lt 20 ]; do
            RUNNING_JOBS=$($FLINK_HOME/bin/flink list 2>/dev/null | grep "RUNNING" | wc -l)
            if [ "$RUNNING_JOBS" -eq 0 ]; then
                break
            fi
            sleep 3
            WAIT_COUNT=$((WAIT_COUNT + 1))
        done

        # 保存日志
        LOG_FILE="${LOGS_DIR}/${EXPERIMENT_ID}_${TIMESTAMP}.log"
        TASK_LOG=$(find $FLINK_HOME/log/ -name "*taskexecutor*.log" -type f | head -1)

        if [ -f "$TASK_LOG" ]; then
            cp "$TASK_LOG" "$LOG_FILE"

            # 提取性能指标
            FINAL_STATS_START=$(grep -n "p×b联合优化最终统计" "$LOG_FILE" | tail -1 | cut -d: -f1)

            if [ ! -z "$FINAL_STATS_START" ] && [ "$EXIT_CODE" -eq 0 ]; then
                STATS_SECTION=$(sed -n "${FINAL_STATS_START},\$p" "$LOG_FILE")

                # 提取关键指标
                THROUGHPUT=$(echo "$STATS_SECTION" | grep "吞吐量:" | tail -1 | sed -n 's/.*吞吐量: *\([0-9.]\+\) *req\/s.*/\1/p' | head -1)
                AVG_LATENCY=$(echo "$STATS_SECTION" | grep "平均延迟:" | tail -1 | sed -n 's/.*平均延迟: *\([0-9]\+\)ms.*/\1/p' | head -1)
                AVG_WAIT=$(echo "$STATS_SECTION" | grep "平均等待:" | tail -1 | sed -n 's/.*平均等待: *\([0-9]\+\)ms.*/\1/p' | head -1)
                SUCCESS_RATE=$(echo "$STATS_SECTION" | grep "成功率:" | tail -1 | sed -n 's/.*成功率: *\([0-9.]\+\)%.*/\1/p' | head -1)
                GPU_LOAD_BALANCE=$(echo "$STATS_SECTION" | grep "GPU负载均衡:" | tail -1 | sed -n 's/.*GPU负载均衡: *\([0-9.]\+\)%.*/\1/p' | head -1)

                # 验证数据有效性
                if [[ "$THROUGHPUT" =~ ^[0-9]+\.?[0-9]*$ ]] && [ ! -z "$AVG_LATENCY" ] && [ ! -z "$SUCCESS_RATE" ]; then
                    # 格式化数值
                    THROUGHPUT=$(printf "%.3f" "$THROUGHPUT" 2>/dev/null || echo "0.000")
                    AVG_LATENCY=$(printf "%.0f" "$AVG_LATENCY" 2>/dev/null || echo "0")
                    AVG_WAIT=$(printf "%.0f" "$AVG_WAIT" 2>/dev/null || echo "0")
                    SUCCESS_RATE=$(printf "%.2f" "$SUCCESS_RATE" 2>/dev/null || echo "0.00")
                    GPU_LOAD_BALANCE=$(printf "%.2f" "$GPU_LOAD_BALANCE" 2>/dev/null || echo "0.00")

                    # 保存到CSV
                    TIMESTAMP_ISO=$(date -Iseconds)
                    echo "$p,$b,$rate,$run,$THROUGHPUT,$AVG_LATENCY,$AVG_WAIT,$SUCCESS_RATE,$GPU_LOAD_BALANCE,$EXPERIMENT_ID,$TIMESTAMP_ISO" >> "$RESULT_FILE"

                    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
                    echo "    ✅ 成功: 吞吐量=${THROUGHPUT}req/s, 延迟=${AVG_LATENCY}ms, 成功率=${SUCCESS_RATE}%"
                    echo "成功: ${EXPERIMENT_ID} - 吞吐量=${THROUGHPUT}, 延迟=${AVG_LATENCY}ms" >> "$PROGRESS_FILE"
                else
                    echo "    ❌ 数据提取失败"
                    echo "失败: ${EXPERIMENT_ID} - 数据提取失败" >> "$PROGRESS_FILE"
                    echo "$p,$b,$rate,$run,0.000,0,0,0.00,0.00,$EXPERIMENT_ID,$TIMESTAMP_ISO" >> "$RESULT_FILE"
                fi
            else
                echo "    ❌ 实验失败或超时"
                echo "失败: ${EXPERIMENT_ID} - 实验超时或失败" >> "$PROGRESS_FILE"
                echo "$p,$b,$rate,$run,0.000,0,0,0.00,0.00,$EXPERIMENT_ID,$(date -Iseconds)" >> "$RESULT_FILE"
            fi
        else
            echo "    ❌ 未找到日志文件"
            echo "失败: ${EXPERIMENT_ID} - 日志缺失" >> "$PROGRESS_FILE"
            echo "$p,$b,$rate,$run,0.000,0,0,0.00,0.00,$EXPERIMENT_ID,$(date -Iseconds)" >> "$RESULT_FILE"
        fi

        # 清理
        $FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1 || true
        sleep 2

        # 显示实时进度
        PROGRESS_PCT=$(echo "scale=1; $EXPERIMENT_COUNT * 100 / $TOTAL_EXPERIMENTS" | bc -l)
        SUCCESS_RATE_PCT=$(echo "scale=1; $SUCCESS_COUNT * 100 / $EXPERIMENT_COUNT" | bc -l)
        echo "    📊 总进度: ${PROGRESS_PCT}% (${EXPERIMENT_COUNT}/${TOTAL_EXPERIMENTS}), 成功率: ${SUCCESS_RATE_PCT}%"
    done
    echo ""
done

# 停止Flink
$FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1 || true

# 生成实验总结
echo "" >> "$PROGRESS_FILE"
echo "结束时间: $(date)" >> "$PROGRESS_FILE"
echo "成功实验: ${SUCCESS_COUNT}/${TOTAL_EXPERIMENTS}" >> "$PROGRESS_FILE"

echo "✅ 性能数据收集完成"
echo ""
echo "📊 实验总结:"
echo "  总实验数: ${TOTAL_EXPERIMENTS}"
echo "  成功实验: ${SUCCESS_COUNT}"
echo "  成功率: $(echo "scale=1; $SUCCESS_COUNT * 100 / $TOTAL_EXPERIMENTS" | bc -l)%"
echo ""
echo "📁 输出文件:"
echo "  性能数据: $RESULT_FILE"
echo "  进度日志: $PROGRESS_FILE"
echo "  详细日志: $LOGS_DIR/"
echo ""
echo "🔍 数据预览:"
head -5 "$RESULT_FILE"
echo "..."
echo "总行数: $(wc -l < "$RESULT_FILE") (包含表头)"