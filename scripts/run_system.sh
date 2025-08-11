#!/bin/bash

set -e

# 配置参数
PROJECT_DIR="/workspace/infertuner-simple"
FLINK_HOME="/workspace/flink-setup/flink"
JAR_NAME="infertuner-simple-1.0.0.jar"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

echo "🚀 === InferTuner 简化运行脚本 ==="
echo ""

cd $PROJECT_DIR

# 1. 清理
print_info "步骤1: 清理环境..."

# 停止之前的Flink作业
if $FLINK_HOME/bin/flink list > /dev/null 2>&1; then
    RUNNING_JOBS=$($FLINK_HOME/bin/flink list 2>/dev/null | grep "RUNNING" | awk '{print $4}' | grep -v "Job" || true)
    if [ ! -z "$RUNNING_JOBS" ]; then
        echo "$RUNNING_JOBS" | while read job_id; do
            if [ ! -z "$job_id" ]; then
                print_info "停止作业: $job_id"
                $FLINK_HOME/bin/flink cancel $job_id || true
            fi
        done
        sleep 2
    fi
fi

# 清理Maven构建
mvn clean > /dev/null 2>&1 || true

print_success "清理完成"

# 2. 编译
print_info "步骤2: 编译项目..."

if mvn package -q; then
    print_success "编译成功: $(ls -lh target/$JAR_NAME | awk '{print $5}')"
else
    print_error "编译失败！"
    exit 1
fi

# 3. 启动Flink集群（如果未运行）
print_info "步骤3: 检查Flink集群..."

if ! $FLINK_HOME/bin/flink list > /dev/null 2>&1; then
    print_info "启动Flink集群..."
    $FLINK_HOME/bin/start-cluster.sh
    
    # 等待启动
    for i in {1..10}; do
        if $FLINK_HOME/bin/flink list > /dev/null 2>&1; then
            break
        fi
        print_info "等待Flink启动... ($i/10)"
        sleep 2
    done
fi

if ! $FLINK_HOME/bin/flink list > /dev/null 2>&1; then
    print_error "Flink集群启动失败！"
    exit 1
fi

print_success "Flink集群运行正常"

# 4. 运行作业
print_info "步骤4: 提交InferTuner作业..."

if $FLINK_HOME/bin/flink run target/$JAR_NAME; then
    print_success "作业提交成功！"
    echo ""
    print_info "📊 监控信息:"
    print_info "  Flink Web UI: http://localhost:8081"
    print_info "  查看作业状态: $FLINK_HOME/bin/flink list"
    print_info "  查看日志: tail -f $FLINK_HOME/log/flink-*-taskexecutor-*.log"
    echo ""
else
    print_error "作业提交失败！"
    exit 1
fi

print_success "=== InferTuner系统启动完成 ==="