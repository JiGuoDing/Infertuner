# InferTuner

基于Apache Flink的流式LLM推理性能验证系统，验证GPU扩展性、批处理权衡和缓存策略优化。

## 项目结构

```
/workspace/infertuner/
├── src/main/java/com/infertuner/
│   ├── jobs/                           # Flink作业主类
│   │   ├── GPUScalingJob.java         # GPU扩展性验证作业
│   │   ├── BatchAnalysisJob.java      # 批处理分析作业
│   │   └── CacheExperimentJob.java    # 缓存策略验证作业
│   ├── processors/                     # 数据处理器
│   │   ├── GPUInferenceProcessor.java # GPU推理处理器
│   │   ├── BatchProcessor.java        # 批处理器
│   │   └── CacheEnabledInferenceProcessor.java # 缓存推理处理器
│   ├── sources/                        # 数据源
│   │   ├── BasicRequestSource.java    # 基础请求数据源
│   │   ├── SimpleBatchingRequestSource.java # 批处理请求源
│   │   └── CacheAwareRequestSource.java # 缓存感知请求源
│   ├── sinks/                          # 数据汇
│   │   ├── GPUMonitorSink.java        # GPU监控数据汇
│   │   ├── BatchAnalysisSink.java     # 批处理分析数据汇
│   │   └── SimpleResultSink.java      # 简单结果输出
│   ├── models/                         # 数据模型
│   │   ├── InferenceRequest.java      # 推理请求模型
│   │   └── InferenceResponse.java     # 推理响应模型
│   ├── cache/                          # 缓存组件
│   │   ├── KVCacheEntry.java          # KV缓存条目
│   │   └── TwoLevelCacheManager.java  # 二级缓存管理器
│   └── optimization/                   # 优化算法
│       └── SimpleFrequencyCapture.java # 频率捕获器
├── scripts/                            # 验证脚本
│   ├── run_gpu_scaling.sh             # GPU扩展性验证脚本
│   ├── run_batch_analysis.sh          # 批处理权衡验证脚本
│   ├── run_cache_experiments.sh       # 缓存策略验证脚本
│   └── simple_inference_service.py    # Python推理服务
├── results/                            # 实验结果
│   ├── gpu_scaling/                   # GPU扩展性结果
│   ├── batch_analysis/                # 批处理分析结果
│   └── cache_experiments/             # 缓存实验结果
├── logs/                              # 实验日志
├── pom.xml                            # Maven配置
└── README.md
```

## 核心组件

### 作业类 (jobs/)

**GPUScalingJob.java**
- 验证多GPU并行扩展性
- 测试1、2、4 GPU配置下的吞吐量
- 计算扩展倍数和效率

**BatchAnalysisJob.java**
- 验证批处理大小对延迟和吞吐量的影响
- 测试不同batch_size (1, 2, 4, 8)
- 分析等待时间vs处理效率权衡

**CacheExperimentJob.java**
- 验证三种缓存策略的效果
- 支持STATIC、FLUID、FREQUENCY策略
- 测试命中率和延迟优化

### 处理器 (processors/)

**GPUInferenceProcessor.java**
- 分布式GPU推理处理
- 支持多GPU负载均衡
- 监控各GPU使用情况

**BatchProcessor.java**
- 批处理请求聚合
- 模拟等待时间和批处理延迟
- 计算吞吐量指标

**CacheEnabledInferenceProcessor.java**
- 集成二级缓存的推理处理器
- 三种缓存策略实现
- 缓存命中率统计和动态调整

### 缓存组件 (cache/)

**TwoLevelCacheManager.java**
- 本地缓存 + 远端缓存架构
- LRU淘汰策略
- 缓存统计和大小调整

**KVCacheEntry.java**
- KV缓存数据封装
- 访问时间和频率记录
- 缓存数据大小管理

### 数据源 (sources/)

**BasicRequestSource.java**
- 生成基础推理请求
- 可配置请求数量和间隔

**SimpleBatchingRequestSource.java**
- 生成批处理测试请求
- 支持不同batch_size配置

**CacheAwareRequestSource.java**
- 生成缓存感知的请求流
- 模拟不同用户访问模式
- 支持负载变化场景

## 验证脚本

### GPU扩展性验证
```bash
./scripts/run_gpu_scaling.sh
```

**功能：** 测试1、2、4 GPU配置的扩展性
**输出：** `results/gpu_scaling/gpu_scaling_YYYYMMDD_HHMMSS.csv`
**指标：** 并行度、吞吐量、扩展倍数、GPU分布

### 批处理权衡验证
```bash
./scripts/run_batch_analysis.sh
```

**功能：** 测试不同batch_size的延迟vs吞吐量权衡
**输出：** `results/batch_analysis/batch_analysis_YYYYMMDD_HHMMSS.csv`
**指标：** batch_size、等待时间、总延迟、吞吐量

### 缓存策略验证
```bash
./scripts/run_cache_experiments.sh
```

**功能：** 对比STATIC、FLUID、FREQUENCY三种缓存策略
**输出：** `results/cache_experiments/cache_experiments_YYYYMMDD_HHMMSS.csv`
**指标：** 策略、命中率、平均延迟、缓存大小变化

## 结果文件格式

### GPU扩展性结果
```csv
parallelism,total_throughput,scale_factor,gpu_distribution,execution_time
1,0.61,1.00,GPU0:50,82
2,1.33,2.18,GPU0:25;GPU1:25,78
4,1.84,3.02,GPU0:12;GPU1:13;GPU2:12;GPU3:13,64
```

### 批处理分析结果
```csv
batch_size,avg_wait_ms,avg_latency_ms,throughput_req_per_sec,total_requests,efficiency_notes
1,0,1123,0.89,30,基准配置
2,100,1204,1.66,30,平衡配置
4,300,1804,2.95,30,高吞吐配置
```

### 缓存策略结果
```csv
strategy,total_requests,cache_hits,hit_rate_percent,avg_latency_ms,strategy_config
STATIC,80,55,68.7,1067.6,固定大小=5
FLUID,80,56,70.0,1091.0,初始=5,窗口=3000ms
FREQUENCY,80,56,70.0,1023.3,初始=5,目标命中率=0.85
```

## 缓存策略配置

### STATIC策略
- 固定缓存大小，不动态调整
- 参数：`STATIC_CACHE_SIZE = 5`

### FLUID策略
- 基于请求速率动态调整缓存大小
- 参数：扩容阈值1.35，缩容阈值0.65，时间窗口3000ms

### FREQUENCY策略
- 基于访问频率分布计算最优缓存大小
- 参数：目标命中率0.9，频率bucket数500


## 环境要求

- Apache Flink 1.17+
- Python 3.8+ (PyTorch, Transformers)
- Java 11+, Maven 3.6+
- 多GPU环境 (推荐4×NVIDIA GPU)
- bc计算器

## 🚀 快速开始

### 环境部署

#### 1. 系统基础环境

```bash
# 更新系统
apt update && apt upgrade -y
apt install -y wget curl vim git build-essential bc

# 安装Java 11
apt install -y openjdk-11-jdk
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

#### 2. Python环境配置

```bash
# 安装Python 3.10
apt install -y python3.10 python3.10-dev python3-pip python3.10-venv

# 创建软链接
ln -sf /usr/bin/python3.10 /usr/bin/python
ln -sf /usr/bin/python3.10 /usr/bin/python3

# 安装深度学习依赖
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
pip install transformers>=4.35.0 accelerate datasets numpy pandas sentencepiece

# 验证GPU
python -c "import torch; print(f'CUDA可用: {torch.cuda.is_available()}')"
python -c "import torch; print(f'GPU数量: {torch.cuda.device_count()}')"
```

#### 3. Maven安装

```bash
cd /opt
wget https://archive.apache.org/dist/maven/maven-3/3.9.4/binaries/apache-maven-3.9.4-bin.tar.gz
tar xzf apache-maven-3.9.4-bin.tar.gz
ln -s apache-maven-3.9.4 maven

echo 'export M2_HOME=/opt/maven' >> ~/.bashrc
echo 'export PATH=$M2_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc

mvn -version
```

#### 4. Flink环境配置

```bash
cd /workspace
mkdir flink-setup && cd flink-setup

wget https://archive.apache.org/dist/flink/flink-1.17.0/flink-1.17.0-bin-scala_2.12.tgz
tar -xzf flink-1.17.0-bin-scala_2.12.tgz
mv flink-1.17.0 flink

echo 'export FLINK_HOME=/workspace/flink-setup/flink' >> ~/.bashrc
source ~/.bashrc
```
## 编译和运行

```bash
# 编译项目
mvn clean package

# 运行验证实验
./scripts/run_gpu_scaling.sh
./scripts/run_batch_analysis.sh  
./scripts/run_cache_experiments.sh

# 查看结果
ls results/*/
```