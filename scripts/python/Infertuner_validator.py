"""
DS2 vs InferTuner
DS2: 基于真实处理率的并行度优化 (OSDI'18)
InferTuner: 联合优化并行度和批大小的动态规划方法（使用真实性能数据代替）

使用方法:
python3 infertuner_validator.py ../data/performance_profiling/performance_matrix_20250817_131935.csv

"""

import os
import sys
import math
import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import Tuple, Optional, List, Dict
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error

@dataclass
class Config:
    """配置结果"""
    p: int  # 并行度（GPU数量）
    b: int  # 批大小
    cost: float  # 成本（GPU数量）
    predicted_latency: float
    predicted_throughput: float

class PerformanceModel:
    """性能预测模型"""

    def __init__(self, performance_data: pd.DataFrame):
        self.df = performance_data
        self.latency_model = None
        self.throughput_model = None
        self._build_models()

    def _build_models(self):
        """构建机器学习性能预测模型"""
        print("🧠 构建性能预测模型...")

        # 准备训练数据
        X = self.df[['p', 'b', 'target_rate']].values
        y_latency = self.df['avg_latency'].values
        y_throughput = self.df['actual_throughput'].values

        # 训练延迟预测模型
        self.latency_model = GradientBoostingRegressor(
            n_estimators=200, learning_rate=0.1, max_depth=6, random_state=42
        )
        self.latency_model.fit(X, y_latency)

        # 训练吞吐量预测模型
        self.throughput_model = GradientBoostingRegressor(
            n_estimators=200, learning_rate=0.1, max_depth=6, random_state=42
        )
        self.throughput_model.fit(X, y_throughput)

        # 评估模型精度
        X_train, X_test, y_lat_train, y_lat_test, y_thr_train, y_thr_test = train_test_split(
            X, y_latency, y_throughput, test_size=0.2, random_state=42
        )

        lat_pred = self.latency_model.predict(X_test)
        thr_pred = self.throughput_model.predict(X_test)

        lat_mae = mean_absolute_error(y_lat_test, lat_pred)
        lat_mape = mean_absolute_percentage_error(y_lat_test, lat_pred) * 100
        thr_mae = mean_absolute_error(y_thr_test, thr_pred)
        thr_mape = mean_absolute_percentage_error(y_thr_test, thr_pred) * 100

        print(f"   延迟模型: MAE={lat_mae:.1f}ms, MAPE={lat_mape:.1f}%")
        print(f"   吞吐量模型: MAE={thr_mae:.2f}req/s, MAPE={thr_mape:.1f}%")

    def predict(self, p: int, b: int, target_rate: float) -> Tuple[float, float]:
        """预测给定配置的性能"""
        X = np.array([[p, b, target_rate]])
        latency = self.latency_model.predict(X)[0]
        throughput = self.throughput_model.predict(X)[0]
        return latency, throughput

class DS2Algorithm:
    """
    DS2算法实现 (OSDI'18)
    核心思想：基于真实处理率优化并行度，批大小固定为1
    """

    def __init__(self, performance_data: pd.DataFrame):
        self.df = performance_data
        self.performance_model = PerformanceModel(performance_data)
        self._build_true_rate_model()

    def _build_true_rate_model(self):
        """构建DS2的真实处理率模型"""
        print("🔄 构建DS2真实处理率模型...")

        # DS2只使用batch_size=1的数据
        df_b1 = self.df[self.df['b'] == 1].copy()

        if len(df_b1) == 0:
            raise ValueError("DS2需要batch_size=1的性能数据")

        # 计算每个并行度的单实例真实处理率
        self.true_rate_per_instance = {}
        for p in df_b1['p'].unique():
            p_data = df_b1[df_b1['p'] == p]
            # DS2假设：在无backpressure时，实际吞吐量就是真实处理率
            avg_throughput = p_data['actual_throughput'].mean()
            # 单实例的实际吞吐量（真实处理率）
            single_instance_rate = avg_throughput / p
            self.true_rate_per_instance[p] = single_instance_rate
            print(f"   p={p}: 单实例真实处理率={single_instance_rate:.3f} req/s")

    def estimate_true_processing_rate(self, parallelism: int) -> float:
        """
        DS2核心：估算真实处理率
        假设线性扩展：total_rate = single_instance_rate * parallelism
        """

        # 如果是当前已知的并行度，直接返回线性扩展的真是处理率
        if parallelism in self.true_rate_per_instance:
            return self.true_rate_per_instance[parallelism] * parallelism

        # 否则，进行线性插值估算
        # 获取已知的并行度列表
        known_p = list(self.true_rate_per_instance.keys())
        # 如果当前还没有性能数据
        if not known_p:
            return 1.0

        # 方案1.线性插值
#         known_rates = [self.true_rate_per_instance[p] for p in known_p]
#         estimated_single_rate = np.interp(parallelism, known_p, known_rates)
#         return estimated_single_rate * parallelism

        # 方案2.使用最接近的配置
        closest_p = min(known_p, key=lambda x: abs(x - parallelism))
        single_rate = self.true_rate_per_instance[closest_p]
        return single_rate * parallelism

    def ds2_scaling_decision(self, target_rate: float, target_slo: float) -> Optional[Config]:
        """
        DS2核心算法：基于真实处理率计算最优并行度

        DS2公式：optimal_parallelism = target_rate / true_rate_per_instance
        约束：fixed batch_size = 1
        """
        print(f"\n🔄 DS2算法")
        print(f"   目标: {target_rate} req/s, SLO ≤ {target_slo}ms")
        print(f"   约束: 批大小固定 b=1")

        feasible_configs = []
        batch_size = 1  # DS2核心约束

        # DS2搜索空间：只能调整并行度
        available_p = sorted(self.true_rate_per_instance.keys())
        max_p = max(available_p) if available_p else 4

        for p in range(1, max_p + 1):
            # DS2核心：基于真实处理率判断
            true_processing_rate = self.estimate_true_processing_rate(p)

            # DS2约束检查
            throughput_ok = true_processing_rate >= target_rate * 0.95  # 5%容差

            if not throughput_ok:
                print(f"   ❌ p={p} 不满足吞吐量约束: 真实处理率={true_processing_rate:.2f}req/s < {target_rate * 0.95:.2f}req/s")
                continue

            # 使用性能模型估算延迟
            pred_latency, _ = self.performance_model.predict(p, batch_size, target_rate)
            latency_ok = pred_latency <= target_slo

            if not latency_ok:
                print(f"   ❌ p={p} 不满足延迟约束: 预测延迟={pred_latency:.0f}ms > {target_slo:.0f}ms")
                continue

            cost = p  # GPU数量
            config = Config(p, batch_size, cost, pred_latency, true_processing_rate)
            feasible_configs.append(config)
            print(f"   ✅ 可行: p={p}, b={batch_size}, 成本={cost}GPU, "
                  f"真实处理率={true_processing_rate:.2f}req/s, 延迟≈{pred_latency:.0f}ms")

        if not feasible_configs:
            print(f"   ❌ DS2无可行配置")
            return None

        # DS2选择最小并行度（最小成本）
        best_config = min(feasible_configs, key=lambda x: x.cost)
        print(f"   🏆 DS2最优: p={best_config.p}, b={best_config.b}, 成本={best_config.cost}GPU")

        return best_config

class InferTunerAlgorithm:
    """
    InferTuner算法实现
    核心思想：联合优化并行度和批大小，使用动态规划求解
    """

    def __init__(self, performance_data: pd.DataFrame):
        self.df = performance_data
        self.performance_model = PerformanceModel(performance_data)
        # 按配置分组取平均值
        self.df_avg = self.df.groupby(['p', 'b', 'target_rate']).agg({
            'actual_throughput': 'mean',
            'avg_latency': 'mean'
        }).reset_index()

    def generate_feasible_configs(self, target_rate: float, target_slo: float) -> List[Config]:
        """生成所有可行的(p,b)配置"""
        feasible_configs = []

        # InferTuner搜索空间：联合优化(p,b)
        p_values = sorted(self.df_avg['p'].unique())
        b_values = sorted(self.df_avg['b'].unique())

        for p in p_values:
            for b in b_values:
                # 使用性能模型预测
                pred_latency, pred_throughput = self.performance_model.predict(p, b, target_rate)

                # 约束检查
                throughput_ok = pred_throughput >= target_rate * 0.95  # 5%容差
                if not throughput_ok:
                    print(f"   ❌ p={p} 不满足吞吐量约束: 预测处理率={pred_throughput:.2f}req/s < {target_rate * 0.95:.2f}req/s")
                    continue

                latency_ok = pred_latency <= target_slo
                if not latency_ok:
                    print(f"   ❌ p={p} 不满足延迟约束: 预测延迟={pred_latency:.0f}ms > {target_slo:.0f}ms")
                    continue

                cost = p  # GPU数量作为成本
                config = Config(p, b, cost, pred_latency, pred_throughput)
                feasible_configs.append(config)

        return feasible_configs

    def infertuner_scaling_decision(self, target_rate: float, target_slo: float) -> Optional[Config]:
        """
        InferTuner核心算法：联合优化(p,b)
        动态规划：在可行配置中选择最小成本
        """
        print(f"\n🎯 InferTuner算法")
        print(f"   目标: {target_rate} req/s, SLO ≤ {target_slo}ms")
        print(f"   优势: 联合优化并行度(p)和批大小(b)")

        # 生成可行配置
        feasible_configs = self.generate_feasible_configs(target_rate, target_slo)

        if not feasible_configs:
            print(f"   ❌ InferTuner无可行配置")
            return None

        # 显示可行配置
        print(f"   搜索空间: p∈{sorted(self.df_avg['p'].unique())}, b∈{sorted(self.df_avg['b'].unique())}")
        for config in feasible_configs:
            print(f"   ✅ 可行: p={config.p}, b={config.b}, 成本={config.cost}GPU, "
                  f"吞吐量≈{config.predicted_throughput:.2f}req/s, 延迟≈{config.predicted_latency:.0f}ms")

        # InferTuner选择最小成本配置
        best_config = min(feasible_configs, key=lambda x: x.cost)
        print(f"   🏆 InferTuner最优: p={best_config.p}, b={best_config.b}, 成本={best_config.cost}GPU")

        return best_config

class AlgorithmComparator:
    """算法对比器"""

    def __init__(self, performance_data_file: str):
        # 加载数据
        self.df = pd.read_csv(performance_data_file)
        print(f"📊 加载性能数据: {len(self.df)} 条记录")

        # 数据清洗
        self.df = self.df[
            (self.df['actual_throughput'] > 0) &
            (self.df['avg_latency'] > 0) &
            (self.df['success_rate'] > 90)
        ].copy()
        print(f"   清洗后: {len(self.df)} 条有效记录")

        # 初始化算法
        self.ds2 = DS2Algorithm(self.df)
        self.infertuner = InferTunerAlgorithm(self.df)

        # 显示数据范围
        self._show_data_summary()

    def _show_data_summary(self):
        """显示数据概况"""
        print(f"\n📈 数据概况:")
        print(f"   并行度范围: {self.df['p'].min()}-{self.df['p'].max()}")
        print(f"   批大小范围: {self.df['b'].min()}-{self.df['b'].max()}")
        print(f"   请求速率范围: {self.df['target_rate'].min():.1f}-{self.df['target_rate'].max():.1f} req/s")
        print(f"   延迟范围: {self.df['avg_latency'].min():.0f}-{self.df['avg_latency'].max():.0f} ms")

    def generate_realistic_scenarios(self) -> List[Tuple[str, float, float]]:
        """基于真实数据生成测试场景"""
        min_latency = self.df['avg_latency'].min()

        scenarios = [
            ("低负载场景", 1.0, min_latency + 500),
            ("中负载场景", 2.0, min_latency + 800),
            ("高负载场景", 3.5, min_latency + 1200),
            ("严格SLO场景", 1.5, min_latency + 300),
        ]

        print(f"\n🎯 生成测试场景 (基于最低延迟{min_latency:.0f}ms):")
        for name, rate, slo in scenarios:
            print(f"   {name}: {rate}req/s, SLO≤{slo:.0f}ms")

        return scenarios

    def compare_scenario(self, scenario_name: str, target_rate: float, target_slo: float):
        """对比单个场景"""
        print(f"\n" + "="*70)
        print(f"📊 场景: {scenario_name}")
        print("="*70)

        # 运行两种算法
        ds2_result = self.ds2.ds2_scaling_decision(target_rate, target_slo)
        infertuner_result = self.infertuner.infertuner_scaling_decision(target_rate, target_slo)

        # 对比分析
        return self._analyze_comparison(ds2_result, infertuner_result, scenario_name)

    def _analyze_comparison(self, ds2_result: Optional[Config],
                          infertuner_result: Optional[Config],
                          scenario_name: str) -> Tuple[str, float]:
        """分析对比结果"""
        print(f"\n📋 算法对比:")

        if ds2_result and infertuner_result:
            print(f"DS2:       p={ds2_result.p}, b={ds2_result.b} → {ds2_result.cost}GPU")
            print(f"InferTuner: p={infertuner_result.p}, b={infertuner_result.b} → {infertuner_result.cost}GPU")

            if infertuner_result.cost < ds2_result.cost:
                savings = ds2_result.cost - infertuner_result.cost
                print(f"InferTuner: 节省{savings}GPU")
                return "InferTuner", savings
            elif infertuner_result.cost == ds2_result.cost:
                print(f"成本相同")
                return "Tie", 0
            else:
                print(f"DS2更优")
                return "DS2", 0

        elif infertuner_result and not ds2_result:
            print(f"只有InferTuner找到解: p={infertuner_result.p}, b={infertuner_result.b}")
            return "InferTuner", 0
        elif ds2_result and not infertuner_result:
            print(f"只有DS2找到解: p={ds2_result.p}, b={ds2_result.b}")
            return "DS2", 0
        else:
            print(f"都无解")
            return "None", 0

    def run_complete_comparison(self):
        """运行完整对比"""
        print(f"\n🚀 开始 DS2 vs InferTuner 完整对比")

        # 生成测试场景
        scenarios = self.generate_realistic_scenarios()

        # 执行对比
        results = []
        total_savings = 0

        for name, rate, slo in scenarios:
            winner, savings = self.compare_scenario(name, rate, slo)
            results.append((name, winner, savings))
            total_savings += savings

def main():
    """主函数"""
    if len(sys.argv) != 2:
        print("用法: python3 ds2_vs_infertuner.py <performance_data.csv>")
        print("例如: python3 ds2_vs_infertuner.py data/performance_profiling/performance_matrix_20250817_131935.csv")
        sys.exit(1)

    data_file = sys.argv[1]

    if not os.path.exists(data_file):
        print(f"❌ 数据文件不存在: {data_file}")
        sys.exit(1)

    print("🎯 DS2 vs InferTuner 论文方法完整实现与验证")
    print("="*60)

    try:
        # 创建对比器并运行验证
        comparator = AlgorithmComparator(data_file)
        comparator.run_complete_comparison()

    except Exception as e:
        print(f"❌ 运行错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()