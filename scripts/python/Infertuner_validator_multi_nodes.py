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
from loguru import logger
from collections import deque
from dataclasses import dataclass
from typing import Tuple, Optional, List
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF, ConstantKernel as C
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
        X = self.df[['parallelism', 'batch_size', 'target_rate']].values
        y_latency = self.df['avg_latency_ms'].values
        y_throughput = self.df['throughput_rps'].values

        # 训练延迟预测模型
        self.latency_model = GradientBoostingRegressor(
            n_estimators=200, learning_rate=0.01, max_depth=6, random_state=42
        )
        self.latency_model.fit(X, y_latency)

        # 训练吞吐量预测模型
        self.throughput_model = GradientBoostingRegressor(
            n_estimators=200, learning_rate=0.01, max_depth=6, random_state=42
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


def load_mapping(mapping_file="../data/submit_job_Falcon3-7B-Instruct_1000ms/parallelism_mapping.csv"):
    """
    获取映射表： 并行度 -> (吞吐量, 延迟)
    :param mapping_file:
    :return:
    """
    df = pd.read_csv(mapping_file)
    mapping = {row['parallelism']: (row['throughput_rps'], row['avg_latency_ms']) for _, row in df.iterrows()}
    return mapping


# 根据 parallelism 查询 throughput 和 latency
def get_perf_by_parallelism(parallelism, mapping):
    """在ContTune中使用的实际性能测量函数"""
    if parallelism in mapping:
        return mapping[parallelism]
    else:
        raise ValueError(f"Parallelism {parallelism} not found in mapping table")


class DS2Algorithm:
    """
    DS2算法实现 (OSDI'18)
    核心思想：基于真实处理率优化并行度，批大小固定为1
    """

    def __init__(self, performance_data: pd.DataFrame, max_parallelism: int):
        self.df_b1 = performance_data[performance_data["batch_size"] == 1].copy()
        self.performance_model = PerformanceModel(performance_data)
        self.max_parallelism = max_parallelism
        self._build_true_rate_model()

    def _build_true_rate_model(self):
        """构建DS2的真实处理率模型"""
        print("🔄 构建DS2真实处理率模型...")

        # DS2只使用batch_size=1的数据
        # df_b1 = self.df[self.df['batch_size'] == 1].copy()

        if len(self.df_b1) == 0:
            raise ValueError("DS2需要batch_size=1的性能数据")

        # 计算每个并行度的单实例真实处理率
        self.true_rate_per_instance = {}
        for p in self.df_b1['parallelism'].unique():
            p_data = self.df_b1[self.df_b1['parallelism'] == p]
            # DS2假设：在无backpressure时，实际吞吐量就是真实处理率
            avg_throughput = p_data['throughput_rps'].mean()
            # 单实例的实际吞吐量（真实处理率）
            single_instance_rate = avg_throughput / p
            self.true_rate_per_instance[p] = single_instance_rate
            print(f"p={p}: 单实例真实处理率={single_instance_rate:.3f} req/s")

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

        # 当前并行度不在已知范围内
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
        max_p = max(available_p) if available_p else self.max_parallelism

        for p in range(1, max_p + 1):
            # DS2核心：基于真实处理率判断
            true_processing_rate = self.estimate_true_processing_rate(p)

            # DS2约束检查
            throughput_ok = true_processing_rate >= target_rate * 0.95  # 5%容差

            if not throughput_ok:
                print(
                    f"   ❌ p={p} 不满足吞吐量约束: 真实处理率={true_processing_rate:.2f}req/s < {target_rate * 0.95:.2f}req/s")
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


class GaussianProcessModel:
    """
    高斯过程回归模型，用于 ContTune 算法中配置与性能的映射; 同时预测吞吐量和延迟
    """

    def __init__(self, performance_data, parallelism_search_space: np.ndarray):
        """
        初始化并自动训练 GP 模型
        :param performance_data: 性能采集数据
        """
        self.df = performance_data
        self.parallelism_search_space = parallelism_search_space
        self.scaler_X = StandardScaler()
        self.update(new_data=performance_data)

    def update(self, new_data: pd.DataFrame, prewarm: bool = False):
        """
        增量更新数据并重新训练 GP 模型
        :param new_data: 新增数据 DataFrame
        :param prewarm: 是否添加预热样本（min/max parallelism）
        """
        # 数据清洗
        new_data = new_data.dropna()

        # 添加预热样本
        if prewarm and len(new_data) > 0:
            min_p = new_data["parallelism"].min()
            max_p = new_data["parallelism"].max()
            prewarm_points = []
            for p in [min_p, max_p]:
                row = new_data[new_data["parallelism"] == p]
                if not row.empty:
                    prewarm_points.append(row.iloc[0])
            if prewarm_points:
                prewarm_df = pd.DataFrame(prewarm_points)
                new_data = pd.concat([new_data, prewarm_df], ignore_index=True)

        # 初始化或增量更新训练数据
        X_new = new_data[["parallelism"]].values
        y_throughput_new = new_data[["throughput_rps"]].values
        y_latency_new = new_data[["avg_latency_ms"]].values

        if not hasattr(self, "X_train"):
            self.X_train = X_new
            self.y_train_throughput = y_throughput_new
            self.y_train_latency = y_latency_new
        else:
            self.X_train = np.vstack((self.X_train, X_new))
            self.y_train_throughput = np.vstack((self.y_train_throughput, y_throughput_new))
            self.y_train_latency = np.vstack((self.y_train_latency, y_latency_new))

        # 标准化 X
        self.X_train_scaled = self.scaler_X.fit_transform(self.X_train)

        # 定义核函数
        kernel = C(1.0, (1e-2, 1e2)) * RBF(1.0, (1e-4, 1e4))

        # GP 模型: 吞吐量
        self.gp_throughput = GaussianProcessRegressor(
            kernel=kernel,
            n_restarts_optimizer=25,
            alpha=1e-2,
            normalize_y=True
        )
        self.gp_throughput.fit(self.X_train_scaled, self.y_train_throughput)

        # GP 模型: 延迟
        self.gp_latency = GaussianProcessRegressor(
            kernel=kernel,
            n_restarts_optimizer=25,
            alpha=1e-2,
            normalize_y=True
        )
        self.gp_latency.fit(self.X_train_scaled, self.y_train_latency)

    def predict(self, x):
        """
        预测吞吐量和延迟
        :param x:
        :return:
        """
        X = np.array(x).reshape(-1, 1)
        throughput_mean, throughput_std = self.gp_throughput.predict(X, return_std=True)
        latency_mean, latency_std = self.gp_latency.predict(X, return_std=True)

        return throughput_mean, throughput_std, latency_mean, latency_std

    def suggest_next_parallelism(self, kappa=1.96):
        """
        使用 Upper Confidence Bound (UCB) 策略选择下一个并行度，综合考虑吞吐量和延迟
        :param kappa: UCB 探索-利用权衡参数
        :return: 推荐的并行度
        """
        # 预测搜索空间中的吞吐量和延迟
        X = np.array(self.parallelism_search_space).reshape(-1, 1)
        throughput_mean, throughput_std = self.gp_throughput.predict(X, return_std=True)
        latency_mean, latency_std = self.gp_latency.predict(X, return_std=True)

        # 标准化吞吐量和延迟以便比较（因为吞吐量和延迟的量纲不同）
        throughput_mean_norm = (throughput_mean - throughput_mean.mean()) / throughput_mean.std()
        throughput_std_norm = throughput_std / throughput_mean.std()
        latency_mean_norm = (latency_mean - latency_mean.mean()) / latency_mean.std()
        latency_std_norm = latency_std / latency_mean.std()

        # 计算 UCB 分数，吞吐量最大化（正向），延迟最小化（负向）
        throughput_ucb = throughput_mean_norm + kappa * throughput_std_norm
        latency_ucb = -latency_mean_norm + kappa * latency_std_norm  # 负号表示延迟越小越好
        combined_ucb = throughput_ucb + latency_ucb

        # 选择 UCB 分数最高的并行度
        best_index = np.argmax(combined_ucb)
        return self.parallelism_search_space[best_index]


class ContTuneAlgorithm:
    """
    ContTune算法实现
    核心思想：Big-Small 算法 + CBO
    只涉及并行度调整，批大小固定为1
    """

    # TODO 实现 ContTune 算法
    def __init__(self,
                 measure_fn,
                 target_throughput,
                 slo,
                 performance_data,
                 max_parallelism: int = 19,
                 min_parallelism: int = 1,
                 big_multiplier: int = 2,
                 small_max_iters: int = 3,
                 history_max_len: int = 10):
        """
        初始化 ContTune 算法
        :param target_throughput: 目标吞吐量
        :param measure_fn: 性能测量函数，输入并行度，输出真实吞吐量和延迟
        :param performance_data: 性能采集数据
        :param max_parallelism: 最大并行度
        :param min_parallelism: 最小并行度
        :param big_multiplier: Big Phase 的并行度放大系数
        :param small_max_iters: Small Phase 的最大迭代次数
        """
        self.df_b1 = performance_data[performance_data["batch_size"] == 1].copy()
        self.target_throughput = target_throughput
        self.slo = slo
        self.measure_fn = measure_fn
        # 设置运行时参数
        self.parallelism_search_space = np.arange(min_parallelism, max_parallelism + 1)
        self.min_parallelism = min_parallelism
        self.max_parallelism = max_parallelism
        self.big_multiplier = big_multiplier
        self.small_max_iters = small_max_iters

        self.performance_model = PerformanceModel(performance_data=performance_data)

        # 初始化历史数据，只维护 history_max_len 条记录
        self.history = deque(maxlen=history_max_len)

        # 创建 GP 模型
        self.gp = GaussianProcessModel(
            performance_data=self.df_b1,
            parallelism_search_space=self.parallelism_search_space
        )

        # 加载真实性能映射表
        self.mapping = load_mapping()

    def big_phase(self, start_parallelism):
        """
        Big Phase: 放大并行度，直到吞吐量和延迟同时满足 SLA
        :param start_parallelism: 起始并行度
        :return: (最终并行度, 吞吐量, 延迟)
        """
        current_parallelism = start_parallelism
        current_throughput, current_latency = self.measure_fn(current_parallelism)
        self.history.append((current_parallelism, current_throughput, current_latency))

        logger.info(
            f"[BIG] start with p={current_parallelism}, throughput={current_throughput:.2f} req/s, latency={current_latency:.2f} ms")

        iter_count = 0
        while current_throughput < self.target_throughput or current_latency > self.slo:
            iter_count += 1
            max_history_parallelism = max(p for p, _, _ in self.history) if self.history else current_parallelism

            # 放大并行度
            if current_parallelism >= max_history_parallelism:
                current_parallelism = min(
                    max(math.ceil(max_history_parallelism * self.big_multiplier), max_history_parallelism + 1),
                    self.max_parallelism)
            else:
                current_parallelism = max_history_parallelism

            current_throughput, current_latency = self.measure_fn(current_parallelism)
            self.history.append((current_parallelism, current_throughput, current_latency))

            # 记录 SLA 状态
            if current_latency > self.slo:
                reason = f"latency {current_latency:.2f} > threshold {self.slo}"
                logger.warning(f"[BIG] SLA warning at p={current_parallelism} ({reason})")
            if current_throughput < self.target_throughput:
                reason = f"throughput {current_throughput:.2f} < target {self.target_throughput}"
                logger.warning(f"[BIG] SLA warning at p={current_parallelism} ({reason})")

            logger.info(
                f"[BIG] iter {iter_count}: p={current_parallelism}, throughput={current_throughput:.2f} req/s, latency={current_latency:.2f} ms")

            if current_parallelism >= self.max_parallelism:
                logger.warning("[BIG] reached maximum parallelism, stopping Big Phase")
                break

        # 最终检查 SLA
        if current_throughput < self.target_throughput or current_latency > self.slo:
            reason = []
            if current_throughput < self.target_throughput:
                reason.append(f"throughput {current_throughput:.2f} < target {self.target_throughput}")
            if current_latency > self.slo:
                reason.append(f"latency {current_latency:.2f} > threshold {self.slo}")
            logger.warning(f"[BIG] final Big Phase SLA check: {', '.join(reason)}")

        return current_parallelism, current_throughput, current_latency

    def small_phase(self, start_parallelism):
        """
        Small Phase: 在 Big Phase 的结果基础上，尝试减少并行度以找到最小的 SLA 满足点。

        SLA: throughput >= target_throughput AND latency <= latency_threshold

        :param start_parallelism: Big Phase 结束时的并行度
        :return: (最佳并行度, 吞吐量, 延迟)
        """

        current_parallelism = start_parallelism
        tested_points = {current_parallelism}

        # 记录 Big Phase 起点
        throughput, latency = self.measure_fn(current_parallelism)
        self.history.append((current_parallelism, throughput, latency))
        logger.info(f"[SMALL] start from p={current_parallelism}: throughput={throughput:.2f}, latency={latency:.2f}")

        # 如果 Big Phase 的起点本身不满足 SLA，直接返回
        if not self._meet_sla(throughput, latency):
            reason = self._sla_violation_reason(throughput, latency)
            logger.warning(f"[SMALL] starting point does NOT meet SLA ({reason}), cannot reduce parallelism further.")
            return current_parallelism, throughput, latency

        # 如果满足 SLA，则尝试减少并行度
        for it in range(self.small_max_iters):
            logger.info(f"[SMALL] iteration {it + 1}")
            next_parallelism = self.gp.suggest_next_parallelism()

            # 必须保证候选并行度 < 当前并行度（往下调）
            if next_parallelism >= current_parallelism or next_parallelism in tested_points:
                logger.info("No smaller parallelism suggested, stopping Small Phase.")
                break

            # 实测性能
            throughput, latency = self.measure_fn(next_parallelism)
            logger.info(f"[SMALL] test p={next_parallelism}: throughput={throughput:.2f}, latency={latency:.2f}")

            # 更新 GP 和历史
            new_data = pd.DataFrame([[next_parallelism, throughput, latency]],
                                    columns=["parallelism", "throughput_rps", "avg_latency_ms"])
            self.gp.update(new_data)
            self.history.append((next_parallelism, throughput, latency))
            tested_points.add(next_parallelism)

            # 如果 SLA 仍满足，则更新 current_parallelism（继续往下调）
            if self._meet_sla(throughput, latency):
                logger.info(f"[SMALL] SLA still satisfied at p={next_parallelism}. Continue reducing.")
                current_parallelism = next_parallelism
            else:
                reason = self._sla_violation_reason(throughput, latency)
                logger.info(f"[SMALL] SLA violated at p={next_parallelism} ({reason}). Stop.")
                break

        # 返回最小并行度（SLA 满足）
        best_point = self._select_min_parallelism_sla()
        logger.info(f"[SMALL] final minimal SLA point: p={best_point[0]}, "
                    f"throughput={best_point[1]:.2f}, latency={best_point[2]:.2f}")
        return best_point

    def _meet_sla(self, throughput, latency):
        return throughput >= self.target_throughput and latency <= self.slo

    def _sla_violation_reason(self, throughput, latency):
        reasons = []
        if throughput < self.target_throughput:
            reasons.append(f"throughput {throughput:.2f} < target {self.target_throughput}")
        if latency > self.slo:
            reasons.append(f"latency {latency:.2f} > threshold {self.slo}")
        return " and ".join(reasons)

    def _select_min_parallelism_sla(self):
        # 筛选满足 SLA 的点
        sla_points = [p for p in self.history if self._meet_sla(p[1], p[2])]
        if sla_points:
            # 按并行度升序选择最小的
            return min(sla_points, key=lambda x: x[0])
        else:
            # 如果没有任何点满足 SLA，退而求其次选择吞吐量最高的
            return max(self.history, key=lambda x: x[1])

    def conttune_scaling_decision(self, start_parallelism: int):
        """
        执行 ContTune 调节逻辑：先 Big Phase 放大并行度，再 Small Phase 精调到最小满足 SLA 的并行度
        :param start_parallelism: 初始并行度
        :return: 并行度、吞吐量、延迟
        """
        logger.info("[ContTune] === Start Scaling Decision ===")

        # 1. Big Phase
        big_p, big_thr, big_lat = self.big_phase(start_parallelism=start_parallelism)
        logger.info(f"[ContTune] Big Phase result: p={big_p}, throughput={big_thr:.2f}, latency={big_lat:.2f}")

        # 检查 Big Phase SLA
        if not self._meet_sla(big_thr, big_lat):
            reason = self._sla_violation_reason(big_thr, big_lat)
            logger.warning(f"[ContTune] Big Phase SLA 未达要求: {reason}")
            # 即使未达 SLA，也可以尝试返回当前最大并行度作为配置

            final_config = Config(p=big_p, b=1, cost=big_p, predicted_latency=big_lat, predicted_throughput=big_thr)
            return final_config

        # 2. Small Phase
        best_p, best_thr, best_lat = self.small_phase(start_parallelism=big_p)
        logger.info(f"[ContTune] Small Phase result: p={best_p}, throughput={best_thr:.2f}, latency={best_lat:.2f}")

        # 检查最终 SLA
        sla_met = self._meet_sla(best_thr, best_lat)
        reason = None if sla_met else self._sla_violation_reason(best_thr, best_lat)

        final_config = Config(p=best_p, b=1, cost=best_p, predicted_throughput=best_thr, predicted_latency=best_lat)
        if reason:
            logger.warning(f"[ContTune] Big Phase SLA 未达要求: {reason}")

        logger.info(f"[ContTune] Final scaling decision: {final_config}")
        return final_config


class InferTunerAlgorithm:
    """
    InferTuner算法实现
    核心思想：联合优化并行度和批大小，使用动态规划求解
    """

    def __init__(self, performance_data: pd.DataFrame):
        self.df = performance_data
        self.performance_model = PerformanceModel(performance_data)
        # 按配置分组取平均值
        self.df_avg = self.df.groupby(['parallelism', 'batch_size', 'target_rate']).agg({
            'throughput_rps': 'mean',
            'avg_latency_ms': 'mean'
        }).reset_index()

    def generate_feasible_configs(self, target_rate: float, target_slo: float) -> List[Config]:
        """生成所有可行的(p,b)配置"""
        feasible_configs = []

        # InferTuner搜索空间：联合优化(p,b)
        p_values = sorted(self.df_avg['parallelism'].unique())
        b_values = sorted(self.df_avg['batch_size'].unique())

        for p in p_values:
            for b in b_values:
                # 使用性能模型预测
                pred_latency, pred_throughput = self.performance_model.predict(p, b, target_rate)

                # 约束检查
                throughput_ok = pred_throughput >= target_rate * 0.95  # 5%容差
                if not throughput_ok:
                    print(
                        f"   ❌ p={p}, b={b} 不满足吞吐量约束: 预测处理率={pred_throughput:.2f}req/s < {target_rate * 0.95:.2f}req/s")
                    continue

                latency_ok = pred_latency <= target_slo
                if not latency_ok:
                    print(f"   ❌ p={p}, b={b} 不满足延迟约束: 预测延迟={pred_latency:.0f}ms > {target_slo:.0f}ms")
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
        print(
            f"   搜索空间: p∈{sorted(self.df_avg['parallelism'].unique())}, b∈{sorted(self.df_avg['batch_size'].unique())}")
        for config in feasible_configs:
            print(f"   ✅ 可行: p={config.p}, b={config.b}, 成本={config.cost}GPU, "
                  f"吞吐量≈{config.predicted_throughput:.2f}req/s, 延迟≈{config.predicted_latency:.0f}ms")

        # InferTuner选择最小成本配置
        best_config = min(feasible_configs, key=lambda x: x.cost)
        print(f"   🏆 InferTuner最优: p={best_config.p}, b={best_config.b}, 成本={best_config.cost}GPU")

        return best_config


class AlgorithmComparator:
    """算法对比器"""

    def __init__(self, performance_data_file: str, max_parallelism: int = 19):
        # 加载数据
        self.df = pd.read_csv(performance_data_file)
        print(f"📊 加载性能数据: {len(self.df)} 条记录")

        # 数据清洗
        self.df = self.df[
            (self.df['throughput_rps'] > 0) &
            (self.df['avg_latency_ms'] > 0) &
            (self.df['success_rate_pct'] > 90)
            ].copy()
        print(f"   清洗后: {len(self.df)} 条有效记录")

        # 初始化算法
        self.ds2 = DS2Algorithm(self.df, max_parallelism=max_parallelism)
        self.mapping = load_mapping()
        self.measure_fn = lambda p: get_perf_by_parallelism(p, self.mapping)
        self.infertuner = InferTunerAlgorithm(self.df)

        # 显示数据范围
        self._show_data_summary()

    def _show_data_summary(self):
        """显示数据概况"""
        print(f"\n📈 数据概况:")
        print(f"   并行度范围: {self.df['parallelism'].min()}-{self.df['parallelism'].max()}")
        print(f"   批大小范围: {self.df['batch_size'].min()}-{self.df['batch_size'].max()}")
        print(f"   请求速率范围: {self.df['target_rate'].min():.1f}-{self.df['target_rate'].max():.1f} req/s")
        print(f"   延迟范围: {self.df['avg_latency_ms'].min():.0f}-{self.df['avg_latency_ms'].max():.0f} ms")

    def generate_realistic_scenarios(self) -> List[Tuple[str, float, float]]:
        """基于真实数据生成测试场景"""
        min_latency = self.df['avg_latency_ms'].min()

        scenarios = [
            ("极低负载场景", 0.17, 12000),
            ("低负载场景", 0.5, 14000),
            ("中低负载场景", 0.8, 16000),
            ("中负载场景(小批量)", 1.0, 18000),
            ("中负载场景(大批量)", 1.2, 20000),
            ("中高负载场景", 1.4, 22000),
            ("较高负载场景", 1.5, 25000),
            ("高负载", 1.6, 30000),
            ("接近饱和负载", 1.69, 38000),
            ("峰值/饱和负载", 1.78, 48000),
        ]

        print(f"\n🎯 生成测试场景 (基于最低延迟{min_latency:.0f}ms):")
        for name, rate, slo in scenarios:
            print(f"   {name}: {rate}req/s, SLO≤{slo:.0f}ms")

        return scenarios

    def _analyze_comparison(
            self,
            ds2_result: Optional["Config"],
            conttune_result: Optional["Config"],
            infertuner_result: Optional["Config"],
            scenario_name: str
    ) -> Tuple[str, Optional[float], Optional[float]]:
        """
        分析对比结果:
        返回：
        - 最优算法名字
        - InferTuner 相对于 DS2 的 GPU 节省 (若 DS2 无解则为 None)
        - InferTuner 相对于 ContTune 的 GPU 节省 (若 ContTune 无解或 InferTuner 无解则为 None)
        """

        results = {
            "DS2": ds2_result,
            "ContTune": conttune_result,
            "InferTuner": infertuner_result
        }

        for name, res in results.items():
            if res:
                print(f"{name}: p={res.p}, b={res.b} → cost={res.cost}, latency={res.predicted_latency}, throughput={res.predicted_throughput}")
            else:
                print(f"{name}: 无解")

        # 选择 cost 最小的算法
        valid_results = {name: res for name, res in results.items() if res is not None}
        if not valid_results:
            print("❌ 所有算法无解")
            return "None", None, None

        best_name = min(valid_results, key=lambda k: valid_results[k].cost)
        print(f"✅ 最优算法（按cost）: {best_name}")

        # 计算 GPU 节省（InferTuner vs DS2）
        savings_vs_ds2 = None
        if ds2_result and infertuner_result:
            savings_vs_ds2 = ds2_result.cost - infertuner_result.cost

        # 计算 GPU 节省（InferTuner vs ContTune）
        savings_vs_conttune = None
        if conttune_result and infertuner_result:
            savings_vs_conttune = conttune_result.cost - infertuner_result.cost

        return best_name, savings_vs_ds2, savings_vs_conttune

    def compare_scenario(self, scenario_name: str, target_rate: float, target_slo: float):
        """对比单个场景，并返回详细记录"""
        print(f"\n" + "=" * 70)
        print(f"📊 场景: {scenario_name}")
        print("=" * 70)

        # 运行三种算法
        ds2_result = self.ds2.ds2_scaling_decision(target_rate, target_slo)

        conttune = ContTuneAlgorithm(
            measure_fn=self.measure_fn,
            target_throughput=target_rate,
            slo=target_slo,
            performance_data=self.df,
            max_parallelism=19,
            min_parallelism=1,
            big_multiplier=1.25,
            small_max_iters=3,
            history_max_len=10
        )
        conttune_result = conttune.conttune_scaling_decision(start_parallelism=1)

        infertuner_result = self.infertuner.infertuner_scaling_decision(target_rate, target_slo)

        # 计算最优算法 + 节省情况
        best_name, savings_vs_ds2, savings_vs_conttune = self._analyze_comparison(
            ds2_result=ds2_result,
            conttune_result=conttune_result,
            infertuner_result=infertuner_result,
            scenario_name=scenario_name
        )

        # 提取算法配置
        def extract_info(result: Optional["Config"]):
            if result:
                return result.p, result.b, result.cost, result.predicted_throughput, result.predicted_latency
            return None, None, None, None, None

        ds2_p, ds2_b, ds2_cost, ds2_tp, ds2_lat = extract_info(ds2_result)
        cont_p, cont_b, cont_cost, cont_tp, cont_lat = extract_info(conttune_result)
        inf_p, inf_b, inf_cost, inf_tp, inf_lat = extract_info(infertuner_result)

        record = {
            "Scenario": scenario_name,
            "Target_Throughput(req/s)": target_rate,
            "Target_SLO(ms)": target_slo,

            # DS2
            "DS2_p": ds2_p,
            "DS2_b": ds2_b,
            "DS2_cost": ds2_cost,
            "DS2_throughput": ds2_tp,
            "DS2_latency(ms)": ds2_lat,

            # ContTune
            "ContTune_p": cont_p,
            "ContTune_b": cont_b,
            "ContTune_cost": cont_cost,
            "ContTune_throughput": cont_tp,
            "ContTune_latency(ms)": cont_lat,

            # InferTuner
            "InferTuner_p": inf_p,
            "InferTuner_b": inf_b,
            "InferTuner_cost": inf_cost,
            "InferTuner_throughput": inf_tp,
            "InferTuner_latency(ms)": inf_lat,

            # 对比结果
            "Best_Algorithm": best_name,
            "InferTuner_vs_DS2_Savings": savings_vs_ds2,
            "InferTuner_vs_ContTune_Savings": savings_vs_conttune
        }

        return record

    def run_complete_comparison(self, output_csv="comparison_results.csv"):
        """运行完整对比，并将结果保存为CSV"""
        print(f"\n🚀 开始 DS2 vs ContTune vs InferTuner 完整对比")

        # 生成测试场景
        scenarios = self.generate_realistic_scenarios()

        # 执行对比
        all_records = []
        total_savings_ds2 = 0
        total_savings_conttune = 0

        for name, rate, slo in scenarios:
            record = self.compare_scenario(name, rate, slo)
            all_records.append(record)

            if record["InferTuner_vs_DS2_Savings"]:
                total_savings_ds2 += record["InferTuner_vs_DS2_Savings"]
            if record["InferTuner_vs_ContTune_Savings"]:
                total_savings_conttune += record["InferTuner_vs_ContTune_Savings"]

        # 转换为DataFrame并保存
        df = pd.DataFrame(all_records)
        df.to_csv(output_csv, index=False)
        print(f"\n✅ 结果已保存到 {output_csv}")
        print(f"🎯 总GPU节省 (InferTuner vs DS2): {total_savings_ds2}")
        print(f"🎯 总GPU节省 (InferTuner vs ContTune): {total_savings_conttune}")

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

    print("🎯 DS2 vs ContTune vs InferTuner 论文方法完整实现与验证")
    print("=" * 60)

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
