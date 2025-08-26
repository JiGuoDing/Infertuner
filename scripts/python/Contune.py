# We'll create a self-contained Python module implementing a simplified ContTune algorithm
# with a Big phase (doubling) and a Small phase (conservative binary search per operator).
# Then we'll run a quick demo on a simulated streaming job.

import math
import json
import random
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Tuple

# ------------------------- conttune.py (in-memory) -------------------------

@dataclass
class TuningConfig:
    target_throughput: float                 # SLA: required end-to-end throughput (records/sec)
    max_parallelism: int = 1024              # hard cap per operator
    min_parallelism: int = 1                 # floor per operator
    big_multiplier: float = 2.0              # Big phase expansion factor
    max_big_iters: int = 12                  # safety bound for Big phase
    small_max_passes: int = 4                # how many full passes over all operators
    small_trial_budget_per_op: int = 20      # max measurements per operator in Small phase
    safety_rechecks: int = 1                 # when found a candidate, recheck N times and keep worst
    verbose: bool = True


@dataclass
class MeasureResult:
    throughput: float            # measured end-to-end throughput
    cost: float                  # total resource cost (e.g., sum of parallelism or CPU cores)
    details: Dict = field(default_factory=dict)  # optional implementation-specific info


class ContTune:
    """
    A practical implementation of the ContTune idea for streaming jobs:
      - Big phase: expand parallelism quickly until SLA is met.
      - Small phase: per-operator conservative shrink via binary search, verifying SLA at every step.

    This variant treats the system as a black box with a measurement callback.
    It avoids library-heavy Bayesian optimization to stay lightweight and robust.
    """
    def __init__(self, op_names: List[str], measure_fn: Callable[[Dict[str, int]], MeasureResult], cfg: TuningConfig):
        self.op_names = op_names
        self.measure_fn = measure_fn
        self.cfg = cfg
        self.history: List[Dict] = []  # record of all evaluations

    @staticmethod
    def _resource_cost(config: Dict[str, int]) -> float:
        # Default cost = sum of parallelism; override by setting `cost` in MeasureResult if desired.
        return float(sum(config.values()))

    def _measure(self, config: Dict[str, int]) -> MeasureResult:
        res = self.measure_fn(config)
        if math.isnan(res.cost) or res.cost <= 0:
            res = MeasureResult(throughput=res.throughput, cost=self._resource_cost(config), details=res.details)
        self.history.append({"config": dict(config), "throughput": res.throughput, "cost": res.cost})
        if self.cfg.verbose:
            print(f"[MEASURE] cfg={config} -> thr={res.throughput:.2f}, cost={res.cost:.2f}")
        return res

    def _cap(self, v: int) -> int:
        return max(self.cfg.min_parallelism, min(self.cfg.max_parallelism, int(v)))

    # ----------------------- BIG PHASE -----------------------
    def big_phase(self, start_config: Dict[str, int]) -> Tuple[Dict[str, int], MeasureResult]:
        cfg = dict(start_config)
        res = self._measure(cfg)
        if res.throughput >= self.cfg.target_throughput:
            if self.cfg.verbose:
                print("[BIG] SLA already met at start config.")
            return cfg, res

        it = 0
        while res.throughput < self.cfg.target_throughput and it < self.cfg.max_big_iters:
            it += 1
            for op in self.op_names:
                cfg[op] = self._cap(math.ceil(cfg[op] * self.cfg.big_multiplier))
            if self.cfg.verbose:
                print(f"[BIG] iter={it} expanded cfg={cfg}")
            res = self._measure(cfg)
            if all(cfg[op] >= self.cfg.max_parallelism for op in self.op_names):
                if self.cfg.verbose:
                    print("[BIG] Reached max parallelism for all operators.")
                break

        return cfg, res

    # ----------------------- SMALL PHASE -----------------------
    def _binary_search_min_parallelism(self, current_cfg: Dict[str, int], op: str) -> Tuple[int, MeasureResult]:
        """
        For a single operator, find the smallest parallelism that still satisfies SLA,
        while keeping other operators fixed. Uses conservative checks.
        """
        left = self.cfg.min_parallelism
        right = current_cfg[op]
        best_p = right
        best_res = None

        trials = 0
        while left <= right and trials < self.cfg.small_trial_budget_per_op:
            trials += 1
            mid = (left + right) // 2
            test_cfg = dict(current_cfg)
            test_cfg[op] = mid

            # Conservative rechecks: keep worst observed throughput
            worst_thr = float("inf")
            worst_cost = None
            worst_details = None
            for _ in range(self.cfg.safety_rechecks):
                res = self._measure(test_cfg)
                if res.throughput < worst_thr:
                    worst_thr = res.throughput
                    worst_cost = res.cost
                    worst_details = res.details
            res = MeasureResult(throughput=worst_thr, cost=worst_cost, details=worst_details)

            if res.throughput >= self.cfg.target_throughput:
                best_p = mid
                best_res = res
                right = mid - 1  # try even smaller
            else:
                left = mid + 1   # need bigger

        if best_res is None:
            # couldn't meet SLA at any smaller setting; keep current
            return current_cfg[op], self._measure(current_cfg)
        return best_p, best_res

    def small_phase(self, cfg: Dict[str, int]) -> Tuple[Dict[str, int], MeasureResult]:
        """
        Iterate operators and shrink conservatively with binary search.
        Stop early if no improvement in a full pass.
        """
        # Ensure starting point satisfies SLA
        start_res = self._measure(cfg)
        if start_res.throughput < self.cfg.target_throughput:
            if self.cfg.verbose:
                print("[SMALL] Start config does not meet SLA. Run big_phase first.")
            return cfg, start_res

        prev_cost = start_res.cost
        for pas in range(1, self.cfg.small_max_passes + 1):
            if self.cfg.verbose:
                print(f"[SMALL] Pass {pas} starting ...")
            improved = False
            for op in self.op_names:
                current = dict(cfg)
                p_min, res = self._binary_search_min_parallelism(current, op)
                if p_min < cfg[op]:
                    cfg[op] = p_min
                    # refresh measurement at new cfg to update prev_cost accurately
                    final_res = self._measure(cfg)
                    if final_res.cost < prev_cost - 1e-6:
                        prev_cost = final_res.cost
                        improved = True
                        if self.cfg.verbose:
                            print(f"[SMALL] Reduced {op} -> {p_min}. New cost={prev_cost:.2f}")
                    else:
                        # No real cost improvement; revert
                        cfg[op] = current[op]
                        if self.cfg.verbose:
                            print(f"[SMALL] Reverting {op}, no cost gain.")
                else:
                    if self.cfg.verbose:
                        print(f"[SMALL] {op} already minimal at {cfg[op]} for SLA.")

            if not improved:
                if self.cfg.verbose:
                    print("[SMALL] No further improvements; stopping.")
                break

        final_res = self._measure(cfg)
        return cfg, final_res

    # ----------------------- PUBLIC API -----------------------
    def tune(self, start_config: Dict[str, int]) -> Dict:
        big_cfg, big_res = self.big_phase(start_config)
        if big_res.throughput < self.cfg.target_throughput:
            outcome = {
                "status": "failed_big_phase",
                "config": big_cfg,
                "result": big_res.__dict__,
                "history": self.history,
            }
            return outcome

        small_cfg, small_res = self.small_phase(big_cfg)

        outcome = {
            "status": "ok",
            "start_config": start_config,
            "big_config": big_cfg,
            "big_result": big_res.__dict__,
            "final_config": small_cfg,
            "final_result": small_res.__dict__,
            "history": self.history,
        }
        return outcome


# ------------------------- Demo: simulated streaming job -------------------------
# We create a synthetic DAG with three operators where capacity scales sublinearly
# with parallelism due to coordination overhead: cap_i(p) = a_i * p^alpha / (1 + b_i * (p-1)).
# End-to-end throughput = min_i cap_i(p_i). Cost = sum p_i.

class SimStreamingJob:
    def __init__(self, op_names: List[str]):
        self.op_names = op_names
        # random but fixed parameters for reproducibility
        rnd = random.Random(7)
        self.a = {op: rnd.uniform(80, 140) for op in op_names}   # base per-task rate
        self.alpha = {op: rnd.uniform(0.85, 0.98) for op in op_names}
        self.b = {op: rnd.uniform(0.01, 0.06) for op in op_names}
        self.noise = 0.02  # 2% measurement noise

    def capacity(self, config: Dict[str, int]) -> Dict[str, float]:
        caps = {}
        for op, p in config.items():
            p = max(1, int(p))
            caps[op] = self.a[op] * (p ** self.alpha[op]) / (1.0 + self.b[op] * max(0, p - 1))
        return caps

    def measure(self, config: Dict[str, int]) -> MeasureResult:
        caps = self.capacity(config)
        thr = min(caps.values())
        # add mild multiplicative noise
        thr *= (1.0 + random.uniform(-self.noise, self.noise))
        cost = float(sum(config.values()))
        details = {"per_op_capacity": caps}
        return MeasureResult(throughput=thr, cost=cost, details=details)


def run_demo():
    op_names = ["source", "map", "sink"]
    sim = SimStreamingJob(op_names)
    target_thr = 180.0  # SLA

    cfg = TuningConfig(
        target_throughput=target_thr,
        verbose=True,
        small_max_passes=3,
        small_trial_budget_per_op=12,
        safety_rechecks=1,
    )

    tuner = ContTune(op_names=op_names, measure_fn=sim.measure, cfg=cfg)

    start = {op: 1 for op in op_names}
    result = tuner.tune(start)

    print("\n=== Tuning outcome ===")
    print(json.dumps({
        "status": result["status"],
        "start_config": result.get("start_config"),
        "big_config": result.get("big_config"),
        "final_config": result.get("final_config"),
        "final_result": result.get("final_result"),
    }, indent=2))
