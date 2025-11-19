from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, List, Tuple, Optional
import time
import heapq
import math

from fp_tree import build_fptree, fpgrowth

# --- utils ---

def filter_largest_patterns(expls: List[Explanation]) -> List[Explanation]:
    """Keep the largest patterns, discarding only fully contained subsets, ignoring RR equality."""
    # Sort by pattern length descending
    expls.sort(key=lambda e: len(e.items), reverse=True)

    largest: List[Explanation] = []
    for ex in expls:
        if not any(set(ex.items).issubset(set(kp.items)) for kp in largest):
            largest.append(ex)
    return largest

def filter_maximal_patterns(expls: List[Explanation]) -> List[Explanation]:
    """Keep only maximal patterns with same support and risk ratio."""
    # Sort by pattern length descending so we keep largest first
    expls.sort(key=lambda e: len(e.items), reverse=True)

    maximal: List[Explanation] = []
    for ex in expls:
        is_subset = False
        for kept in maximal:
            if (
                set(ex.items).issubset(set(kept.items)) and
                abs(ex.risk_ratio - kept.risk_ratio) < 1e-9 and
                abs(ex.support_outlier - kept.support_outlier) < 1e-9 and
                abs(ex.support_inlier - kept.support_inlier) < 1e-9
            ):
                is_subset = True
                break
        if not is_subset:
            maximal.append(ex)

    return maximal

def risk_ratio(ao: float, ai: float, bo: float, bi: float, eps: float = 1e-9) -> float:
    num = (ao +1)/ max(ao + ai+2, eps)
    den = (bo+1) / max(bo + bi+2, eps)
    if den == 0:
        den = eps
    return num / den


def canonical_items(attrs: Dict[str, Any]) -> Tuple[Tuple[str, Any], ...]:
    return tuple(sorted(attrs.items()))


# --- AMC ---
class AMC:
    def __init__(self, epsilon: float = 0.001, decay_rate: float = 0.0) -> None:
        assert 0 < epsilon <= 1.0
        assert 0.0 <= decay_rate < 1.0
        self.counts: Dict[Any, float] = {}
        self._wi: float = 0.0
        self.decay_rate = decay_rate

    def observe(self, item: Any, c: float = 1.0) -> None:
        if item in self.counts:
            self.counts[item] += c
        else:
            self.counts[item] = self._wi + c

    def batch_observe(self, items: Iterable[Any], c: float = 1.0) -> None:
        for it in items:
            self.observe(it, c)

    def maintain_by_size(self, stable_size: int) -> None:
        if stable_size <= 0 or not self.counts or len(self.counts) <= stable_size:
            return
        largest = heapq.nlargest(stable_size, self.counts.items(), key=lambda kv: kv[1])
        items_sorted = sorted(self.counts.items(), key=lambda kv: kv[1], reverse=True)
        new_counts: Dict[Any, float] = {}
        kept = 0
        removed_max = 0.0
        for k, v in items_sorted:
            if kept < stable_size:
                new_counts[k] = v
                kept += 1
            else:
                removed_max = max(removed_max, v)
        self.counts = new_counts
        self._wi = removed_max

    def decay(self) -> None:
        if self.decay_rate <= 0 or not self.counts:
            return
        factor = 1.0 - self.decay_rate
        for k in list(self.counts.keys()):
            self.counts[k] *= factor
            if self.counts[k] < 1e-12:
                del self.counts[k]
        self._wi *= factor

    def get(self, item: Any) -> float:
        return self.counts.get(item, 0.0)

    def keys(self):
        return self.counts.keys()


@dataclass
class Explanation:
    items: Tuple[Tuple[str, Any], ...]
    ao: float
    ai: float
    bo: float
    bi: float
    support_outlier: float
    support_inlier: float
    risk_ratio: float
    k: int


class MDPStreamExplainer:
    def __init__(
        self,
        *,
        min_outlier_support: float = 0.1,
        min_risk_ratio: float = 1.1,
        max_len: int = 3,
        epsilon_amc: float = 0.001,
        decay_rate: float = 0.0,
        amc_stable_size: int = 5000,
        window_max_events: int = 5,
        window_max_seconds: Optional[float] = 10,
        slide_step:int=None
    ) -> None:
        self.min_outlier_support = min_outlier_support
        self.min_risk_ratio = min_risk_ratio
        self.max_len = max_len
        self.amc_out = AMC(epsilon=epsilon_amc, decay_rate=decay_rate)
        self.amc_in = AMC(epsilon=epsilon_amc, decay_rate=decay_rate)
        self.total_o = 0.0
        self.total_i = 0.0
        self.window_outlier_tx: List[List[Tuple[str, Any]]] = []
        self.window_inlier_tx: List[List[Tuple[str, Any]]] = []
        self.window_events = 0
        self.window_started_at = time.time()
        self.window_max_events = window_max_events
        self.window_max_seconds = window_max_seconds
        self.slide_step=slide_step or window_max_events
        self.amc_stable_size = amc_stable_size
        self.decay_rate = decay_rate

    # ---- discretization helper ----
    @staticmethod
    def bucketize_01(x: float, bins: int = 5) -> str:
        if x is None or math.isnan(x):
            return "nan"
        i = int(min(bins - 1, max(0, math.floor(x * bins))))
        return f"[{i}/{bins})"

    @staticmethod
    def to_attributes(raw_features: Dict[str, Any], *, bins: int = 5, extra_cats: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        # Convert numeric 0..1 features into categorical bins; merge extra categorical attrs (e.g., weather, country)
        attrs: Dict[str, Any] = {}
        for k, v in raw_features.items():
            if isinstance(v, (int, float)):
                attrs[f"{k}_bin"] = MDPStreamExplainer.bucketize_01(float(v), bins)
            else:
                attrs[k] = v
        if extra_cats:
            for k, v in extra_cats.items():
                attrs[k] = v
        return attrs

    # ---- observe message ----
    def observe(self, attrs: Dict[str, Any], is_outlier: bool) -> None:
        items = canonical_items(attrs)
        if is_outlier:
            self.total_o += 1.0
            self.amc_out.batch_observe(items, 1.0)
            self.window_outlier_tx.append(list(items))
        else:
            self.total_i += 1.0
            self.amc_in.batch_observe(items, 1.0)
            self.window_inlier_tx.append(list(items))
        self.window_events += 1
        # if self.window_events % 256 == 0:
        #     self.amc_out.maintain_by_size(self.amc_stable_size)
        #     self.amc_in.maintain_by_size(self.amc_stable_size)
        if len(self.window_outlier_tx) > self.window_max_events:
            self.window_outlier_tx.pop(0)
        if len(self.window_inlier_tx) > self.window_max_events:
            self.window_inlier_tx.pop(0)


    # def _window_ready(self) -> bool:
    #     if self.window_max_events and self.window_events >= self.window_max_events:
    #         return True
    #     if self.window_max_seconds is not None and (time.time() - self.window_started_at) >= self.window_max_seconds:
    #         return True
    #     return False
    def _window_ready(self) -> bool:
        # Εκπέμπει κάθε φορά που έχουν έρθει slide_step νέα γεγονότα
        return self.window_events % self.slide_step == 0


    def maybe_emit(self) -> List[Explanation]:
        if not self._window_ready():
            return []
        return self._emit_and_roll()

    def force_emit(self) -> List[Explanation]:
        return self._emit_and_roll()

    def _emit_and_roll(self) -> List[Explanation]:
        expls: List[Explanation] = []

        # (1) Singletons filter
        passed: List[Tuple[str, Any]] = []
        if self.total_o > 0 and self.total_i > 0:
            for it in list(self.amc_out.keys()):
                ao = self.amc_out.get(it)
                ai = self.amc_in.get(it)
                sup_o = ao / max(self.total_o, 1e-9)
                bo = self.total_o - ao
                bi = self.total_i - ai
                rr = risk_ratio(ao, ai, bo, bi)
                print("TOTAL OUTLIERS:",self.total_o,"TOTAL INLIERS:",self.total_i)
                print("inliers with this attribute:",ai,"outliers with this attribute:",ao,"inliers WITHOUT this attribute:",bi,"outliers WITHOUT this attribute:",bo)
                print("RISK RATIO:",rr)
                if sup_o >= self.min_outlier_support and rr >= self.min_risk_ratio:
                    passed.append(it)

        # (2) FP-Growth on outliers with only passed items
        if self.total_o > 0 and passed:
            pset = set(passed)
            filtered_outlier_tx: List[List[Tuple[str, Any]]] = []
            for tx in self.window_outlier_tx:
                keep = [it for it in tx if it in pset]
                if keep:
                    filtered_outlier_tx.append(keep)
            min_support_count = self.min_outlier_support * self.total_o
            tree = build_fptree(filtered_outlier_tx, None, min_support_count)
            candidates = fpgrowth(tree, suffix=tuple(), min_support_count=min_support_count, max_len=self.max_len)

            # (3) Final RR filter vs inliers (only for candidates)
            for cand_items, ao in candidates:
                if len(cand_items) > self.max_len:
                    continue
                ai = 0.0
                cset = set(cand_items)
                for itx in self.window_inlier_tx:
                    if cset.issubset(set(itx)):
                        ai += 1.0
                bo = self.total_o - ao
                bi = self.total_i - ai
                sup_o = ao / max(self.total_o, 1e-9)
                sup_i = ai / max(self.total_i, 1e-9) if self.total_i > 0 else 0.0
                rr = risk_ratio(ao, ai, bo, bi)
                if rr >= self.min_risk_ratio:
                    expls.append(Explanation(
                        items=tuple(sorted(cand_items)),
                        ao=ao, ai=ai, bo=bo, bi=bi,
                        support_outlier=sup_o, support_inlier=sup_i,
                        risk_ratio=rr, k=len(cand_items)
                    ))

        # Include singleton explanations too (MacroBase returns all subsets that pass)
        for it in passed:
            ao = self.amc_out.get(it)
            ai = self.amc_in.get(it)
            bo = self.total_o - ao
            bi = self.total_i - ai
            sup_o = ao / max(self.total_o, 1e-9)
            sup_i = ai / max(self.total_i, 1e-9) if self.total_i > 0 else 0.0
            rr = risk_ratio(ao, ai, bo, bi)
            expls.append(Explanation(
                items=(it,),
                ao=ao, ai=ai, bo=bo, bi=bi,
                support_outlier=sup_o, support_inlier=sup_i,
                risk_ratio=rr, k=1
            ))

        # sort
        expls.sort(key=lambda e: (e.risk_ratio, e.support_outlier), reverse=True)

        # Option 1: keep current maximal filtering (existing behavior)
        expls = filter_maximal_patterns(expls)

        # # Option 2: keep the largest patterns (new behavior)
        # expls = filter_largest_patterns(expls)

        # roll window: decay & maintenance, reset buffers
        self.amc_out.decay()
        self.amc_in.decay()
        factor = 1.0 - self.decay_rate
        self.total_o *= factor
        self.total_i *= factor
        self.amc_out.maintain_by_size(self.amc_stable_size)
        self.amc_in.maintain_by_size(self.amc_stable_size)
        # self.window_outlier_tx.clear(); self.window_inlier_tx.clear()
        # self.window_events = 0
        # self.window_started_at = time.time()
        return expls