from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, List, Tuple, Optional
import math

from fp_tree import build_fptree, fpgrowth

# --- utils ---

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



def dict_to_tuple_sorted(attrs: Dict[str, Any]) -> Tuple[Tuple[str, Any], ...]:
    return tuple(sorted(attrs.items()))


# --- AMC ---
class AMC:
    def __init__(self, decay_rate: float = 0.0) -> None:
        assert 0.0 <= decay_rate < 1.0
        self.counts: Dict[Any, float] = {}
        self.error_floor: float = 0.0
        self.decay_rate = decay_rate

    def observe(self, item: Any, c: float = 1.0) -> None:
        if item in self.counts:
            self.counts[item] += c
        else:
            self.counts[item] = self.error_floor + c

    def batch_observe(self, items: Iterable[Any], c: float = 1.0) -> None:
        for it in items:
            self.observe(it, c)

    def maintain_by_size(self, stable_size: int) -> None:
        if stable_size <= 0 or not self.counts or len(self.counts) <= stable_size:
            return
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
        self.error_floor = removed_max

    def decay(self) -> None:
        if self.decay_rate <= 0 or not self.counts:
            return
        factor = 1.0 - self.decay_rate
        for k in list(self.counts.keys()):
            self.counts[k] *= factor
            if self.counts[k] < 1e-12:
                del self.counts[k]
        self.error_floor *= factor

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
        decay_rate: float = 0.0,
        amc_stable_size: int = 500,
        window_max_events: int = 100,
        emit_after_events:int=20
    ) -> None:
        self.min_outlier_support = min_outlier_support
        self.min_risk_ratio = min_risk_ratio
        self.max_len = max_len
        self.decay_rate = decay_rate
        self.amc_stable_size = amc_stable_size
        self.window_max_events = window_max_events
        self.emit_after_events=emit_after_events
        self.amc_out = AMC(decay_rate=decay_rate)
        self.amc_in = AMC(decay_rate=decay_rate)
        self.total_o = 0.0
        self.total_i = 0.0
        self.window_observations: List[Tuple[bool, List[Tuple[str, Any]]]] = []
        self.window_events = 0

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
        items = dict_to_tuple_sorted(attrs)
        if is_outlier:
            self.total_o += 1.0
            self.amc_out.batch_observe(items, 1.0)
            # self.window_outliers_observations.append(list(items))
        else:
            self.total_i += 1.0
            self.amc_in.batch_observe(items, 1.0)
            # self.window_inlier_observations.append(list(items))
        
        self.window_observations.append((is_outlier,list(items)))
        self.window_events += 1
        if len(self.window_observations) > self.window_max_events:
            self.window_observations.pop(0)

    def window_ready(self) -> bool:
        # Εκπέμπει κάθε φορά που έχουν έρθει emit_after_events νέα γεγονότα
        return self.window_events % self.emit_after_events == 0


    def maybe_emit(self) -> List[Explanation]:
        if not self.window_ready():
            return []
        return self._emit_and_roll()

    def force_emit(self) -> List[Explanation]:
        return self._emit_and_roll()

    def _emit_and_roll(self) -> List[Explanation]:
        expls: List[Explanation] = []
        seen: set = set()

        # (1) singleton-amc based
        passed: List[Tuple[str, Any]] = []

        if self.total_o > 0 and self.total_i > 0:
            for it in list(self.amc_out.keys()):
                ao = self.amc_out.get(it)
                ai = self.amc_in.get(it)
                bo = self.total_o - ao
                bi = self.total_i - ai
                sup_o = ao / max(self.total_o, 1e-9)
                sup_i = ai / max(self.total_i, 1e-9) if self.total_i > 0 else 0.0
                rr = risk_ratio(ao, ai, bo, bi)
                if sup_o >= self.min_outlier_support and rr >= self.min_risk_ratio:
                    passed.append(it)
                    key = (it,)
                    seen.add(it,)
                    expls.append(Explanation(
                        items=(it,),
                        ao=ao, ai=ai, bo=bo, bi=bi,
                        support_outlier=sup_o, support_inlier=sup_i,
                        risk_ratio=rr, k=1
                    ))

        # windows data for outliers/inliers
        window_out: List[List[Tuple[str, Any]]] = []
        window_in: List[List[Tuple[str, Any]]] = []

        for is_out, obs in self.window_observations:
            if is_out:
                window_out.append(obs)
            else:
                window_in.append(obs)

        total_o_w = float(len(window_out))
        total_i_w = float(len(window_in))

        # patterns fp-growth based
        if total_o_w > 0 and passed:
            pset = set(passed)
            filtered_out: List[List[Tuple[str, Any]]] = []

            for obs in window_out:
                keep = [it for it in obs if it in pset]
                if len(keep) >= 2:
                    filtered_out.append(keep)

            min_support_count = self.min_outlier_support * total_o_w

            tree = build_fptree(filtered_out, None, min_support_count)
            candidates = fpgrowth(
                tree,
                suffix=tuple(),
                min_support_count=min_support_count,
                max_len=self.max_len
            )

            for cand_items, ao in candidates:
                key = tuple(sorted(cand_items))
                if key in seen:
                    continue
                seen.add(key)
                if len(cand_items) < 2:
                    continue
                cset = set(cand_items)
                ai = 0.0
                for obs in window_in:
                    if cset.issubset(set(obs)):
                        ai += 1.0
                bo = total_o_w - ao
                bi = total_i_w - ai
                sup_o = ao / max(total_o_w, 1e-9)
                sup_i = ai / max(total_i_w, 1e-9) if total_i_w > 0 else 0.0
                rr = risk_ratio(ao, ai, bo, bi)
                if rr >= self.min_risk_ratio:
                    expls.append(Explanation(
                        items=key,
                        ao=ao, ai=ai, bo=bo, bi=bi,
                        support_outlier=sup_o, support_inlier=sup_i,
                        risk_ratio=rr, k=len(key)
                    ))
        
        # sort and filter
        expls.sort(key=lambda e: (e.risk_ratio, e.support_outlier), reverse=True)
        expls = filter_maximal_patterns(expls)

        # AMC decay
        self.amc_out.decay()
        self.amc_in.decay()
        factor = 1.0 - self.decay_rate
        self.total_o *= factor
        self.total_i *= factor
        self.amc_out.maintain_by_size(self.amc_stable_size)
        self.amc_in.maintain_by_size(self.amc_stable_size)

        return expls
