from __future__ import annotations
from typing import Any, Dict, Iterable, Tuple


def risk_ratio(ao: float, ai: float, bo: float, bi: float, eps: float = 1e-9) -> float:
    """Relative Risk (RR) used by MacroBase.
    RR = (ao / (ao + ai)) / (bo / (bo + bi))
    Smoothing via eps to avoid div-by-zero explosions.
    """
    num = ao / max(ao + ai, eps)
    den = bo / max(bo + bi, eps)
    if den == 0:
        den = eps
    return num / den


def canonical_items(attrs: Dict[str, Any]) -> Tuple[Tuple[str, Any], ...]:
    """Return (attr, value) pairs in deterministic sorted order."""
    return tuple(sorted(attrs.items()))


def normalize_event_attrs(attrs: Dict[str, Any]) -> Dict[str, Any]:
    """Optional hook to coerce values to hashable/str for pattern mining."""
    norm = {}
    for k, v in attrs.items():
        if isinstance(v, (list, dict, set)):
            v = str(v)
        norm[k] = v
    return norm