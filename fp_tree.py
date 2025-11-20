from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple, Iterable
from collections import Counter

class FPNode:
    __slots__ = ("item", "count", "parent", "children", "node_link")


    def __init__(self, item: Any, count: float, parent: Optional["FPNode"]) -> None:
        self.item = item
        self.count = count
        self.parent = parent
        self.children: Dict[Any, FPNode] = {}
        self.node_link: Optional[FPNode] = None

class FPTree:
    def __init__(self, min_support_count: float) -> None:
        self.root = FPNode(None, 0.0, None)
        self.header: Dict[Any, FPNode] = {}
        self.min_support_count = min_support_count
        self.item_counts: Counter = Counter()


    def add_observation(self, items: Iterable[Any], weight: float = 1.0) -> None:
        cur = self.root
        for it in items:
            if it in cur.children:
                node = cur.children[it]
                node.count += weight
            else:
                node = FPNode(it, weight, cur)
                cur.children[it] = node
                # header link
                if it in self.header:
                    last = self.header[it]
                    while last.node_link is not None:
                        last = last.node_link
                    last.node_link = node
                else:
                    self.header[it] = node
            cur = node


    def conditional_pattern_base(self, item: Any) -> List[Tuple[List[Any], float]]:
        patterns: List[Tuple[List[Any], float]] = []
        node = self.header.get(item)
        while node is not None:
            path: List[Any] = []
            p = node.parent
            while p is not None and p.item is not None:
                path.append(p.item)
                p = p.parent
            if path:
                patterns.append((list(reversed(path)), node.count))
            node = node.node_link
        return patterns
    
def build_fptree(observations: List[List[Any]], weights: Optional[List[float]], min_support_count: float) -> FPTree:
    item_counts: Counter = Counter()
    if weights is None:
        weights = [1.0] * len(observations)
    for obs, w in zip(observations, weights):
        for it in obs:
            item_counts[it] += w


    frequent = {it for it, c in item_counts.items() if c >= min_support_count}
    tree = FPTree(min_support_count)
    tree.item_counts = item_counts
    if not frequent:
        return tree


    order_key = lambda it: (-item_counts[it], it)
    for obs, w in zip(observations, weights):
        filtered = [it for it in obs if it in frequent]
        if not filtered:
            continue
        ordered = sorted(filtered, key=order_key)
        tree.add_observation(ordered, w)
    return tree

def fpgrowth(tree: FPTree, suffix: Tuple[Any, ...], min_support_count: float, max_len: int) -> List[Tuple[Tuple[Any, ...], float]]:
    results: List[Tuple[Tuple[Any, ...], float]] = []
    if not tree.header:
        return results
    items = sorted(tree.header.keys(), key=lambda it: tree.item_counts[it])
    for it in items:
        sup = tree.item_counts[it]
        if sup < min_support_count:
            continue
        new_itemset = tuple(sorted((it,) + suffix))
        if len(new_itemset) <= max_len:
            results.append((new_itemset, sup))
        # conditional
        base = tree.conditional_pattern_base(it)
        if not base:
            continue
        cond_obs = [p for p, _ in base]
        cond_w = [w for _, w in base]
        cond_tree = build_fptree(cond_obs, cond_w, min_support_count)
        if cond_tree.header:
            results.extend(fpgrowth(cond_tree, new_itemset, min_support_count, max_len))
    return results