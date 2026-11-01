"""
execution-accuracy scoring

an answer is correct when its result contains the gold result:
  - same number of rows
  - every gold column matches some distinct predicted column (names ignored)
  - after projecting the prediction onto those columns, the rows are the same
    multiset (or the same sequence when the question is ordered)
extra predicted columns are allowed, so "district, total" still matches a gold
"district" only. floats are compared after rounding, strings case-insensitively
"""
from __future__ import annotations

from collections import Counter
from typing import Any

FLOAT_DP = 1


def norm(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return round(float(v), FLOAT_DP)
    if hasattr(v, "__float__") and not isinstance(v, str):  # decimals
        return round(float(v), FLOAT_DP)
    return str(v).strip().lower()


def _col(rows: list[tuple], i: int) -> list:
    return [norm(r[i]) for r in rows]


def _sort_key(v):
    return (v is None, str(type(v)), v if v is not None else 0)


def results_match(pred_rows: list[tuple], gold_rows: list[tuple], ordered: bool = False) -> bool:
    if len(pred_rows) != len(gold_rows):
        return False
    if not gold_rows:
        return True
    n_gold, n_pred = len(gold_rows[0]), len(pred_rows[0])
    if n_pred < n_gold:
        return False

    gold_cols = [sorted(_col(gold_rows, i), key=_sort_key) for i in range(n_gold)]
    pred_cols = [sorted(_col(pred_rows, j), key=_sort_key) for j in range(n_pred)]
    candidates = [[j for j in range(n_pred) if pred_cols[j] == gold_cols[i]] for i in range(n_gold)]
    if any(not c for c in candidates):
        return False

    gold_proj = [tuple(norm(v) for v in r) for r in gold_rows]
    # try column assignments; gold results are narrow so this stays tiny
    for assign in _assignments(candidates):
        pred_proj = [tuple(norm(r[j]) for j in assign) for r in pred_rows]
        if ordered:
            if pred_proj == gold_proj:
                return True
        elif Counter(pred_proj) == Counter(gold_proj):
            return True
    return False


def _assignments(candidates: list[list[int]], used: frozenset = frozenset()):
    if not candidates:
        yield ()
        return
    for j in candidates[0]:
        if j in used:
            continue
        for rest in _assignments(candidates[1:], used | {j}):
            yield (j,) + rest
