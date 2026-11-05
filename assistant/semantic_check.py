"""
value-aware checks on a query, run after the sql guard

the guard asks "is this query allowed". this asks "does this query make sense for
this data": do the names it filters on exist, and are periods and quarters in the
right format. problems go back to the model as fixable hints, e.g.

    period = '2025-Q3'            -> period is 'YYYYMM'; for quarters use quarter = '2025-Q3'
    data_element = 'DMPA stock-outs' -> not a data element. closest: ... if none fit, decline

these rules are general, not tied to any evaluation question
"""
from __future__ import annotations

import difflib
import re

import duckdb
import sqlglot
from sqlglot import exp

PERIOD_RE = re.compile(r"^\d{6}$")
QUARTER_RE = re.compile(r"^\d{4}-Q[1-4]$")


def build_vocab(con: duckdb.DuckDBPyConnection) -> dict[str, set[str]]:
    """distinct values for the categorical columns people filter on"""
    q = {
        "country": "SELECT DISTINCT country FROM facilities",
        "district": "SELECT DISTINCT district FROM facilities",
        "region": "SELECT DISTINCT region FROM facilities",
        "data_element": "SELECT DISTINCT data_element FROM data_elements",
        "value_type": "SELECT DISTINCT value_type FROM data_elements",
        "health_area": "SELECT DISTINCT health_area FROM service_delivery",
        "reason": "SELECT DISTINCT reason FROM quarantine_summary",
    }
    return {col: {r[0] for r in con.execute(sql).fetchall() if r[0] is not None} for col, sql in q.items()}


def _literal_pairs(tree: exp.Expression):
    """yield (column_name, literal_string, node_kind) for col = 'x', col IN ('x', ...), col LIKE 'x'"""
    for node in tree.find_all(exp.EQ, exp.NEQ, exp.Like, exp.ILike, exp.In):
        if isinstance(node, exp.In):
            col = node.this
            lits = [e for e in node.expressions if isinstance(e, exp.Literal) and e.is_string]
            if isinstance(col, exp.Column):
                for lit in lits:
                    yield col.name.lower(), lit.this, "eq"
            continue
        left, right = node.this, node.expression
        if isinstance(right, exp.Column) and isinstance(left, exp.Literal):
            left, right = right, left
        if isinstance(left, exp.Column) and isinstance(right, exp.Literal) and right.is_string:
            kind = "like" if isinstance(node, (exp.Like, exp.ILike)) else "eq"
            yield left.name.lower(), right.this, kind


def _suggest(value: str, choices: set[str], n: int = 4) -> list[str]:
    lower = {c.lower(): c for c in choices}
    if value.lower() in lower:
        return [lower[value.lower()]]
    close = difflib.get_close_matches(value.lower(), list(lower), n=n, cutoff=0.5)
    if not close:  # fall back to word overlap, e.g. 'RDT tests' vs 'RDT tests conducted'
        words = set(re.findall(r"\w+", value.lower()))
        scored = sorted(lower, key=lambda c: -len(words & set(re.findall(r"\w+", c))))
        close = [c for c in scored[:n] if words & set(re.findall(r"\w+", c))]
    return [lower[c] for c in close]


def check(sql: str, vocab: dict[str, set[str]]) -> list[str]:
    """returns a list of problems (empty when the query looks sound)"""
    try:
        tree = sqlglot.parse_one(sql, read="duckdb")
    except sqlglot.errors.ParseError:
        return []  # the guard reports parse errors
    problems: list[str] = []

    if not any(True for _ in tree.find_all(exp.Table)):
        problems.append("the query does not read any table. select the answer from the data")

    for col, val, kind in _literal_pairs(tree):
        if col == "period":
            pattern = val.replace("%", "").replace("_", "")
            if kind == "like" and pattern and not pattern.isdigit():
                problems.append(f"period LIKE '{val}': period is 'YYYYMM' digits only, e.g. LIKE '2025%'"
                                " or use the year, month or quarter columns")
            elif kind == "eq" and not PERIOD_RE.match(val):
                hint = (f"; for quarters use quarter = '{val}'" if QUARTER_RE.match(val) else "")
                problems.append(f"period = '{val}': period is 'YYYYMM' text, e.g. '202509'{hint}")
        elif col == "quarter" and kind == "eq" and not QUARTER_RE.match(val):
            problems.append(f"quarter = '{val}': quarter looks like '2025-Q3'")
        elif col in vocab and kind == "eq" and val not in vocab[col]:
            sugg = _suggest(val, vocab[col])
            if sugg and sugg[0].lower() == val.lower():
                problems.append(f"{col} = '{val}': values are case-sensitive, use '{sugg[0]}'")
            else:
                tail = f" closest: {', '.join(repr(s) for s in sugg)}." if sugg else ""
                if col == "data_element":
                    extra = (" if none of these is what the user asked for, the data does not contain it: call decline"
                             if sugg else " nothing similar exists, so the data does not contain it: call decline")
                else:
                    extra = ""
                problems.append(f"{col} = '{val}' does not exist in the data.{tail}{extra}")
    # keep each message once, in order
    return list(dict.fromkeys(problems))
