"""
static checks on llm-written sql before it touches the database

the database is already locked down (see catalog.build_connection), so this is
the first of two layers. it gives the model a clear, fixable error message
instead of an opaque duckdb failure, and it is unit-testable without an api key
"""
from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp

# table functions and helpers that reach outside the curated tables
BLOCKED_FUNCTION_PREFIXES = ("read_", "parquet_", "sniff_", "glob", "query", "duckdb_",
                             "pragma_", "getenv", "current_setting", "sqlite_", "postgres_",
                             "iceberg_", "delta_", "json_execute", "load", "install")

DISALLOWED_NODES = (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create, exp.Alter,
                    exp.Command, exp.Pragma, exp.Set, exp.Copy, exp.Merge, exp.TruncateTable,
                    exp.Use, exp.Transaction, exp.Commit, exp.Rollback)


class GuardError(ValueError):
    """raised with a message that is safe and useful to show the model"""


@dataclass
class CheckedSQL:
    sql: str            # the sql actually executed (with row cap)
    original: str
    tables: set[str]


def check_sql(sql: str, allowed_tables: frozenset[str] | set[str], max_rows: int = 200) -> CheckedSQL:
    text = (sql or "").strip().rstrip(";").strip()
    if not text:
        raise GuardError("empty query")

    try:
        statements = sqlglot.parse(text, read="duckdb")
    except sqlglot.errors.ParseError as e:
        raise GuardError(f"could not parse sql: {str(e).splitlines()[0]}") from None
    statements = [s for s in statements if s is not None]
    if len(statements) != 1:
        raise GuardError("send exactly one statement")
    tree = statements[0]

    if not isinstance(tree, exp.Query):
        raise GuardError("only SELECT queries are allowed")
    for node in tree.walk():
        if isinstance(node, DISALLOWED_NODES):
            raise GuardError(f"{type(node).__name__.upper()} is not allowed, only SELECT")

    # functions that read files, settings or the catalog
    for fn in tree.find_all(exp.Func):
        name = (fn.sql_name() if not isinstance(fn, exp.Anonymous) else fn.name).lower()
        if name.startswith(BLOCKED_FUNCTION_PREFIXES):
            raise GuardError(f"function {name}() is not allowed")

    cte_names = {c.alias_or_name.lower() for c in tree.find_all(exp.CTE)}
    used: set[str] = set()
    for t in tree.find_all(exp.Table):
        if not isinstance(t.this, exp.Identifier):
            # e.g. FROM read_csv(...) or FROM 'file.parquet'
            raise GuardError("only the listed tables can appear in FROM/JOIN")
        if t.args.get("db") or t.args.get("catalog"):
            raise GuardError(f"schema-qualified table {t.sql()} is not allowed")
        name = t.name.lower()
        if name in cte_names:
            continue
        if name not in allowed_tables:
            raise GuardError(
                f"table '{t.name}' is not available. allowed tables: {', '.join(sorted(allowed_tables))}"
            )
        used.add(name)

    # cap rows; fetch one extra so the caller can tell the result was truncated
    # (set the limit on the query itself so its ORDER BY is honoured)
    if isinstance(tree, exp.Select):
        lim = tree.args.get("limit")
        lim_val = lim.expression if lim is not None else None
        if not (isinstance(lim_val, exp.Literal) and lim_val.is_int and int(lim_val.name) <= max_rows):
            tree = tree.limit(max_rows + 1)
        capped = tree.sql(dialect="duckdb")
    else:
        capped = f"SELECT * FROM ({tree.sql(dialect='duckdb')}) AS _q LIMIT {max_rows + 1}"
    return CheckedSQL(sql=capped, original=text, tables=used)
