"""
ask-the-data assistant: question in, answer + sql + caveats out

the model gets three tools:
  run_sql        explore and test queries (guarded, row-capped)
  submit_answer  final sql + plain-language answer + caveats
  decline        the data cannot answer this (e.g. stock levels, which are not in this warehouse)

the final sql is re-run by us, not trusted from the model, and deterministic
caveats are added on top of whatever the model wrote
"""
from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any

import duckdb

from assistant.catalog import ALLOWED_TABLES, DataContext, schema_prompt
from assistant.sql_guard import GuardError, check_sql

DEFAULT_MODEL = os.environ.get("ASSISTANT_MODEL", "claude-sonnet-5-5")
MAX_ROWS = 200
ROWS_SHOWN_TO_MODEL = 40

SYSTEM_TEMPLATE = """You answer questions from health programme staff using a DHIS2 service-delivery warehouse.
Your users are not technical. You work by writing DuckDB SQL against the tables below, running it, and explaining the result.

{schema}

RULES
1. Only use the tables and columns above. Use run_sql to check names or values before your final query when unsure
   (e.g. SELECT DISTINCT district FROM facilities WHERE country = 'Uganda').
2. Relative dates are relative to the data, not today: "last month" = {last_period}, "last quarter" = {last_quarter}
   (the latest complete quarter), "this year" = {last_year}.
3. NULL value means not reported. Never treat it as zero. Use is_explicit_zero for reported zeros.
4. Never add up PERCENTAGE data elements; average them or report them per row.
5. Sum across category_option_combo to get totals. Group facilities by facility_id, not name.
6. Match the user's wording to the closest data element name (e.g. "ART enrolment" -> 'ART patients enrolled').
   If the mapping is genuinely ambiguous, say which one you used.
7. If the question needs data that is not in these tables (stock levels, stock-outs, commodities, populations,
   targets, budgets, individual patients, periods outside the range), call decline. Do not approximate with other data.
8. Finish with exactly one call to submit_answer or decline. The sql in submit_answer must, on its own,
   return the rows that answer the question, with readable column names.
9. In the answer, lead with the direct answer in one or two sentences of plain language, with the key numbers.
   Put data limitations in caveats, not in the answer.
"""

TOOLS: list[dict[str, Any]] = [
    {
        "name": "run_sql",
        "description": "Run one read-only DuckDB SELECT against the allowed tables. Returns up to "
                       f"{ROWS_SHOWN_TO_MODEL} rows, the total row count, or an error to fix.",
        "input_schema": {
            "type": "object",
            "properties": {"sql": {"type": "string"}},
            "required": ["sql"],
        },
    },
    {
        "name": "submit_answer",
        "description": "Final answer. sql must reproduce the answer rows by itself.",
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "final SELECT that returns the answer rows"},
                "answer": {"type": "string", "description": "plain-language answer for non-technical staff"},
                "caveats": {"type": "array", "items": {"type": "string"},
                            "description": "data limitations that affect this answer"},
            },
            "required": ["sql", "answer"],
        },
    },
    {
        "name": "decline",
        "description": "Use when the tables cannot answer the question. Explain what data would be needed.",
        "input_schema": {
            "type": "object",
            "properties": {"reason": {"type": "string"}},
            "required": ["reason"],
        },
    },
]


@dataclass
class Answer:
    question: str
    status: str                      # "answered" | "declined" | "failed"
    answer: str = ""
    sql: str | None = None
    columns: list[str] = field(default_factory=list)
    rows: list[tuple] = field(default_factory=list)
    truncated: bool = False
    caveats: list[str] = field(default_factory=list)
    trace: list[dict] = field(default_factory=list)  # every sql attempt and its outcome
    steps: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    seconds: float = 0.0

    def to_dict(self) -> dict:
        d = self.__dict__.copy()
        d["rows"] = [list(r) for r in self.rows]
        return d


def run_query(con: duckdb.DuckDBPyConnection, sql: str) -> tuple[list[str], list[tuple], bool, str]:
    """guard + execute. returns columns, rows, truncated, executed_sql. raises GuardError or duckdb.Error"""
    checked = check_sql(sql, ALLOWED_TABLES, max_rows=MAX_ROWS)
    cur = con.execute(checked.sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    truncated = len(rows) > MAX_ROWS
    return cols, rows[:MAX_ROWS], truncated, checked.sql


def _jsonable(v: Any) -> Any:
    if isinstance(v, float):
        return round(v, 4)
    if isinstance(v, (int, str, bool)) or v is None:
        return v
    return str(v)


def deterministic_caveats(ctx: DataContext, sql: str, columns: list[str], rows: list[tuple],
                          truncated: bool) -> list[str]:
    """caveats we can detect from the query and result without trusting the model"""
    out = []
    flat = {str(v) for r in rows for v in r}
    touched_recent = [p for p in ctx.recent_periods if p in sql or p in flat]
    last_q = f"{ctx.last_period[:4]}-Q{(int(ctx.last_period[4:]) - 1) // 3 + 1}"
    if touched_recent or (last_q != ctx.latest_complete_quarter and (last_q in sql or last_q in flat)):
        out.append(
            f"The most recent months ({', '.join(sorted(ctx.recent_periods))}) may still be filling in: "
            f"across the dataset {ctx.late_share_pct}% of values arrive more than 60 days after the month ends."
        )
    if re.search(r"\b(sum|avg)\s*\(\s*\w*\.?value\b", sql, re.IGNORECASE):
        out.append("Totals and averages only include values that were reported; missing reports are excluded, not counted as zero.")
    if not rows:
        out.append("No rows matched. Check spelling of place or indicator names, or the time period.")
    if truncated:
        out.append(f"Only the first {MAX_ROWS} rows are shown.")
    return out


class Assistant:
    def __init__(self, con: duckdb.DuckDBPyConnection, ctx: DataContext, client: Any = None,
                 model: str = DEFAULT_MODEL, max_steps: int = 6, max_tokens: int = 2048):
        if client is None:
            import anthropic  # imported lazily so tests and the guard need no api key
            client = anthropic.Anthropic()
        self.con, self.ctx, self.client = con, ctx, client
        self.model, self.max_steps, self.max_tokens = model, max_steps, max_tokens
        self.system = SYSTEM_TEMPLATE.format(
            schema=schema_prompt(ctx),
            last_period=ctx.last_period,
            last_quarter=ctx.latest_complete_quarter,
            last_year=ctx.last_period[:4],
        )

    def ask(self, question: str) -> Answer:
        t0 = time.time()
        ans = Answer(question=question, status="failed")
        messages: list[dict] = [{"role": "user", "content": question}]

        for step in range(self.max_steps):
            ans.steps = step + 1
            final_turn = step == self.max_steps - 1
            resp = self.client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                system=[{"type": "text", "text": self.system, "cache_control": {"type": "ephemeral"}}],
                tools=TOOLS,
                # on the last step force a decision instead of more exploring
                tool_choice={"type": "any"} if final_turn else {"type": "auto"},
                messages=messages,
            )
            usage = getattr(resp, "usage", None)
            if usage is not None:
                ans.input_tokens += getattr(usage, "input_tokens", 0) or 0
                ans.output_tokens += getattr(usage, "output_tokens", 0) or 0

            content = [b.model_dump(exclude_none=True) if hasattr(b, "model_dump") else b for b in resp.content]
            messages.append({"role": "assistant", "content": content})
            tool_uses = [b for b in content if b.get("type") == "tool_use"]

            if not tool_uses:
                # model replied in prose without a tool: nudge it once more
                messages.append({"role": "user", "content": "Please finish with submit_answer or decline."})
                continue

            results = []
            for tu in tool_uses:
                name, inp = tu["name"], tu.get("input", {}) or {}
                if name == "decline":
                    ans.status = "declined"
                    ans.answer = inp.get("reason", "")
                    ans.seconds = round(time.time() - t0, 2)
                    return ans
                if name == "submit_answer":
                    return self._finalise(ans, inp, t0)
                if name == "run_sql":
                    results.append({"type": "tool_result", "tool_use_id": tu["id"],
                                    **self._tool_run_sql(ans, inp.get("sql", ""))})
                else:
                    results.append({"type": "tool_result", "tool_use_id": tu["id"],
                                    "content": f"unknown tool {name}", "is_error": True})
            messages.append({"role": "user", "content": results})

        ans.answer = "I could not reach an answer within the step limit."
        ans.seconds = round(time.time() - t0, 2)
        return ans

    def _tool_run_sql(self, ans: Answer, sql: str) -> dict:
        try:
            cols, rows, truncated, _ = run_query(self.con, sql)
        except GuardError as e:
            ans.trace.append({"sql": sql, "error": f"guard: {e}"})
            return {"content": f"blocked: {e}", "is_error": True}
        except duckdb.Error as e:
            msg = str(e).splitlines()[0][:400]
            ans.trace.append({"sql": sql, "error": msg})
            return {"content": f"sql error: {msg}", "is_error": True}
        ans.trace.append({"sql": sql, "rows": len(rows)})
        payload = {
            "columns": cols,
            "rows": [[_jsonable(v) for v in r] for r in rows[:ROWS_SHOWN_TO_MODEL]],
            "row_count": len(rows) if not truncated else f">{MAX_ROWS}",
        }
        return {"content": json.dumps(payload)}

    def _finalise(self, ans: Answer, inp: dict, t0: float) -> Answer:
        sql = inp.get("sql", "")
        ans.sql = sql
        try:
            cols, rows, truncated, _ = run_query(self.con, sql)
        except (GuardError, duckdb.Error) as e:
            ans.status = "failed"
            ans.answer = f"The final query could not run: {str(e).splitlines()[0]}"
            ans.trace.append({"sql": sql, "error": str(e)[:400], "final": True})
            ans.seconds = round(time.time() - t0, 2)
            return ans
        ans.status = "answered"
        ans.answer = inp.get("answer", "")
        ans.columns, ans.rows, ans.truncated = cols, rows, truncated
        model_caveats = [c for c in (inp.get("caveats") or []) if c and c.strip()]
        ans.caveats = model_caveats + deterministic_caveats(self.ctx, sql, cols, rows, truncated)
        ans.trace.append({"sql": sql, "rows": len(rows), "final": True})
        ans.seconds = round(time.time() - t0, 2)
        return ans
