"""
run the question set through the assistant and score it

    python -m assistant eval --output-dir ./output --model claude-sonnet-5-5
    python -m assistant eval --only q03,q28 --repeats 3

writes output/assistant_eval/<timestamp>/results.jsonl and report.md
"""
from __future__ import annotations

import json
import os
import statistics
import time
from collections import defaultdict
from pathlib import Path

import yaml

from assistant.agent import Answer, Assistant, run_query
from assistant.eval.scoring import results_match

QUESTIONS_PATH = Path(__file__).with_name("questions.yaml")


def load_questions(path: Path = QUESTIONS_PATH, only: set[str] | None = None) -> list[dict]:
    qs = yaml.safe_load(path.read_text())["questions"]
    return [q for q in qs if not only or q["id"] in only]


def score(q: dict, ans: Answer, con) -> tuple[bool, str]:
    expect = q["expect"]
    if expect == "decline":
        return ans.status == "declined", f"status={ans.status}"
    if expect == "decline_or_empty":
        ok = ans.status == "declined" or (ans.status == "answered" and not ans.rows)
        return ok, f"status={ans.status} rows={len(ans.rows)}"
    if ans.status != "answered":
        return False, f"status={ans.status}"
    _, gold_rows, _, _ = run_query(con, q["gold_sql"])
    ok = results_match(ans.rows, gold_rows, ordered=q.get("ordered", False))
    return ok, "match" if ok else f"mismatch: got {len(ans.rows)} rows, gold {len(gold_rows)}"


def run(assistant: Assistant, questions: list[dict], out_root: str, repeats: int = 1) -> dict:
    stamp = time.strftime("%Y%m%d-%H%M%S")
    out_dir = Path(out_root) / "assistant_eval" / stamp
    out_dir.mkdir(parents=True, exist_ok=True)
    results = []
    with open(out_dir / "results.jsonl", "w") as fh:
        for q in questions:
            for rep in range(repeats):
                try:
                    ans = assistant.ask(q["question"])
                except Exception as e:  # api errors etc: count as a failure, keep going
                    ans = Answer(question=q["question"], status="failed", answer=f"error: {e}")
                ok, detail = score(q, ans, assistant.con)
                guard_blocks = sum(1 for t in ans.trace if str(t.get("error", "")).startswith("guard"))
                rec = {"id": q["id"], "rep": rep, "tags": q.get("tags", []), "expect": q["expect"],
                       "correct": ok, "detail": detail, "guard_blocks": guard_blocks, **ans.to_dict()}
                results.append(rec)
                fh.write(json.dumps(rec, default=str) + "\n")
                print(f"{q['id']} rep{rep} {'PASS' if ok else 'FAIL'} {detail} ({ans.seconds}s, {ans.steps} steps)")

    summary = summarise(results, assistant.model)
    (out_dir / "report.md").write_text(render_report(summary, results))
    print(f"\n{summary['correct']}/{summary['total']} correct ({summary['accuracy']:.0%}). report: {out_dir / 'report.md'}")
    return summary


def summarise(results: list[dict], model: str) -> dict:
    by_tag: dict[str, list[bool]] = defaultdict(list)
    for r in results:
        for t in r["tags"]:
            by_tag[t].append(r["correct"])
    answerable = [r for r in results if r["expect"] == "rows"]
    refusals = [r for r in results if r["expect"] != "rows"]
    secs = [r["seconds"] for r in results if r["seconds"]]
    return {
        "model": model,
        "total": len(results),
        "correct": sum(r["correct"] for r in results),
        "accuracy": (sum(r["correct"] for r in results) / len(results)) if results else 0.0,
        "answerable_accuracy": (sum(r["correct"] for r in answerable) / len(answerable)) if answerable else None,
        "decline_accuracy": (sum(r["correct"] for r in refusals) / len(refusals)) if refusals else None,
        "false_declines": sum(1 for r in answerable if r["status"] == "declined"),
        "failed_runs": sum(1 for r in results if r["status"] == "failed"),
        "guard_blocks": sum(r["guard_blocks"] for r in results),
        "median_seconds": statistics.median(secs) if secs else None,
        "mean_steps": statistics.mean(r["steps"] for r in results) if results else None,
        "input_tokens": sum(r["input_tokens"] for r in results),
        "output_tokens": sum(r["output_tokens"] for r in results),
        "by_tag": {t: (sum(v), len(v)) for t, v in sorted(by_tag.items())},
    }


def _pct(x):
    return "n/a" if x is None else f"{x:.0%}"


def render_report(s: dict, results: list[dict]) -> str:
    lines = [
        "# ask-the-data assistant evaluation", "",
        f"model: `{s['model']}`", "",
        "| metric | value |", "|---|---|",
        f"| overall | {s['correct']}/{s['total']} ({_pct(s['accuracy'])}) |",
        f"| answerable questions, execution accuracy | {_pct(s['answerable_accuracy'])} |",
        f"| out-of-scope / safety, correct decline | {_pct(s['decline_accuracy'])} |",
        f"| false declines on answerable questions | {s['false_declines']} |",
        f"| runs that failed outright | {s['failed_runs']} |",
        f"| sql attempts blocked by the guard | {s['guard_blocks']} |",
        f"| median seconds per question | {s['median_seconds']} |",
        f"| mean model calls per question | {s['mean_steps']:.1f} |" if s["mean_steps"] else "",
        f"| tokens in / out | {s['input_tokens']:,} / {s['output_tokens']:,} |",
        "", "## by tag", "", "| tag | correct |", "|---|---|",
    ]
    lines += [f"| {t} | {c}/{n} |" for t, (c, n) in s["by_tag"].items()]
    lines += ["", "## failures", ""]
    fails = [r for r in results if not r["correct"]]
    if not fails:
        lines.append("none")
    for r in fails:
        lines += [f"### {r['id']} (rep {r['rep']}): {r['question']}", "",
                  f"- {r['detail']}", f"- answer: {r['answer'][:300]}",
                  f"- sql: `{(r['sql'] or '').strip()[:400]}`", ""]
    return "\n".join(lines)
