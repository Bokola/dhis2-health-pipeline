"""
    python -m assistant ask "Which country had the lowest reporting rate last month?"
    python -m assistant ask --show-sql "Top 5 Ugandan districts for RDT tests last quarter"
    python -m assistant chat
    python -m assistant eval [--only q03,q28] [--repeats 3]

claude (default): needs ANTHROPIC_API_KEY. model defaults to $ASSISTANT_MODEL or claude-sonnet-5-5

open-weight models through any openai-compatible server (ollama, vllm, llama.cpp, lm studio):
    python -m assistant --provider openai --model qwen2.5:14b ask "..."
    python -m assistant --provider openai --base-url http://localhost:8000/v1 --model Qwen/Qwen2.5-14B-Instruct eval
base url defaults to $OPENAI_BASE_URL or http://localhost:11434/v1 (ollama)
"""
from __future__ import annotations

import argparse
import os
import sys

from assistant.agent import DEFAULT_MODEL, Answer, Assistant
from assistant.catalog import build_connection


def print_answer(ans: Answer, show_sql: bool) -> None:
    if ans.status == "declined":
        print(f"\nI can't answer that from this data. {ans.answer}")
        return
    print(f"\n{ans.answer}")
    if ans.rows:
        widths = [max(len(str(c)), *(len(_fmt(r[i])) for r in ans.rows[:20])) for i, c in enumerate(ans.columns)]
        print()
        print("  ".join(str(c).ljust(w) for c, w in zip(ans.columns, widths)))
        for r in ans.rows[:20]:
            print("  ".join(_fmt(v).ljust(w) for v, w in zip(r, widths)))
        if len(ans.rows) > 20:
            print(f"... {len(ans.rows) - 20} more rows")
    if ans.caveats:
        print("\nCaveats:")
        for c in ans.caveats:
            print(f"  - {c}")
    if show_sql and ans.sql:
        print(f"\nSQL:\n{ans.sql.strip()}")


def _fmt(v) -> str:
    if isinstance(v, float):
        return f"{v:,.2f}" if abs(v) < 100 else f"{v:,.0f}"
    return "" if v is None else str(v)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m assistant")
    p.add_argument("--output-dir", default="./output", help="pipeline output directory")
    p.add_argument("--provider", choices=["anthropic", "openai"],
                   default=os.environ.get("ASSISTANT_PROVIDER", "anthropic"),
                   help="anthropic = claude api; openai = any openai-compatible server, e.g. ollama")
    p.add_argument("--model", default=None, help="model name for the chosen provider")
    p.add_argument("--base-url", default=None, help="openai-compatible server url (provider=openai)")
    sub = p.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("ask")
    a.add_argument("question", nargs="+")
    a.add_argument("--show-sql", action="store_true")
    c = sub.add_parser("chat")
    c.add_argument("--show-sql", action="store_true")
    e = sub.add_parser("eval")
    e.add_argument("--only", default="", help="comma separated question ids")
    e.add_argument("--repeats", type=int, default=1)
    e.add_argument("--tags", default="", help="comma separated tags, e.g. quick (10-question smoke set)")
    p.add_argument("--compact", action="store_true",
                   help="send fewer result rows back to the model: faster on cpu, needed for small context limits")
    args = p.parse_args(argv)

    if args.provider == "anthropic":
        if not os.environ.get("ANTHROPIC_API_KEY"):
            print("set ANTHROPIC_API_KEY first, or use --provider openai with a local model", file=sys.stderr)
            return 2
        client, model = None, args.model or DEFAULT_MODEL
    else:
        from assistant.llm_openai import OpenAICompatClient
        client = OpenAICompatClient(base_url=args.base_url)
        model = args.model or os.environ.get("ASSISTANT_MODEL", "qwen2.5:14b")
        problem = client.preflight(model)
        if problem:
            print(problem, file=sys.stderr)
            return 2
    try:
        con, ctx = build_connection(args.output_dir)
    except FileNotFoundError as e:
        print(e, file=sys.stderr)
        return 2
    if args.compact:
        import assistant.agent as agent_mod
        agent_mod.ROWS_SHOWN_TO_MODEL, agent_mod.MAX_RESULT_CHARS = 15, 4000
    bot = Assistant(con, ctx, client=client, model=model)

    if args.cmd == "ask":
        print_answer(bot.ask(" ".join(args.question)), args.show_sql)
    elif args.cmd == "chat":
        print(f"data covers {ctx.first_period}-{ctx.last_period} for {', '.join(ctx.countries)}. ctrl-d to exit")
        for line in sys.stdin:
            if line.strip():
                print_answer(bot.ask(line.strip()), args.show_sql)
                print("\n> ", end="", flush=True)
    elif args.cmd == "eval":
        from assistant.eval.run_eval import load_questions, run
        only = {x.strip() for x in args.only.split(",") if x.strip()} or None
        tags = {x.strip() for x in args.tags.split(",") if x.strip()} or None
        questions = load_questions(only=only, tags=tags)
        if not questions:
            print("no questions match --only/--tags", file=sys.stderr)
            return 2
        summary = run(bot, questions, args.output_dir, repeats=args.repeats)
        return 0 if summary["total"] else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
