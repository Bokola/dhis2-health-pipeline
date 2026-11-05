"""
pick or validate a github models model for the assistant

    python -m assistant.github_models                 # print the chosen model id
    python -m assistant.github_models --model X       # validate X, exit 1 with a listing if unusable
    python -m assistant.github_models --list          # markdown table of tool-calling models

the agent needs tool calling, and a full eval needs ~60-100 requests, which fits the
free daily quota of "low" tier models but not "high" tier ones
"""
from __future__ import annotations

import argparse
import os
import sys

import httpx

CATALOG_URL = "https://models.github.ai/catalog/models"
OPEN_WEIGHT_PUBLISHERS = ("meta", "mistral", "microsoft", "deepseek", "cohere", "ai21", "qwen", "alibaba", "nvidia")


class CatalogError(RuntimeError):
    pass


def fetch_catalog(token: str | None = None, transport: httpx.BaseTransport | None = None) -> list[dict]:
    headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    with httpx.Client(timeout=30, transport=transport, follow_redirects=True) as c:
        r = c.get(CATALOG_URL, headers=headers)
    body = r.text or ""
    if r.status_code >= 400:
        raise CatalogError(f"catalog request failed: HTTP {r.status_code}: {body[:300]}")
    try:
        data = r.json()
    except ValueError:
        raise CatalogError(
            f"catalog returned non-json (HTTP {r.status_code}, content-type "
            f"{r.headers.get('content-type', '?')}, {len(body)} bytes): {body[:200]!r}") from None
    items = data if isinstance(data, list) else (data.get("data") or data.get("models") or [])
    if not isinstance(items, list):
        raise CatalogError(f"unexpected catalog shape: {str(data)[:200]}")
    return items


def tool_models(catalog: list[dict]) -> list[dict]:
    return [m for m in catalog if "tool-calling" in (m.get("capabilities") or [])]


def _is_open(m: dict) -> bool:
    who = f"{m.get('publisher', '')} {m.get('id', '')}".lower()
    return any(p in who for p in OPEN_WEIGHT_PUBLISHERS)


def pick(catalog: list[dict], prefer_open: bool = True) -> str | None:
    usable = tool_models(catalog)
    tier_rank = {"low": 0, "medium": 1, "high": 2}
    usable.sort(key=lambda m: (tier_rank.get(m.get("rate_limit_tier", ""), 3),
                               0 if (prefer_open and _is_open(m)) else 1,
                               m.get("id", "")))
    return usable[0]["id"] if usable else None


def table(catalog: list[dict]) -> str:
    rows = ["| model id | tier | open weights |", "|---|---|---|"]
    for m in sorted(tool_models(catalog), key=lambda m: (m.get("rate_limit_tier", ""), m.get("id", ""))):
        rows.append(f"| `{m.get('id')}` | {m.get('rate_limit_tier', '?')} | {'yes' if _is_open(m) else 'no'} |")
    return "\n".join(rows)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--model", default="", help="validate this id instead of picking ('auto' = pick)")
    p.add_argument("--list", action="store_true")
    p.add_argument("--any-publisher", action="store_true", help="don't prefer open-weight models")
    args = p.parse_args(argv)

    explicit = args.model and args.model != "auto"
    try:
        catalog = fetch_catalog(os.environ.get("GITHUB_TOKEN") or os.environ.get("OPENAI_API_KEY"))
    except (CatalogError, httpx.HTTPError) as e:
        if args.list:
            print(f"_could not read the github models catalog: {e}_")
            return 0
        if explicit:
            # the inference call itself will tell us if the id is wrong
            print(f"warning: {e}. using {args.model} without checking it", file=sys.stderr)
            print(args.model)
            return 0
        print(f"{e}\ncould not pick a model automatically. re-run with a model id, e.g. "
              f"model: openai/gpt-4.1-mini (see https://github.com/marketplace?type=models)", file=sys.stderr)
        return 1
    if args.list:
        print(table(catalog))
        return 0
    if args.model and args.model != "auto":
        found = {m.get("id"): m for m in catalog}.get(args.model)
        if found and "tool-calling" in (found.get("capabilities") or []):
            if found.get("rate_limit_tier") == "high":
                print(f"warning: {args.model} is a high-tier model; the free daily quota may not cover a full eval",
                      file=sys.stderr)
            print(args.model)
            return 0
        why = "is not in the catalog" if not found else "does not support tool calling"
        print(f"{args.model} {why}. tool-calling models:\n{table(catalog)}", file=sys.stderr)
        return 1
    chosen = pick(catalog, prefer_open=not args.any_publisher)
    if not chosen:
        print("no tool-calling models found in the github models catalog", file=sys.stderr)
        return 1
    print(chosen)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
