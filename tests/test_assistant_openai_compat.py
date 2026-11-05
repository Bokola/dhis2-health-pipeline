"""
the open-weight path, tested against a mock openai-compatible server (no model download)
"""
import json

import httpx

from assistant.agent import Assistant
from assistant.llm_openai import OpenAICompatClient, _convert_messages
from tests.test_assistant import db, gold_dir  # noqa: F401  reuse the miniature gold layer


def server(replies, seen, reject_required=False):
    """mock /v1/chat/completions that returns scripted assistant messages"""
    replies = list(replies)

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if reject_required and body.get("tool_choice") == "required":
            return httpx.Response(400, json={"error": "tool_choice required not supported"})
        seen.append(body)
        return httpx.Response(200, json={
            "choices": [{"message": replies.pop(0)}],
            "usage": {"prompt_tokens": 50, "completion_tokens": 10},
        })
    return httpx.MockTransport(handler)


def call(id_, name, **args):
    return {"id": id_, "type": "function", "function": {"name": name, "arguments": json.dumps(args)}}


def test_full_agent_loop_over_openai_api(db):  # noqa: F811
    con, ctx = db
    seen = []
    final = ("SELECT country, reporting_rate FROM country_reporting_rates "
             "WHERE period = '202510' ORDER BY reporting_rate LIMIT 1")
    transport = server([
        {"role": "assistant", "content": None,
         "tool_calls": [call("c1", "run_sql", sql="SELECT DISTINCT period FROM country_reporting_rates")]},
        {"role": "assistant", "content": "",
         "tool_calls": [call("c2", "submit_answer", sql=final, answer="Uganda, at 50%.")]},
    ], seen)
    client = OpenAICompatClient(base_url="http://mock/v1", transport=transport)
    ans = Assistant(con, ctx, client=client, model="qwen2.5:14b").ask("Lowest reporting rate last month?")

    assert ans.status == "answered" and ans.rows == [("Uganda", 50.0)]
    assert ans.input_tokens == 100
    first, second = seen
    assert first["messages"][0]["role"] == "system" and "service_delivery" in first["messages"][0]["content"]
    assert {t["function"]["name"] for t in first["tools"]} == {"run_sql", "submit_answer", "decline"}
    # the sql result went back as a role=tool message tied to the call id
    tool_msg = second["messages"][-1]
    assert tool_msg["role"] == "tool" and tool_msg["tool_call_id"] == "c1" and "202510" in tool_msg["content"]


def test_recovers_tool_call_written_as_text(db):  # noqa: F811
    con, ctx = db
    text_call = 'Sure. {"name": "decline", "arguments": {"reason": "No stock data here."}}'
    client = OpenAICompatClient(base_url="http://mock/v1",
                                transport=server([{"role": "assistant", "content": text_call}], []))
    ans = Assistant(con, ctx, client=client, model="llama3.1:8b").ask("DMPA stock-outs?")
    assert ans.status == "declined" and "stock" in ans.answer


def test_falls_back_when_server_rejects_required_tool_choice(db):  # noqa: F811
    con, ctx = db
    seen = []
    transport = server([{"role": "assistant", "content": None,
                         "tool_calls": [call("c1", "decline", reason="out of scope")]}],
                       seen, reject_required=True)
    client = OpenAICompatClient(base_url="http://mock/v1", transport=transport)
    ans = Assistant(con, ctx, client=client, model="m", max_steps=1).ask("x")
    assert ans.status == "declined" and seen[0]["tool_choice"] == "auto"


def test_error_tool_results_are_marked():
    msgs = _convert_messages(None, [{"role": "user", "content": [
        {"type": "tool_result", "tool_use_id": "a", "content": "blocked: no", "is_error": True}]}])
    assert msgs == [{"role": "tool", "tool_call_id": "a", "content": "ERROR: blocked: no"}]


def test_preflight_reports_missing_model():
    transport = httpx.MockTransport(lambda req: httpx.Response(200, json={"data": [{"id": "llama3.1:8b"}]}))
    client = OpenAICompatClient(base_url="http://mock/v1", transport=transport)
    assert client.preflight("llama3.1:8b") is None
    assert "not available" in client.preflight("qwen2.5:14b")


def test_retries_short_rate_limit_then_succeeds(monkeypatch):
    import assistant.llm_openai as m
    monkeypatch.setattr(m.time, "sleep", lambda s: None)
    calls = []

    def handler(req):
        calls.append(1)
        if len(calls) == 1:
            return httpx.Response(429, headers={"retry-after": "3"}, json={"error": "slow down"})
        return httpx.Response(200, json={"choices": [{"message": {"role": "assistant", "content": "hi"}}]})
    client = OpenAICompatClient(base_url="http://mock/v1", transport=httpx.MockTransport(handler))
    resp = client.messages.create(model="m", max_tokens=10, messages=[{"role": "user", "content": "x"}])
    assert len(calls) == 2 and resp.content[0]["text"] == "hi"


def test_daily_quota_raises_and_eval_stops(db, tmp_path, monkeypatch):  # noqa: F811
    import assistant.llm_openai as m
    from assistant.eval.run_eval import load_questions, run
    monkeypatch.setattr(m.time, "sleep", lambda s: None)
    msg = "Rate limit of 150 per 86400s exceeded for UserByModelByDay. Please wait 19704 seconds before retrying."
    transport = httpx.MockTransport(lambda req: httpx.Response(429, json={"error": {"message": msg}}))
    client = OpenAICompatClient(base_url="http://mock/v1", transport=transport)
    con, ctx = db
    summary = run(Assistant(con, ctx, client=client, model="m"), load_questions(only={"q01", "q02"}), str(tmp_path))
    assert summary["stopped"] and summary["total"] == 0 and summary["planned"] == 2
    assert "incomplete run" in (tmp_path / "assistant_eval" / "latest" / "report.md").read_text()


def test_preflight_skips_servers_without_model_listing():
    transport = httpx.MockTransport(lambda req: httpx.Response(404, json={"error": "not found"}))
    assert OpenAICompatClient(base_url="http://mock/v1", transport=transport).preflight("any") is None


def test_github_models_picker_prefers_low_tier_open_tool_models():
    from assistant.github_models import fetch_catalog, pick, table
    catalog = [
        {"id": "openai/gpt-4.1", "publisher": "OpenAI", "capabilities": ["tool-calling"], "rate_limit_tier": "high"},
        {"id": "openai/gpt-4.1-mini", "publisher": "OpenAI", "capabilities": ["tool-calling"], "rate_limit_tier": "low"},
        {"id": "meta/llama-3.1-70b", "publisher": "Meta", "capabilities": [], "rate_limit_tier": "low"},
        {"id": "mistral-ai/mistral-small", "publisher": "Mistral AI", "capabilities": ["tool-calling"], "rate_limit_tier": "low"},
    ]
    transport = httpx.MockTransport(lambda req: httpx.Response(200, json=catalog))
    cat = fetch_catalog(transport=transport)
    assert pick(cat) == "mistral-ai/mistral-small"
    assert pick(cat, prefer_open=False) in {"openai/gpt-4.1-mini", "mistral-ai/mistral-small"}  # never the high tier
    assert "llama-3.1-70b" not in table(cat)


def test_catalog_follows_redirects_and_reports_non_json(monkeypatch, capsys):
    import pytest
    from assistant import github_models as gm

    def handler(req):
        if req.url.path == "/catalog/models":
            return httpx.Response(302, headers={"location": "https://models.github.ai/v2/catalog"})
        assert req.headers["x-github-api-version"]
        return httpx.Response(200, json=[{"id": "meta/x", "capabilities": ["tool-calling"], "rate_limit_tier": "low"}])
    assert gm.fetch_catalog(transport=httpx.MockTransport(handler))[0]["id"] == "meta/x"

    empty = httpx.MockTransport(lambda req: httpx.Response(200, text="", headers={"content-type": "text/html"}))
    with pytest.raises(gm.CatalogError, match="non-json"):
        gm.fetch_catalog(transport=empty)

    # explicit model: a broken catalog must not block the run
    def broken(*a, **k):
        raise gm.CatalogError("non-json")
    monkeypatch.setattr(gm, "fetch_catalog", broken)
    assert gm.main(["--model", "openai/gpt-4.1-mini"]) == 0
    assert capsys.readouterr().out.strip() == "openai/gpt-4.1-mini"
    assert gm.main(["--model", "auto"]) == 1
    assert gm.main(["--list"]) == 0
