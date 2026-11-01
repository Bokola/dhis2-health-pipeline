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
