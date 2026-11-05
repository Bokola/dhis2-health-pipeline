"""
the review step between the model and a final answer, using the scripted fake client
"""
from assistant.agent import Assistant
from assistant.llm_openai import _convert_messages
from assistant.semantic_check import build_vocab, check
from tests.test_assistant import FakeClient, db, gold_dir, tool  # noqa: F401


def test_checker_flags_quarter_in_period_and_unknown_names(db):  # noqa: F811
    con, _ = db
    v = build_vocab(con)
    assert "use quarter = '2025-Q3'" in check("SELECT * FROM service_delivery WHERE period = '2025-Q3'", v)[0]
    assert "does not exist" in check("SELECT * FROM service_delivery WHERE data_element = 'DMPA stock-outs'", v)[0]
    assert "case-sensitive" in check("SELECT * FROM facilities WHERE country = 'uganda'", v)[0]
    assert check("SELECT 'none' AS answer", v)
    assert check("SELECT * FROM service_delivery WHERE quarter = '2025-Q3' AND country IN ('Uganda', 'Kenya')", v) == []


def test_wrong_quarter_filter_is_bounced_then_fixed(db):  # noqa: F811
    con, ctx = db
    bad = "SELECT sum(value) FROM service_delivery WHERE data_element = 'Malaria confirmed cases' AND period = '2025-Q4'"
    good = "SELECT sum(value) FROM service_delivery WHERE data_element = 'Malaria confirmed cases' AND quarter = '2025-Q4'"
    client = FakeClient([
        [tool("submit_answer", "t1", sql=bad, answer="none")],
        [tool("submit_answer", "t2", sql=good, answer="25 cases")],
    ])
    ans = Assistant(con, ctx, client=client).ask("malaria cases this quarter?")
    assert ans.status == "answered" and ans.rows == [(25.0,)]
    feedback = client.calls[1]["messages"][-1]["content"][0]
    assert feedback["is_error"] and "quarter = '2025-Q4'" in feedback["content"]


def test_unknown_indicator_leads_to_decline(db):  # noqa: F811
    con, ctx = db
    client = FakeClient([
        [tool("submit_answer", "t1", sql="SELECT district FROM service_delivery WHERE data_element = 'DMPA stock-outs'",
              answer="No stock-outs.")],
        [tool("decline", "t2", reason="There is no stock data.")],
    ])
    ans = Assistant(con, ctx, client=client).ask("DMPA stock-outs?")
    assert ans.status == "declined"
    assert "call decline" in client.calls[1]["messages"][-1]["content"][0]["content"]


def test_empty_result_gets_one_recheck_then_is_accepted(db):  # noqa: F811
    con, ctx = db
    empty = "SELECT facility_id FROM service_delivery WHERE country = 'Kenya' AND is_explicit_zero"
    client = FakeClient([
        [tool("submit_answer", "t1", sql=empty, answer="none")],
        [tool("submit_answer", "t2", sql=empty, answer="none, confirmed")],
    ])
    ans = Assistant(con, ctx, client=client).ask("kenyan facilities reporting explicit zeros?")
    assert ans.status == "answered" and ans.rows == [] and ans.answer == "none, confirmed"
    assert "returns no rows" in client.calls[1]["messages"][-1]["content"][0]["content"]


def test_bounces_are_capped(db):  # noqa: F811
    con, ctx = db
    bad = "SELECT 1 AS x"
    client = FakeClient([[tool("submit_answer", f"t{i}", sql=bad, answer="x")] for i in range(3)])
    ans = Assistant(con, ctx, client=client, max_bounces=2).ask("q")
    assert ans.status == "answered" and len(client.calls) == 3


def test_tool_only_assistant_turn_has_empty_string_content():
    msgs = _convert_messages(None, [{"role": "assistant", "content": [
        {"type": "tool_use", "id": "a", "name": "run_sql", "input": {"sql": "SELECT 1"}}]}])
    assert msgs[0]["content"] == "" and msgs[0]["tool_calls"][0]["id"] == "a"


def test_quick_set_covers_each_behaviour():
    from assistant.eval.run_eval import load_questions
    quick = load_questions(tags={"quick"})
    assert len(quick) == 10
    tags = {t for q in quick for t in q["tags"]}
    assert {"relative-date", "percentage", "ordered", "change", "stock", "injection", "synonym"} <= tags
    assert any(q["expect"] == "rows" for q in quick) and any(q["expect"] == "decline" for q in quick)
