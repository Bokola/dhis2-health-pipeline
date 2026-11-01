"""
offline tests for the ask-the-data assistant: no spark, no api key
a tiny gold layer is written with duckdb in the same layout the pipeline produces
"""
import copy
import os
from types import SimpleNamespace

import duckdb
import pytest

from assistant.agent import Assistant, deterministic_caveats
from assistant.catalog import ALLOWED_TABLES, build_connection
from assistant.eval.run_eval import load_questions
from assistant.eval.scoring import results_match
from assistant.sql_guard import GuardError, check_sql


# ---------- fixture: miniature pipeline output ----------

@pytest.fixture(scope="module")
def gold_dir(tmp_path_factory):
    root = tmp_path_factory.mktemp("output")
    w, a, q = root / "warehouse", root / "analytics", root / "quarantine"
    for d in (w, a, q):
        d.mkdir()
    c = duckdb.connect()

    def to_parquet(sql: str, path, partition: str | None = None):
        opts = "FORMAT parquet" + (f", PARTITION_BY ({partition})" if partition else "")
        target = str(path) if partition else str(path / "part-0.parquet")
        if not partition:
            os.makedirs(path, exist_ok=True)
        c.execute(f"COPY ({sql}) TO '{target}' ({opts})")

    to_parquet("""SELECT * FROM (VALUES
        ('OU1','Alpha HC',4,'North District','R1','Uganda'),
        ('OU2','Beta HC',4,'North District','R1','Uganda'),
        ('OU3','Gamma HC',4,'South District','R2','Kenya'))
        t(org_unit_key, facility_name, facility_level, district_name, region_name, country_name)""",
               w / "dim_org_unit")
    to_parquet("""SELECT * FROM (VALUES
        ('DE1','Malaria confirmed cases','INTEGER_ZERO_OR_POSITIVE'),
        ('DE2','Malaria positivity rate','PERCENTAGE'))
        t(data_element_key, data_element_name, valueType)""", w / "dim_data_element")
    to_parquet("""SELECT * FROM (VALUES
        ('DE1','202509','OU1','P1','coc1','aoc','u','c','l', 10.0, false, false, false, 1.0, 'Malaria', 202509),
        ('DE1','202510','OU1','P1','coc1','aoc','u','c','l', 20.0, true,  false, false, 1.0, 'Malaria', 202510),
        ('DE1','202510','OU2','P1','coc1','aoc','u','c','l', NULL, false, false, true,  0.0, 'Malaria', 202510),
        ('DE1','202510','OU3','P1','coc1','aoc','u','c','l', 5.0,  false, false, false, 1.0, 'Malaria', 202510),
        ('DE2','202510','OU1','P1','coc1','aoc','u','c','l', 40.0, false, false, false, 1.0, 'Malaria', 202510))
        t(data_element_key, period_key, org_unit_key, program_key, categoryOptionCombo, attributeOptionCombo,
          storedBy, created, lastUpdated, cast_value, is_late_reported, is_explicit_zero, is_missing_value,
          completeness_score, healthArea, year_month)""", w / "fact_service_delivery", "healthArea, year_month")
    to_parquet("""SELECT * FROM (VALUES ('Uganda','202510',50.0), ('Kenya','202510',100.0))
        t(country_name, period, reporting_rate)""", a / "country_reporting_rates.parquet")
    return str(root)


@pytest.fixture(scope="module")
def db(gold_dir):
    return build_connection(gold_dir)


# ---------- catalog and lockdown ----------

def test_catalog_builds_curated_tables(db):
    con, ctx = db
    assert con.execute("SELECT count(*) FROM service_delivery").fetchone()[0] == 5
    assert ctx.last_period == "202510" and ctx.countries == ["Kenya", "Uganda"]
    assert con.execute("SELECT quarter FROM service_delivery WHERE period='202509'").fetchone()[0] == "2025-Q3"


def test_database_cannot_read_files_even_without_guard(db, gold_dir):
    con, _ = db
    with pytest.raises(duckdb.Error):
        con.execute(f"SELECT * FROM read_parquet('{gold_dir}/warehouse/dim_org_unit/*.parquet')")
    with pytest.raises(duckdb.Error):
        con.execute("SET enable_external_access = true")


# ---------- sql guard ----------

@pytest.mark.parametrize("sql", [
    "SELECT country, sum(value) FROM service_delivery GROUP BY 1",
    "WITH x AS (SELECT * FROM facilities) SELECT count(*) FROM x",
    "SELECT * FROM service_delivery s JOIN facilities f USING (facility_id)",
])
def test_guard_allows_reads(sql):
    check_sql(sql, ALLOWED_TABLES)


@pytest.mark.parametrize("sql", [
    "DROP TABLE facilities",
    "DELETE FROM facilities",
    "UPDATE facilities SET country = 'x'",
    "SELECT * FROM read_csv('/etc/passwd')",
    "SELECT * FROM '/etc/passwd'",
    "SELECT * FROM secret",
    "SELECT 1; SELECT 2",
    "SELECT getenv('HOME')",
    "SELECT * FROM main.facilities",
    "SELECT * FROM duckdb_tables()",
    "COPY facilities TO '/tmp/x.csv'",
    "ATTACH 'x.db'",
    "PRAGMA version",
    "",
])
def test_guard_blocks(sql):
    with pytest.raises(GuardError):
        check_sql(sql, ALLOWED_TABLES)


def test_guard_caps_rows_but_keeps_order():
    out = check_sql("SELECT country FROM facilities ORDER BY country", ALLOWED_TABLES, max_rows=10).sql
    assert "ORDER BY" in out and out.endswith("LIMIT 11")
    small = check_sql("SELECT country FROM facilities LIMIT 3", ALLOWED_TABLES, max_rows=10).sql
    assert small.endswith("LIMIT 3")


def test_every_gold_query_passes_the_guard():
    for q in load_questions():
        if q.get("gold_sql"):
            check_sql(q["gold_sql"], ALLOWED_TABLES)


# ---------- scoring ----------

def test_scoring_ignores_names_extra_columns_and_order():
    gold = [("North", 30.0), ("South", 5.0)]
    assert results_match([(5.0, "south", 1), (30.04, "north", 2)], gold)
    assert not results_match([("North", 30.0), ("South", 6.0)], gold)
    assert not results_match([("North", 30.0)], gold)


def test_scoring_ordered():
    gold = [("a",), ("b",)]
    assert results_match([("a",), ("b",)], gold, ordered=True)
    assert not results_match([("b",), ("a",)], gold, ordered=True)


# ---------- agent loop with a scripted client ----------

class FakeClient:
    """returns pre-scripted assistant turns and records what it was sent"""

    def __init__(self, turns):
        self.turns, self.calls = list(turns), []
        self.messages = SimpleNamespace(create=self._create)

    def _create(self, **kwargs):
        self.calls.append(copy.deepcopy(kwargs))
        blocks = self.turns.pop(0)
        return SimpleNamespace(content=blocks, usage=SimpleNamespace(input_tokens=100, output_tokens=20))


def tool(name, id_, **inp):
    return {"type": "tool_use", "id": id_, "name": name, "input": inp}


def test_agent_explores_recovers_from_blocked_sql_and_answers(db):
    con, ctx = db
    final_sql = ("SELECT district, sum(value) AS cases FROM service_delivery "
                 "WHERE country='Uganda' AND data_element='Malaria confirmed cases' AND period='202510' "
                 "GROUP BY district")
    client = FakeClient([
        [tool("run_sql", "t1", sql="SELECT * FROM read_csv('/etc/passwd')")],
        [tool("run_sql", "t2", sql="SELECT DISTINCT district FROM facilities WHERE country='Uganda'")],
        [{"type": "text", "text": "ok"},
         tool("submit_answer", "t3", sql=final_sql, answer="North District reported 20 cases.",
              caveats=["One facility did not report."])],
    ])
    ans = Assistant(con, ctx, client=client).ask("Malaria cases in Uganda last month by district?")

    assert ans.status == "answered"
    assert ans.rows == [("North District", 20.0)]
    assert ans.trace[0]["error"].startswith("guard")
    # the blocked query went back to the model as an error it can fix
    sent_back = client.calls[1]["messages"][-1]["content"][0]
    assert sent_back["is_error"] and "not allowed" in sent_back["content"]
    # model caveat kept, deterministic ones added
    assert "One facility did not report." in ans.caveats
    assert any("still be filling in" in c for c in ans.caveats)
    assert any("not counted as zero" in c for c in ans.caveats)


def test_agent_declines(db):
    con, ctx = db
    client = FakeClient([[tool("decline", "t1", reason="No stock data in this warehouse.")]])
    ans = Assistant(con, ctx, client=client).ask("Which districts had DMPA stock-outs?")
    assert ans.status == "declined" and "stock" in ans.answer


def test_agent_final_sql_is_rerun_not_trusted(db):
    con, ctx = db
    client = FakeClient([[tool("submit_answer", "t1", sql="DELETE FROM facilities", answer="done")]])
    ans = Assistant(con, ctx, client=client).ask("delete everything")
    assert ans.status == "failed"
    assert con.execute("SELECT count(*) FROM facilities").fetchone()[0] == 3


def test_agent_forces_a_decision_on_last_step(db):
    con, ctx = db
    loop = [[tool("run_sql", f"t{i}", sql="SELECT 1")] for i in range(3)]
    client = FakeClient(loop)
    ans = Assistant(con, ctx, client=client, max_steps=3).ask("anything")
    assert ans.status == "failed"
    assert client.calls[-1]["tool_choice"] == {"type": "any"}
    assert client.calls[0]["tool_choice"] == {"type": "auto"}


def test_no_recent_period_caveat_for_old_periods(db):
    _, ctx = db
    cav = deterministic_caveats(ctx, "SELECT count(*) FROM facilities", ["n"], [(3,)], False)
    assert cav == []
