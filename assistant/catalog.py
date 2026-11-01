"""
read-only semantic layer over the pipeline's gold outputs

the assistant never sees parquet paths. it sees a handful of curated tables
with plain-language column descriptions. data is copied into an in-memory
duckdb database and external file access is then switched off and locked,
so even a query that slips past the sql guard cannot read anything else
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

import duckdb


@dataclass
class TableSpec:
    name: str
    description: str
    columns: dict[str, str]  # column -> description


# the only tables the llm may query: keep this list short and well described
TABLES: list[TableSpec] = [
    TableSpec(
        "service_delivery",
        "one row per reported value: facility x data element x month x category option combo. "
        "fact_service_delivery already joined to facility, data element and period dimensions. "
        "deduplicated: only the latest submission for each key is kept",
        {
            "country": "country name, one of the countries listed under data context",
            "region": "region name",
            "district": "district name. district names are only unique within a country",
            "facility_id": "dhis2 org unit uid of the facility. facility names are NOT unique, group by facility_id",
            "facility": "facility name",
            "health_area": "programme area: Malaria, HIV, Tuberculosis, Reproductive Health, Child Health",
            "data_element_id": "dhis2 data element uid",
            "data_element": "indicator name, e.g. 'Malaria confirmed cases'",
            "value_type": "INTEGER_ZERO_OR_POSITIVE (a count) or PERCENTAGE (a rate)",
            "category_option_combo": "disaggregation uid (age/sex etc). sum across it to get totals",
            "period": "reporting month as 'YYYYMM' text",
            "year": "integer year",
            "month": "integer month 1-12",
            "quarter": "e.g. '2025-Q3'",
            "value": "numeric value. NULL means nothing was reported, it is NOT zero",
            "is_missing_value": "true when the facility submitted the row with no value",
            "is_explicit_zero": "true when the facility explicitly reported 0",
            "is_late_reported": "true when last updated more than 60 days after the period ended",
        },
    ),
    TableSpec(
        "facilities",
        "every known facility (expected reporters) with its place in the hierarchy",
        {
            "facility_id": "dhis2 org unit uid",
            "facility": "facility name (not unique)",
            "district": "district name",
            "region": "region name",
            "country": "country name",
        },
    ),
    TableSpec(
        "data_elements",
        "catalogue of indicators",
        {
            "data_element_id": "dhis2 data element uid",
            "data_element": "indicator name",
            "value_type": "INTEGER_ZERO_OR_POSITIVE or PERCENTAGE",
        },
    ),
    TableSpec(
        "country_reporting_rates",
        "monthly reporting rate per country: facilities with at least one non-empty value "
        "divided by all known facilities in that country, times 100",
        {
            "country": "country name",
            "period": "reporting month 'YYYYMM'",
            "reporting_rate": "percent, 0-100",
        },
    ),
    TableSpec(
        "district_monthly_totals",
        "monthly total per district and data element with month-over-month change. "
        "built by the pipeline (task 06). note: districts are identified by name only, "
        "so a name shared by two countries is combined here; prefer service_delivery when country matters",
        {
            "data_element": "indicator name",
            "district": "district name",
            "period": "reporting month 'YYYYMM'",
            "total_value": "sum of values for the month (NULL if nothing reported)",
            "prev_value": "previous month's total",
            "mom_pct_change": "percent change vs previous month",
        },
    ),
    TableSpec(
        "quarantine_summary",
        "rows the pipeline rejected before the gold layer, counted by reason and month",
        {
            "reason": "'unresolvable_data_element' (uid not in metadata) or 'orphaned_org_unit' (facility uid not in hierarchy)",
            "period": "reporting month 'YYYYMM'",
            "row_count": "number of rejected rows",
        },
    ),
]

ALLOWED_TABLES = frozenset(t.name for t in TABLES)


@dataclass
class DataContext:
    first_period: str
    last_period: str
    latest_complete_quarter: str
    countries: list[str]
    health_areas: list[str]
    data_elements: list[str]
    late_share_pct: float
    extra: dict = field(default_factory=dict)

    @property
    def recent_periods(self) -> list[str]:
        # the last two months are the ones most likely to still be filling in
        y, m = int(self.last_period[:4]), int(self.last_period[4:])
        py, pm = (y, m - 1) if m > 1 else (y - 1, 12)
        return [self.last_period, f"{py}{pm:02d}"]


def _glob(path: str) -> str:
    return path.replace("'", "''")


def build_connection(output_dir: str = "./output") -> tuple[duckdb.DuckDBPyConnection, DataContext]:
    """materialise curated tables in memory, then lock the database down"""
    w = os.path.join(output_dir, "warehouse")
    a = os.path.join(output_dir, "analytics")
    q = os.path.join(output_dir, "quarantine")
    for required in ("fact_service_delivery", "dim_org_unit", "dim_data_element"):
        if not os.path.exists(os.path.join(w, required)):
            raise FileNotFoundError(
                f"{os.path.join(w, required)} not found, run `python pipeline.py` first"
            )

    con = duckdb.connect(":memory:")
    fact = f"read_parquet('{_glob(w)}/fact_service_delivery/**/*.parquet', hive_partitioning=true)"
    dim_ou = f"read_parquet('{_glob(w)}/dim_org_unit/*.parquet')"
    dim_de = f"read_parquet('{_glob(w)}/dim_data_element/*.parquet')"

    con.execute(f"""
        CREATE TABLE facilities AS
        SELECT org_unit_key AS facility_id, facility_name AS facility,
               district_name AS district, region_name AS region, country_name AS country
        FROM {dim_ou}
    """)
    con.execute(f"""
        CREATE TABLE data_elements AS
        SELECT data_element_key AS data_element_id, data_element_name AS data_element,
               valueType AS value_type
        FROM {dim_de}
    """)
    con.execute(f"""
        CREATE TABLE service_delivery AS
        SELECT o.country, o.region, o.district, o.facility_id, o.facility,
               f.healthArea AS health_area,
               d.data_element_id, d.data_element, d.value_type,
               f.categoryOptionCombo AS category_option_combo,
               f.period_key AS period,
               CAST(substr(f.period_key, 1, 4) AS INTEGER) AS year,
               CAST(substr(f.period_key, 5, 2) AS INTEGER) AS month,
               substr(f.period_key, 1, 4) || '-Q' || CAST(ceil(CAST(substr(f.period_key, 5, 2) AS INTEGER) / 3.0) AS INTEGER) AS quarter,
               f.cast_value AS value,
               f.is_missing_value, f.is_explicit_zero, f.is_late_reported
        FROM {fact} f
        JOIN facilities o ON o.facility_id = f.org_unit_key
        JOIN data_elements d ON d.data_element_id = f.data_element_key
    """)

    rr_path = os.path.join(a, "country_reporting_rates.parquet")
    if os.path.exists(rr_path):
        con.execute(f"""
            CREATE TABLE country_reporting_rates AS
            SELECT country_name AS country, period, reporting_rate
            FROM read_parquet('{_glob(rr_path)}/*.parquet')
        """)
    else:
        con.execute("CREATE TABLE country_reporting_rates (country VARCHAR, period VARCHAR, reporting_rate DOUBLE)")

    mom_path = os.path.join(a, "mom_indicators.parquet")
    if os.path.exists(mom_path):
        con.execute(f"""
            CREATE TABLE district_monthly_totals AS
            SELECT name AS data_element, district_name AS district, period,
                   total_value, prev_value, mom_pct_change
            FROM read_parquet('{_glob(mom_path)}/*.parquet')
        """)
    else:
        con.execute("""CREATE TABLE district_monthly_totals (data_element VARCHAR, district VARCHAR,
                       period VARCHAR, total_value DOUBLE, prev_value DOUBLE, mom_pct_change DOUBLE)""")

    parts = []
    for reason, folder in (("unresolvable_data_element", "unresolvable_metadata.parquet"),
                           ("orphaned_org_unit", "orphaned_org_units.parquet")):
        p = os.path.join(q, folder)
        if os.path.exists(p):
            parts.append(f"SELECT '{reason}' AS reason, period FROM read_parquet('{_glob(p)}/*.parquet')")
    if parts:
        con.execute(f"""
            CREATE TABLE quarantine_summary AS
            SELECT reason, period, count(*) AS row_count FROM ({' UNION ALL '.join(parts)})
            GROUP BY reason, period
        """)
    else:
        con.execute("CREATE TABLE quarantine_summary (reason VARCHAR, period VARCHAR, row_count BIGINT)")

    ctx = _data_context(con)

    # defence in depth: no file, network or extension access from here on
    con.execute("SET enable_external_access = false")
    con.execute("SET lock_configuration = true")
    return con, ctx


def _data_context(con: duckdb.DuckDBPyConnection) -> DataContext:
    first, last = con.execute("SELECT min(period), max(period) FROM service_delivery").fetchone()
    ly, lm = int(last[:4]), int(last[4:])
    # a quarter counts as complete only if its last month is present
    q = (lm // 3) if lm % 3 == 0 else (lm - 1) // 3
    latest_complete = f"{ly}-Q{q}" if q > 0 else f"{ly - 1}-Q4"
    countries = [r[0] for r in con.execute("SELECT DISTINCT country FROM facilities ORDER BY 1").fetchall()]
    areas = [r[0] for r in con.execute("SELECT DISTINCT health_area FROM service_delivery ORDER BY 1").fetchall()]
    des = [r[0] for r in con.execute("SELECT data_element FROM data_elements ORDER BY 1").fetchall()]
    late = con.execute("SELECT 100.0 * avg(is_late_reported::INT) FROM service_delivery").fetchone()[0] or 0.0
    return DataContext(first, last, latest_complete, countries, areas, des, round(late, 1))


def schema_prompt(ctx: DataContext) -> str:
    """plain-text description of the allowed tables for the system prompt"""
    lines = []
    for t in TABLES:
        lines.append(f"TABLE {t.name}: {t.description}")
        for c, d in t.columns.items():
            lines.append(f"  - {c}: {d}")
        lines.append("")
    lines.append("DATA CONTEXT")
    lines.append(f"  - periods available: {ctx.first_period} to {ctx.last_period} (monthly)")
    lines.append(f"  - latest complete quarter: {ctx.latest_complete_quarter}")
    lines.append(f"  - countries: {', '.join(ctx.countries)}")
    lines.append(f"  - health areas: {', '.join(ctx.health_areas)}")
    lines.append(f"  - data elements: {'; '.join(ctx.data_elements)}")
    lines.append(f"  - {ctx.late_share_pct}% of all values were reported more than 60 days late")
    return "\n".join(lines)
