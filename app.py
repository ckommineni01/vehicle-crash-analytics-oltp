import os
import streamlit as st
import pandas as pd
from datetime import timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import plotly.express as px


st.set_page_config(
    page_title="NYC Vehicle Crash Analytics",
    page_icon="🚗",
    layout="wide"
)

st.title("🚗 NYC Vehicle Crash Analytics")
st.caption("Phase 3 Streamlit Dashboard — PostgreSQL (OLTP)")


@st.cache_resource
def get_engine():
    """
    Works in:
    - Docker (DB_URL passed via docker-compose environment)
    - Local machine (fallback to localhost:5433)
    - Streamlit Cloud (optional: DB_URL in st.secrets)
    """
    db_url = os.getenv("DB_URL")

    if not db_url:
        try:
            db_url = st.secrets.get("DB_URL", None)
        except Exception:
            db_url = None

    if not db_url:
        db_url = "postgresql+psycopg2://postgres:postgres@127.0.0.1:5433/collisions"

    try:
        engine = create_engine(db_url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except SQLAlchemyError as e:
        st.error("❌ Database connection failed")
        st.code(str(e))
        st.stop()


@st.cache_data(ttl=300)
def run_query(sql, params=None):
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


@st.cache_data(ttl=300)
def get_schema_snapshot():
    """Return dict: tables set + columns dict for public schema."""
    tables_df = run_query("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';
    """)
    tables = set(tables_df["table_name"].tolist())

    cols_df = run_query("""
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public';
    """)
    cols_map = {}
    for _, r in cols_df.iterrows():
        t = r["table_name"]
        c = r["column_name"]
        cols_map.setdefault(t, set()).add(c)

    return {"tables": tables, "cols": cols_map}


def table_exists(name: str) -> bool:
    snap = get_schema_snapshot()
    return name in snap["tables"]


def col_exists(table: str, col: str) -> bool:
    snap = get_schema_snapshot()
    return col in snap["cols"].get(table, set())


def safe_query(sql, params=None, where="main"):
    """Run query but show clean error instead of crashing whole app."""
    try:
        return run_query(sql, params)
    except Exception as e:
        st.error(f"❌ Query failed in {where}")
        st.code(str(e))
        return None



with st.expander("🛠️ Schema Debug (what tables/columns exist in this deployed DB)", expanded=False):
    snap = get_schema_snapshot()
    st.write("**Public tables found:**")
    st.write(sorted(list(snap["tables"])))
    st.write("**Key table columns (if present):**")
    for t in ["collisions", "boroughs", "factors", "collision_factors"]:
        if t in snap["tables"]:
            st.write(f"- `{t}`: {sorted(list(snap['cols'].get(t, [])))}")
        else:
            st.write(f"- `{t}`: ❌ missing")



st.sidebar.header("🔎 Filters")


if not table_exists("collisions"):
    st.error("❌ `public.collisions` table not found in this database. Your DB is not initialized.")
    st.stop()


date_bounds = safe_query("""
    SELECT MIN(crash_date) AS min_date, MAX(crash_date) AS max_date
    FROM public.collisions;
""", where="date_bounds")

if date_bounds is None or date_bounds.empty or pd.isna(date_bounds.loc[0, "min_date"]):
    st.error("❌ Could not read crash_date bounds. Check `public.collisions.crash_date` exists and has data.")
    st.stop()

min_dt = pd.to_datetime(date_bounds.loc[0, "min_date"]).date()
max_dt = pd.to_datetime(date_bounds.loc[0, "max_date"]).date()

end_date = st.sidebar.date_input(
    "End date",
    value=max_dt,
    min_value=min_dt,
    max_value=max_dt
)

default_start = max(min_dt, end_date - timedelta(days=30))

start_date = st.sidebar.date_input(
    "Start date",
    value=default_start,
    min_value=min_dt,
    max_value=end_date
)



use_borough_lookup = table_exists("boroughs") and col_exists("collisions", "borough_id") \
                     and col_exists("boroughs", "borough_id") and col_exists("boroughs", "borough_name")

use_borough_text = col_exists("collisions", "borough")

# Borough list
boroughs = ["All"]
if use_borough_lookup:
    boroughs_df = safe_query("""
        SELECT borough_name AS borough
        FROM public.boroughs
        ORDER BY borough_name;
    """, where="borough_list")
    if boroughs_df is not None and not boroughs_df.empty:
        boroughs += boroughs_df["borough"].dropna().tolist()
elif use_borough_text:
    boroughs_df = safe_query("""
        SELECT DISTINCT borough
        FROM public.collisions
        WHERE borough IS NOT NULL AND borough <> ''
        ORDER BY borough;
    """, where="borough_list_fallback")
    if boroughs_df is not None and not boroughs_df.empty:
        boroughs += boroughs_df["borough"].dropna().tolist()
else:
    st.sidebar.warning("⚠️ Borough filtering disabled: no `boroughs` table + no `collisions.borough` column found.")

borough = st.sidebar.selectbox("Borough", boroughs)

metric = st.sidebar.selectbox(
    "Trend metric",
    [
        "Crashes",
        "Persons Injured",
        "Persons Killed",
        "Pedestrians Injured",
        "Cyclists Injured",
        "Motorists Injured",
    ],
)

top_n = st.sidebar.slider("Top N (Factors)", 5, 20, 10)

params = {
    "start_date": start_date,
    "end_date": end_date,
    "borough": borough,
    "top_n": top_n
}


def borough_join_and_filter(alias_c="c", alias_b="b"):
    """
    Returns:
      join_sql: string
      borough_name_expr: SQL expression that yields borough name
      where_filter_sql: string that enforces borough filter
    """
    if use_borough_lookup:
        join_sql = f"LEFT JOIN public.boroughs {alias_b} ON {alias_c}.borough_id = {alias_b}.borough_id"
        borough_expr = f"{alias_b}.borough_name"
        where_filter = f"AND (:borough = 'All' OR {alias_b}.borough_name = :borough)"
        return join_sql, borough_expr, where_filter

    if use_borough_text:
        join_sql = ""  # no join needed
        borough_expr = f"{alias_c}.borough"
        where_filter = f"AND (:borough = 'All' OR {alias_c}.borough = :borough)"
        return join_sql, borough_expr, where_filter

    # no borough support
    return "", "'UNKNOWN'", ""


JOIN_SQL, BOROUGH_EXPR, BOROUGH_FILTER = borough_join_and_filter()

# KPI
KPI_SQL = f"""
SELECT
  COUNT(*) AS total_crashes,
  COALESCE(SUM(c.number_of_persons_injured), 0) AS total_injured,
  COALESCE(SUM(c.number_of_persons_killed), 0) AS total_killed
FROM public.collisions c
{JOIN_SQL}
WHERE c.crash_date BETWEEN :start_date AND :end_date
{BOROUGH_FILTER};
"""

# Trend
TREND_SQL = f"""
SELECT
  c.crash_date::date AS day,
  COUNT(*) AS crashes,
  COALESCE(SUM(c.number_of_persons_injured), 0) AS persons_injured,
  COALESCE(SUM(c.number_of_persons_killed), 0) AS persons_killed,
  COALESCE(SUM(c.number_of_pedestrians_injured), 0) AS pedestrians_injured,
  COALESCE(SUM(c.number_of_cyclist_injured), 0) AS cyclists_injured,
  COALESCE(SUM(c.number_of_motorist_injured), 0) AS motorists_injured
FROM public.collisions c
{JOIN_SQL}
WHERE c.crash_date BETWEEN :start_date AND :end_date
{BOROUGH_FILTER}
GROUP BY day
ORDER BY day;
"""

# By borough (only if we can compute borough name)
BY_BOROUGH_SQL = f"""
SELECT
  COALESCE({BOROUGH_EXPR}, 'UNKNOWN') AS borough,
  COUNT(*) AS crashes
FROM public.collisions c
{JOIN_SQL}
WHERE c.crash_date BETWEEN :start_date AND :end_date
GROUP BY COALESCE({BOROUGH_EXPR}, 'UNKNOWN')
HAVING COALESCE({BOROUGH_EXPR}, 'UNKNOWN') <> 'UNKNOWN'
ORDER BY crashes DESC;
"""

# Detail
DETAIL_SQL = f"""
SELECT
  c.collision_id,
  c.crash_date,
  c.crash_time,
  COALESCE({BOROUGH_EXPR}, 'UNKNOWN') AS borough,
  c.zip_code,
  c.on_street_name,
  c.cross_street_name,
  c.off_street_name,
  c.number_of_persons_injured,
  c.number_of_persons_killed,
  c.number_of_pedestrians_injured,
  c.number_of_cyclist_injured,
  c.number_of_motorist_injured
FROM public.collisions c
{JOIN_SQL}
WHERE c.crash_date BETWEEN :start_date AND :end_date
{BOROUGH_FILTER}
ORDER BY c.crash_date DESC, c.crash_time DESC
LIMIT 500;
"""

# Factors support (optional)
factors_supported = table_exists("collision_factors") and table_exists("factors") \
    and col_exists("collision_factors", "collision_id") and col_exists("collision_factors", "factor_id") \
    and col_exists("factors", "factor_id")

TOP_FACTORS_SQL = f"""
SELECT
  COALESCE(f.factor_desc, f.factor_name, 'UNKNOWN') AS factor,
  COUNT(*) AS crashes
FROM public.collision_factors cf
JOIN public.factors f
  ON cf.factor_id = f.factor_id
JOIN public.collisions c
  ON cf.collision_id = c.collision_id
{JOIN_SQL}
WHERE c.crash_date BETWEEN :start_date AND :end_date
{BOROUGH_FILTER}
GROUP BY COALESCE(f.factor_desc, f.factor_name, 'UNKNOWN')
ORDER BY crashes DESC
LIMIT :top_n;
"""


tab1, tab2, tab3 = st.tabs(["📌 Overview", "📈 Trends", "📋 Data"])

with tab1:
    st.subheader("Key Metrics")
    kpi = safe_query(KPI_SQL, params, where="KPI")
    if kpi is not None and not kpi.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Crashes", int(kpi.iloc[0]["total_crashes"]))
        c2.metric("Total Injured", int(kpi.iloc[0]["total_injured"]))
        c3.metric("Total Killed", int(kpi.iloc[0]["total_killed"]))
    else:
        st.warning("No KPI results (check filters / data).")

    st.subheader("Crashes by Borough")
    if (use_borough_lookup or use_borough_text):
        by_b = safe_query(BY_BOROUGH_SQL, params, where="by_borough")
        if by_b is not None and not by_b.empty:
            st.plotly_chart(px.bar(by_b, x="borough", y="crashes"), use_container_width=True)
        else:
            st.warning("No borough breakdown available for selected date range.")
    else:
        st.info("Borough chart hidden because borough is not available in this schema.")

    st.subheader(f"Top {top_n} Contributing Factors")
    if factors_supported:
        factors = safe_query(TOP_FACTORS_SQL, params, where="top_factors")
        if factors is not None and not factors.empty:
            st.plotly_chart(
                px.bar(factors, x="crashes", y="factor", orientation="h"),
                use_container_width=True
            )
        else:
            st.warning("No factor results for selected filters.")
    else:
        st.info("Top Factors hidden: `collision_factors`/`factors` tables not found or missing required columns.")

with tab2:
    st.subheader("Trend Over Time")
    trend = safe_query(TREND_SQL, params, where="trend")
    if trend is None or trend.empty:
        st.warning("No trend results for selected filters.")
    else:
        metric_map = {
            "Crashes": "crashes",
            "Persons Injured": "persons_injured",
            "Persons Killed": "persons_killed",
            "Pedestrians Injured": "pedestrians_injured",
            "Cyclists Injured": "cyclists_injured",
            "Motorists Injured": "motorists_injured",
        }
        fig = px.line(trend, x="day", y=metric_map[metric], markers=True)
        st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.subheader("Latest 500 Records (Filtered)")
    detail = safe_query(DETAIL_SQL, params, where="detail")
    if detail is None or detail.empty:
        st.warning("No records found for selected filters.")
    else:
        st.dataframe(detail, use_container_width=True)

        st.download_button(
            "Download CSV",
            detail.to_csv(index=False),
            "filtered_collisions.csv",
            "text/csv"
        )
