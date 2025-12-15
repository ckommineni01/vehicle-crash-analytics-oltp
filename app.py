import os
import streamlit as st
import pandas as pd
from datetime import date, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import plotly.express as px

# ---------------- PAGE CONFIG ----------------
st.set_page_config(
    page_title="NYC Vehicle Crash Analytics",
    page_icon="🚗",
    layout="wide"
)

st.title("🚗 NYC Vehicle Crash Analytics")
st.caption("Phase 3 Streamlit Dashboard — Docker + PostgreSQL (OLTP)")

# ---------------- DATABASE ----------------
@st.cache_resource
def get_engine():
    """
    Works in:
    - Docker (DB_URL passed via docker-compose environment)
    - Local machine (fallback to localhost:5433)
    - Streamlit Cloud (optional: DB_URL in st.secrets)
    """
    db_url = os.getenv("DB_URL")

    # Try Streamlit secrets only if present; if secrets file doesn't exist, ignore
    if not db_url:
        try:
            db_url = st.secrets.get("DB_URL", None)
        except Exception:
            db_url = None

    # Local fallback
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

# ---------------- SIDEBAR FILTERS ----------------
st.sidebar.header("🔎 Filters")

# --- Date bounds from DB  ---
date_bounds = run_query("""
    SELECT MIN(crash_date) AS min_date, MAX(crash_date) AS max_date
    FROM public.collisions;
""")

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

# Borough list 
boroughs_df = run_query("""
    SELECT borough_name AS borough
    FROM public.boroughs
    ORDER BY borough_name;
""")

boroughs = ["All"] + boroughs_df["borough"].dropna().tolist()
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
}

# ---------------- SQL QUERIES (MATCHES YOUR OLTP SCHEMA) ----------------
KPI_SQL = """
SELECT
  COUNT(*) AS total_crashes,
  COALESCE(SUM(c.number_of_persons_injured), 0) AS total_injured,
  COALESCE(SUM(c.number_of_persons_killed), 0) AS total_killed
FROM public.collisions c
LEFT JOIN public.boroughs b ON c.borough_id = b.borough_id
WHERE c.crash_date BETWEEN :start_date AND :end_date
  AND (:borough = 'All' OR b.borough_name = :borough);
"""

TREND_SQL = """
SELECT
  c.crash_date::date AS day,
  COUNT(*) AS crashes,
  COALESCE(SUM(c.number_of_persons_injured), 0) AS persons_injured,
  COALESCE(SUM(c.number_of_persons_killed), 0) AS persons_killed,
  COALESCE(SUM(c.number_of_pedestrians_injured), 0) AS pedestrians_injured,
  COALESCE(SUM(c.number_of_cyclist_injured), 0) AS cyclists_injured,
  COALESCE(SUM(c.number_of_motorist_injured), 0) AS motorists_injured
FROM public.collisions c
LEFT JOIN public.boroughs b ON c.borough_id = b.borough_id
WHERE c.crash_date BETWEEN :start_date AND :end_date
  AND (:borough = 'All' OR b.borough_name = :borough)
GROUP BY day
ORDER BY day;
"""

BY_BOROUGH_SQL = """
SELECT
  COALESCE(b.borough_name, 'UNKNOWN') AS borough,
  COUNT(*) AS crashes
FROM public.collisions c
LEFT JOIN public.boroughs b ON c.borough_id = b.borough_id
WHERE c.crash_date BETWEEN :start_date AND :end_date
GROUP BY COALESCE(b.borough_name, 'UNKNOWN')
HAVING COALESCE(b.borough_name, 'UNKNOWN') <> 'UNKNOWN'
ORDER BY crashes DESC;
"""

TOP_FACTORS_SQL = """
SELECT
  f.factor_desc AS factor,
  COUNT(*) AS crashes
FROM public.collision_factors cf
JOIN public.factors f
  ON cf.factor_id = f.factor_id
JOIN public.collisions c
  ON cf.collision_id = c.collision_id
LEFT JOIN public.boroughs b
  ON c.borough_id = b.borough_id
WHERE c.crash_date BETWEEN :start_date AND :end_date
  AND (:borough = 'All' OR b.borough_name = :borough)
GROUP BY f.factor_desc
ORDER BY crashes DESC
LIMIT :top_n;
"""

DETAIL_SQL = """
SELECT
  c.collision_id,
  c.crash_date,
  c.crash_time,
  b.borough_name AS borough,
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
LEFT JOIN public.boroughs b ON c.borough_id = b.borough_id
WHERE c.crash_date BETWEEN :start_date AND :end_date
  AND (:borough = 'All' OR b.borough_name = :borough)
ORDER BY c.crash_date DESC, c.crash_time DESC
LIMIT 500;
"""

# ---------------- UI TABS ----------------
tab1, tab2, tab3 = st.tabs(["📌 Overview", "📈 Trends", "📋 Data"])

with tab1:
    st.subheader("Key Metrics")
    kpi = run_query(KPI_SQL, params)

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Crashes", int(kpi.iloc[0]["total_crashes"]))
    c2.metric("Total Injured", int(kpi.iloc[0]["total_injured"]))
    c3.metric("Total Killed", int(kpi.iloc[0]["total_killed"]))

    st.subheader("Crashes by Borough")
    by_b = run_query(BY_BOROUGH_SQL, params)
    st.plotly_chart(px.bar(by_b, x="borough", y="crashes"), use_container_width=True)

    st.subheader(f"Top {top_n} Contributing Factors")
    factors = run_query(TOP_FACTORS_SQL, {**params, "top_n": top_n})
    st.plotly_chart(
        px.bar(factors, x="crashes", y="factor", orientation="h"),
        use_container_width=True
    )

with tab2:
    st.subheader("Trend Over Time")
    trend = run_query(TREND_SQL, params)

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
    detail = run_query(DETAIL_SQL, params)
    st.dataframe(detail, use_container_width=True)

    st.download_button(
        "Download CSV",
        detail.to_csv(index=False),
        "filtered_collisions.csv",
        "text/csv"
    )
