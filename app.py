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
st.caption("Phase 3 Streamlit Dashboard — Neon PostgreSQL + Streamlit Cloud")

# ---------------- DATABASE ----------------
@st.cache_resource
def get_engine():
    db_url = None

    # 1️⃣ Streamlit Cloud secrets
    if "DB_URL" in st.secrets:
        db_url = st.secrets["DB_URL"]

    # 2️⃣ Local fallback
    if not db_url:
        db_url = os.getenv("DB_URL")

    if not db_url:
        st.error(
            "❌ DB_URL not found.\n\n"
            "Add it in **Streamlit → App Settings → Secrets** as:\n"
            "`DB_URL = 'postgresql://...'`"
        )
        st.stop()

    try:
        engine = create_engine(
            db_url,
            pool_pre_ping=True,
            connect_args={"sslmode": "require"},
        )
        # test connection
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

end_date = st.sidebar.date_input("End date", value=date.today())
start_date = st.sidebar.date_input(
    "Start date", value=end_date - timedelta(days=30)
)

# Borough list
boroughs_df = run_query("""
    SELECT DISTINCT borough
    FROM public.collisions
    WHERE borough IS NOT NULL AND borough <> ''
    ORDER BY borough;
""")

boroughs = ["All"] + boroughs_df["borough"].tolist()
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

top_n = st.sidebar.slider("Top N", 5, 20, 10)

params = {
    "start_date": start_date,
    "end_date": end_date,
    "borough": borough,
}

# ---------------- SQL QUERIES ----------------
KPI_SQL = """
SELECT
  COUNT(*) AS total_crashes,
  COALESCE(SUM(number_of_persons_injured), 0) AS total_injured,
  COALESCE(SUM(number_of_persons_killed), 0) AS total_killed
FROM public.collisions
WHERE crash_date BETWEEN :start_date AND :end_date
  AND (:borough = 'All' OR borough = :borough);
"""

TREND_SQL = """
SELECT
  crash_date::date AS day,
  COUNT(*) AS crashes,
  COALESCE(SUM(number_of_persons_injured), 0) AS persons_injured,
  COALESCE(SUM(number_of_persons_killed), 0) AS persons_killed,
  COALESCE(SUM(number_of_pedestrians_injured), 0) AS pedestrians_injured,
  COALESCE(SUM(number_of_cyclist_injured), 0) AS cyclists_injured,
  COALESCE(SUM(number_of_motorist_injured), 0) AS motorists_injured
FROM public.collisions
WHERE crash_date BETWEEN :start_date AND :end_date
  AND (:borough = 'All' OR borough = :borough)
GROUP BY day
ORDER BY day;
"""

BY_BOROUGH_SQL = """
SELECT
  COALESCE(NULLIF(borough,''), 'UNKNOWN') AS borough,
  COUNT(*) AS crashes
FROM public.collisions
WHERE crash_date BETWEEN :start_date AND :end_date
GROUP BY COALESCE(NULLIF(borough,''), 'UNKNOWN')
ORDER BY crashes DESC;
"""

TOP_FACTORS_SQL = """
SELECT factor, COUNT(*) AS crashes
FROM (
  SELECT contributing_factor_vehicle_1 AS factor
  FROM public.collisions
  WHERE crash_date BETWEEN :start_date AND :end_date
    AND (:borough = 'All' OR borough = :borough)
    AND contributing_factor_vehicle_1 IS NOT NULL
    AND contributing_factor_vehicle_1 <> ''
    AND contributing_factor_vehicle_1 <> 'Unspecified'
) t
GROUP BY factor
ORDER BY crashes DESC
LIMIT :top_n;
"""

DETAIL_SQL = """
SELECT
  crash_date, crash_time, borough, zip_code,
  on_street_name, cross_street_name, off_street_name,
  number_of_persons_injured, number_of_persons_killed,
  number_of_pedestrians_injured, number_of_cyclist_injured,
  number_of_motorist_injured,
  contributing_factor_vehicle_1,
  vehicle_type_code1,
  collision_id
FROM public.collisions
WHERE crash_date BETWEEN :start_date AND :end_date
  AND (:borough = 'All' OR borough = :borough)
ORDER BY crash_date DESC, crash_time DESC
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
    st.plotly_chart(
        px.bar(by_b, x="borough", y="crashes"),
        use_container_width=True
    )

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

    fig = px.line(
        trend,
        x="day",
        y=metric_map[metric],
        markers=True
    )
    st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.subheader("Latest 500 Records")
    detail = run_query(DETAIL_SQL, params)
    st.dataframe(detail, use_container_width=True)

    st.download_button(
        "Download CSV",
        detail.to_csv(index=False),
        "filtered_collisions.csv",
        "text/csv"
    )
