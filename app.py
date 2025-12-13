import os
import streamlit as st
import pandas as pd
from datetime import date, timedelta
from sqlalchemy import create_engine, text
import plotly.express as px

st.set_page_config(page_title="NYC Crash Analytics", page_icon="🚗", layout="wide")
st.title("🚗 NYC Vehicle Crash Analytics")
st.caption("Phase 3 Streamlit Dashboard — interactive filters + charts + table")

@st.cache_resource
def get_engine():
    # Use Streamlit Cloud Secrets if available, else env var
    db_url = None
    try:
        db_url = st.secrets.get("DB_URL")
    except Exception:
        pass
    if not db_url:
        db_url = os.getenv("DB_URL")

    if not db_url:
        return None
    return create_engine(db_url)

@st.cache_data(ttl=300)
def run_query(sql, params=None):
    eng = get_engine()
    if eng is None:
        return pd.DataFrame()
    with eng.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


st.sidebar.header("Filters")
end_date = st.sidebar.date_input("End date", value=date.today())
start_date = st.sidebar.date_input("Start date", value=end_date - timedelta(days=30))
borough_list_sql = """
SELECT DISTINCT borough
FROM collisions
WHERE borough IS NOT NULL AND borough <> ''
ORDER BY borough;
"""
boroughs_df = run_query(borough_list_sql)
boroughs = ["All"] + (boroughs_df["borough"].tolist() if not boroughs_df.empty else [])

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

top_n = st.sidebar.slider("Top N", min_value=5, max_value=20, value=10)

params = {"start_date": start_date, "end_date": end_date, "borough": borough}

KPI_SQL = """
SELECT
  COUNT(*) AS total_crashes,
  COALESCE(SUM(number_of_persons_injured), 0) AS total_injured,
  COALESCE(SUM(number_of_persons_killed), 0) AS total_killed
FROM collisions
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
FROM collisions
WHERE crash_date BETWEEN :start_date AND :end_date
  AND (:borough = 'All' OR borough = :borough)
GROUP BY day
ORDER BY day;
"""

BY_BOROUGH_SQL = """
SELECT
  COALESCE(NULLIF(borough,''), 'UNKNOWN') AS borough,
  COUNT(*) AS crashes
FROM collisions
WHERE crash_date BETWEEN :start_date AND :end_date
GROUP BY COALESCE(NULLIF(borough,''), 'UNKNOWN')
ORDER BY crashes DESC;
"""

TOP_FACTORS_SQL = """
SELECT factor, COUNT(*) AS crashes
FROM (
  SELECT contributing_factor_vehicle_1 AS factor
  FROM collisions
  WHERE crash_date BETWEEN :start_date AND :end_date
    AND (:borough = 'All' OR borough = :borough)
    AND contributing_factor_vehicle_1 IS NOT NULL
    AND contributing_factor_vehicle_1 <> ''
    AND contributing_factor_vehicle_1 <> 'Unspecified'

  UNION ALL
  SELECT contributing_factor_vehicle_2 AS factor
  FROM collisions
  WHERE crash_date BETWEEN :start_date AND :end_date
    AND (:borough = 'All' OR borough = :borough)
    AND contributing_factor_vehicle_2 IS NOT NULL
    AND contributing_factor_vehicle_2 <> ''
    AND contributing_factor_vehicle_2 <> 'Unspecified'
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
  number_of_pedestrians_injured, number_of_cyclist_injured, number_of_motorist_injured,
  contributing_factor_vehicle_1, contributing_factor_vehicle_2,
  vehicle_type_code1, vehicle_type_code2,
  collision_id
FROM collisions
WHERE crash_date BETWEEN :start_date AND :end_date
  AND (:borough = 'All' OR borough = :borough)
ORDER BY crash_date DESC, crash_time DESC
LIMIT 500;
"""
tab1, tab2, tab3 = st.tabs(["📌 Overview", "📈 Trends", "📋 Data"])

with tab1:
    st.subheader("Key Metrics")
    kpi = run_query(KPI_SQL, params)

    if kpi.empty:
        st.error("No data returned. Check DB connection / table name (collisions).")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Crashes", int(kpi["total_crashes"].iloc[0]))
        c2.metric("Total Injured", int(kpi["total_injured"].iloc[0]))
        c3.metric("Total Killed", int(kpi["total_killed"].iloc[0]))

    st.subheader("Crashes by Borough (selected date range)")
    by_b = run_query(BY_BOROUGH_SQL, {"start_date": start_date, "end_date": end_date})
    if not by_b.empty:
        fig_b = px.bar(by_b, x="borough", y="crashes")
        st.plotly_chart(fig_b, use_container_width=True)

    st.subheader(f"Top {top_n} Contributing Factors (selected filters)")
    factors = run_query(TOP_FACTORS_SQL, {**params, "top_n": top_n})
    if not factors.empty:
        fig_f = px.bar(factors, x="crashes", y="factor", orientation="h")
        st.plotly_chart(fig_f, use_container_width=True)
    else:
        st.info("No contributing factor data for the selected filters.")

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
    ycol = metric_map[metric]

    if trend.empty:
        st.warning("No data for selected filters.")
    else:
        fig_t = px.line(trend, x="day", y=ycol, markers=True)
        st.plotly_chart(fig_t, use_container_width=True)

with tab3:
    st.subheader("Filtered Records (Latest 500)")
    detail = run_query(DETAIL_SQL, params)
    st.dataframe(detail, use_container_width=True)

    if not detail.empty:
        st.download_button(
            "Download CSV",
            data=detail.to_csv(index=False).encode("utf-8"),
            file_name="filtered_collisions.csv",
            mime="text/csv",
        )
