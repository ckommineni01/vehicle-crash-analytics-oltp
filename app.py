import streamlit as st

st.title("NYC Vehicle Crash Analytics")

borough = st.selectbox(
    "Select Borough",
    ["All", "BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND"]
)

st.line_chart([10, 20, 15, 30])
st.bar_chart([5, 8, 3, 10])
