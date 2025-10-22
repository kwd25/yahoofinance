import os
import pandas as pd
import streamlit as st
import databricks.sql as dbsql

# -----------------------
# Databricks connection
# -----------------------
@st.cache_resource
def get_connection():
    return dbsql.connect(
        server_hostname=os.environ["DATABRICKS_SERVER"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"]
    )

@st.cache_data(ttl=600)
def run_query(sql: str) -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql(sql, conn)

CATALOG = "workspace"
SCHEMA = "yahoo"

# -----------------------
# Streamlit layout
# -----------------------
st.set_page_config(page_title="Market Momentum Dashboard", layout="wide")
st.title("Market Momentum Dashboard")
st.caption("Live S&P 500 data pipeline — Bronze → Silver → Gold → Visualization")

tabs = st.tabs([
    "Momentum Leaderboard",
    "Momentum vs Volatility",
    "Price vs Moving Averages"
])

# -----------------------
# Momentum Leaderboard
# -----------------------
with tabs[0]:
    st.subheader("Top 20 Stocks by 20-Day Momentum")

    q1 = f"""
    WITH mx AS (SELECT CAST(MAX(date) AS DATE) AS d FROM {CATALOG}.{SCHEMA}.gold_features)
    SELECT symbol, mom_20d, vol_20d, close, sma_50
    FROM {CATALOG}.{SCHEMA}.gold_features g
    JOIN mx ON CAST(g.date AS DATE) = mx.d
    WHERE mom_20d IS NOT NULL
    ORDER BY mom_20d DESC
    LIMIT 20
    """
    df1 = run_query(q1)
    st.bar_chart(df1, x="symbol", y="mom_20d", height=400)

    st.dataframe(df1, use_container_width=True)

# -----------------------
# Momentum vs Volatility
# -----------------------
with tabs[1]:
    st.subheader("Momentum vs Volatility (Current Snapshot)")

    q2 = f"""
    WITH mx AS (SELECT CAST(MAX(date) AS DATE) AS d FROM {CATALOG}.{SCHEMA}.gold_features)
    SELECT symbol, mom_20d, vol_20d
    FROM {CATALOG}.{SCHEMA}.gold_features g
    JOIN mx ON CAST(g.date AS DATE) = mx.d
    WHERE mom_20d IS NOT NULL AND vol_20d IS NOT NULL
    """
    df2 = run_query(q2)

    st.scatter_chart(df2, x="vol_20d", y="mom_20d", color=None, size=None, height=400)
    st.dataframe(df2, use_container_width=True)

# -----------------------
# Price vs SMA
# -----------------------
with tabs[2]:
    st.subheader("Price vs Moving Averages (Past 60 Days)")

    symbol = st.selectbox("Select a Symbol", options=sorted(run_query(
        f"SELECT DISTINCT symbol FROM {CATALOG}.{SCHEMA}.gold_features ORDER BY symbol"
    )["symbol"].tolist()), index=0)

    q3 = f"""
    WITH mx AS (SELECT CAST(MAX(date) AS DATE) AS d FROM {CATALOG}.{SCHEMA}.gold_features)
    SELECT CAST(g.date AS DATE) AS date, close, sma_20, sma_50
    FROM {CATALOG}.{SCHEMA}.gold_features g, mx
    WHERE g.symbol = '{symbol}'
      AND CAST(g.date AS DATE) >= date_sub(mx.d, 60)
    ORDER BY date
    """
    df3 = run_query(q3)

    st.line_chart(df3.set_index("date")[["close", "sma_20", "sma_50"]])
    st.dataframe(df3, use_container_width=True)
