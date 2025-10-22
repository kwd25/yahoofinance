import os
import pandas as pd
import streamlit as st
import databricks.sql as dbsql
import altair as alt

# -----------------------
# Databricks connection
# -----------------------
def run_query(sql: str) -> pd.DataFrame:
    """Run Databricks SQL safely (open connection per query)."""
    with dbsql.connect(
        server_hostname=os.environ["DATABRICKS_SERVER"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"]
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            return pd.DataFrame(rows, columns=cols)

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
    st.subheader("Momentum Leaderboard, Top 15 Stocks")
    st.markdown(
        """
        This chart highlights the **15 strongest momentum performers** in the S&P 500 over the past 20 trading days.  
        A high momentum percentage suggests recent outperformance, often signaling short-term strength or speculative attention.  
        Traders can use this view to quickly identify which stocks are *leading* or *lagging* the current market trend.
        """
        
    )

    q1 = f"""
    WITH mx AS (SELECT CAST(MAX(date) AS DATE) AS d FROM {CATALOG}.{SCHEMA}.gold_features)
    SELECT symbol, mom_20d * 100 AS mom_20d_pct, vol_20d, close
    FROM {CATALOG}.{SCHEMA}.gold_features g
    JOIN mx ON CAST(g.date AS DATE) = mx.d
    WHERE mom_20d IS NOT NULL
    ORDER BY mom_20d DESC
    LIMIT 15
    """
    df1 = run_query(q1)

    chart = (
        alt.Chart(df1)
        .mark_bar()
        .encode(
            x=alt.X("mom_20d_pct:Q", title="20-Day Momentum (%)"),
            y=alt.Y("symbol:N", sort="-x", title="Symbol"),
            tooltip=["symbol", alt.Tooltip("mom_20d_pct:Q", title="20-Day Momentum (%)"), "vol_20d", "close"],
        )
        .properties(height=500, width="container")
    )

    st.altair_chart(chart, use_container_width=True)
    st.caption("Each bar represents a stock’s recent 20-day momentum as a percentage change from 20 trading days ago.")

    st.dataframe(
        df1.rename(
            columns={
                "symbol": "Symbol",
                "mom_20d_pct": "20-Day Momentum (%)",
                "vol_20d": "Volatility",
                "close": "Close Price ($)",
            }
        ).sort_values("20-Day Momentum (%)", ascending=False),
        use_container_width=True,
        hide_index=True,
    )
    st.markdown("""**Feature Definitions:**  
        **20-Day Momentum**: Measures a stock's percentage price change compared to 20 trading days ago.  
        **Volatility**: Reflects how much a stock's price fluctuates over the same 20 day window, scaled to an annual rate.  
        **Close Price** The closing price of the stock today.""")
# -----------------------
# Momentum vs Volatility
# -----------------------
with tabs[1]:
    st.subheader("Momentum vs. Volatility — Market Snapshot")

    st.markdown(
        """
        This scatter plot compares each stock's **20-day momentum** (recent performance) and **annualized volatility** (risk).  
        Each dot represents one S&P 500 stock as of the most recent trading day.  

        The **x-axis (Volatility)** measures how much a stock’s price fluctuates. Lower volatility = steadier performance.  
        The **y-axis (Momentum)** shows the recent 20-day percentage gain or loss. Higher = stronger short-term trend.  
        """
    )

    # Query for latest snapshot
    q2 = f"""
    WITH mx AS (SELECT CAST(MAX(date) AS DATE) AS d FROM {CATALOG}.{SCHEMA}.gold_features)
    SELECT symbol, mom_20d * 100 AS mom_20d_pct, vol_20d, close
    FROM {CATALOG}.{SCHEMA}.gold_features g
    JOIN mx ON CAST(g.date AS DATE) = mx.d
    WHERE mom_20d IS NOT NULL AND vol_20d IS NOT NULL
    ORDER BY mom_20d DESC
    """
    df2 = run_query(q2)

    # Compute median lines for the compass
    mom_med = df2["mom_20d_pct"].median()
    vol_med = df2["vol_20d"].median()

    # Main scatter chart
    base = alt.Chart(df2).encode(
        x=alt.X("vol_20d:Q", title="Volatility (Annualized)", scale=alt.Scale(zero=False)),
        y=alt.Y("mom_20d_pct:Q", title="20-Day Momentum (%)"),
        tooltip=[
            alt.Tooltip("symbol", title="Symbol"),
            alt.Tooltip("mom_20d_pct:Q", title="20-Day Momentum (%)", format=".2f"),
            alt.Tooltip("vol_20d:Q", title="Volatility (Annualized)", format=".2f"),
            alt.Tooltip("close:Q", title="Close Price ($)", format=".2f"),
        ],
    )

    scatter = base.mark_circle(size=80, color="steelblue", opacity=0.7)

    # Add quadrant "compass" lines
    vline = (
        alt.Chart(pd.DataFrame({"x": [vol_med]}))
        .mark_rule(color="gray", strokeDash=[5, 5])
        .encode(x="x:Q")
    )
    hline = (
        alt.Chart(pd.DataFrame({"y": [mom_med]}))
        .mark_rule(color="gray", strokeDash=[5, 5])
        .encode(y="y:Q")
    )


    text = alt.Chart(labels).mark_text(
        align="center", baseline="middle", fontSize=13, fontWeight="bold", color="gray"
    ).encode(x="vol_20d:Q", y="mom_20d_pct:Q", text="label")

    # Combine layers
    chart = (scatter + vline + hline + text).properties(
        height=550,
        width="container",
        title="Momentum vs Volatility (20-Day Snapshot)"
    )

    st.altair_chart(chart, use_container_width=True)

    st.markdown("""**Interpretation**  
        **High Momentum, Low Volatility:** “Steady Winners” — consistent strength with manageable risk.  
        **High Momentum, High Volatility:** “Hot Movers” — big gainers, but higher risk.  
        **Low Momentum, Low Volatility:** “Stable/Neutral” — low excitement, steady.  
        **Low Momentum, High Volatility:** “Falling/Choppy” — underperformers or volatile corrections.  """)

    # --- Data table ---
    st.dataframe(
        df2.rename(
            columns={
                "symbol": "Symbol",
                "mom_20d_pct": "20-Day Momentum (%)",
                "vol_20d": "Volatility (Annualized)",
                "close": "Close Price ($)",
            }
        ).sort_values("20-Day Momentum (%)", ascending=False),
        use_container_width=True,
        hide_index=True,
    )

    # --- Footer description ---
    st.markdown(
        """
        **Feature Notes**  
        - **Momentum (mom_20d)** captures short-term price trends.  
        - **Volatility (vol_20d)** measures recent price variability scaled to a one-year equivalent.  
        - Comparing both reveals whether high performers are *stable leaders* or *high-risk movers*.  
        """
    )

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
