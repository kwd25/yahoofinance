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
st.caption("Fed by a daily batch S&P 500 data pipeline. Currently only 339 fully backfilled symbols (patch later).")

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
    st.subheader("Momentum vs. Volatility, Market Snapshot")

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


    # Combine layers
    chart = (scatter + vline + hline).properties(
        height=550,
        width="container",
        title="Momentum vs Volatility (20-Day Snapshot)"
    )

    st.altair_chart(chart, use_container_width=True)

    st.markdown("""**Interpretation:**  
        High Momentum, Low Volatility: “Steady Winners”, consistent strength with manageable risk (Top-left quadrant).  
        High Momentum, High Volatility: “Hot Movers”, big gainers, but higher risk (Top-right quadrant).  
        Low Momentum, Low Volatility: “Stable/Neutral”, low excitement, steady (Bottom-left quadrant).  
        Low Momentum, High Volatility: “Falling/Choppy”, underperformers or volatile corrections (Bottom-right quadrant).  """)

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
    st.subheader("Price vs. Moving Averages, 30-Day Trend")

    st.markdown(
        """This visualization shows how a selected stock’s **closing price** compares to its **20-day** and **50-day Simple Moving Averages (SMAs)** over the past 30 trading days.   

        **Closing Price** represents the daily market close.  
        **SMA (Simple Moving Average)** smooths fluctuations to reveal underlying trends.  
        """
    )

    # Symbol selector
    symbols = run_query(f"SELECT DISTINCT symbol FROM {CATALOG}.{SCHEMA}.gold_features ORDER BY symbol")
    selected_symbol = st.selectbox("Select a stock symbol:", symbols["symbol"].tolist(), index=0)

    # Query 30-day lookback
    q3 = f"""
    WITH mx AS (SELECT CAST(MAX(date) AS DATE) AS d FROM {CATALOG}.{SCHEMA}.gold_features)
    SELECT
      CAST(g.date AS DATE) AS date,
      g.close,
      g.sma_20,
      g.sma_50
    FROM {CATALOG}.{SCHEMA}.gold_features g, mx
    WHERE g.symbol = '{selected_symbol}'
      AND CAST(g.date AS DATE) >= date_sub(mx.d, 30)
    ORDER BY date
    """
    df3 = run_query(q3)

    # Melt into long format for shared color/legend
    df_long = df3.melt("date", value_vars=["close", "sma_20", "sma_50"],
                       var_name="Series", value_name="Price")

    # Clean labels
    label_map = {
        "close": "Close Price",
        "sma_20": "SMA (20-Day)",
        "sma_50": "SMA (50-Day)"
    }
    df_long["Series"] = df_long["Series"].map(label_map)

    # --- Line chart with legend ---
    chart = (
        alt.Chart(df_long)
        .mark_line(strokeWidth=2)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("Price:Q", title="Price ($)"),
            color=alt.Color(
                "Series:N",
                title="Line Type",
                scale=alt.Scale(
                    domain=["Close Price", "SMA (20-Day)", "SMA (50-Day)"],
                    range=["steelblue", "orange", "green"]
                )
            ),
            tooltip=[
                alt.Tooltip("date:T", title="Date"),
                alt.Tooltip("Series:N", title="Type"),
                alt.Tooltip("Price:Q", title="Price ($)", format=".2f"),
            ],
        )
        .properties(
            height=500,
            width="container",
            title=f"{selected_symbol}: Price vs 20 & 50-Day Moving Averages (Last 30 Days)"
        )
        .configure_legend(labelFontSize=12, titleFontSize=13)
    )

    st.altair_chart(chart, use_container_width=True)

    st.markdown("""**Interpretation:**  
        When **Price > SMA(20)** → bullish short-term momentum.  
        When **Price > SMA(50)** → strong medium-term trend.  
        When **Price < both SMAs** → possible correction or downtrend.""")
    

    # --- Data table ---
    st.dataframe(
        df3.rename(
            columns={
                "date": "Date",
                "close": "Close ($)",
                "sma_20": "SMA (20-Day)",
                "sma_50": "SMA (50-Day)",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

    # --- Feature Notes ---
    st.markdown(
        """
        **Feature Notes:**  
        **Close ($):** End-of-day market price.  
        **SMA (20-Day):** Tracks short-term momentum.  
        **SMA (50-Day):** Reflects longer-term trend strength.  

        Monitoring crossovers (e.g., 20-day rising above 50-day) can highlight **trend reversals** or **entry signals**.
        """
    )
