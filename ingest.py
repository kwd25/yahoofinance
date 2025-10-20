import os
from datetime import date, timedelta
import pandas as pd
import yfinance as yf
from databricks import sql

# -----------------------
# Config
# -----------------------
CATALOG = os.getenv("CATALOG", "workspace")
SCHEMA = os.getenv("SCHEMA", "yahoo")
TABLE = os.getenv("BRONZE_TABLE", "bronze_prices")

DEFAULT_SYMBOLS = ",".join([
    "AAPL","MSFT","GOOGL","AMZN","META","TSLA","NVDA","AMD","INTC","AVGO",
    "NFLX","CRM","ORCL","IBM","PYPL","SQ","SHOP","ADBE","QCOM","CSCO",
    "TSM","JPM","BAC","GS","MS","V","MA","AXP","XOM","CVX",
    "WMT","COST","HD","NKE","MCD","SBUX","JNJ","MRK","UNH","KO"
])
SYMS = [s.strip().upper() for s in os.getenv("SYMBOLS", DEFAULT_SYMBOLS).split(",") if s.strip()]

BACKFILL_YEARS = int(os.getenv("BACKFILL_YEARS", "10"))
INCREMENTAL_BUFFER_DAYS = int(os.getenv("BUFFER_DAYS", "7"))

DATABRICKS_SERVER    = os.environ["DATABRICKS_SERVER"]
DATABRICKS_HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
DATABRICKS_TOKEN     = os.environ["DATABRICKS_TOKEN"]


def ensure_table(cur):
    cur.execute(f"USE CATALOG {CATALOG}")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    cur.execute(f"USE {CATALOG}.{SCHEMA}")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
          symbol STRING,
          date DATE,
          open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, adj_close DOUBLE, volume BIGINT,
          _ingest_ts TIMESTAMP
        ) USING DELTA
    """)

def get_current_max_date(cur):
    cur.execute(f"SELECT MAX(date) FROM {CATALOG}.{SCHEMA}.{TABLE}")
    row = cur.fetchone()
    
    return row[0] if row and row[0] is not None else None

def plan_date_window(cur):
    """Decide START and END based on whether we have data already."""
    current_max = get_current_max_date(cur)
    today = date.today()
    end = (today + timedelta(days=1)).isoformat()  # yfinance end is exclusive

    if current_max is None:
        # Initial backfill
        start = (today - timedelta(days=BACKFILL_YEARS * 365)).isoformat()
        mode = f"initial_backfill_{BACKFILL_YEARS}y"
    else:
        # Incremental window with a small overlap buffer
        start_dt = current_max - timedelta(days=INCREMENTAL_BUFFER_DAYS)
        start = start_dt.isoformat()
        mode = "incremental"
    return start, end, mode

def fetch(symbol: str, start: str, end: str) -> pd.DataFrame:
    df = yf.download(symbol, start=start, end=end, interval="1d", auto_adjust=False, progress=False)
    if df.empty:
        return df
    df = df.reset_index().rename(columns=str.lower)
    df["symbol"] = symbol
    df = df[["symbol","date","open","high","low","close","adj close","volume"]]
    df = df.rename(columns={"adj close":"adj_close"})
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    return df

def to_values_rows(df: pd.DataFrame):
    rows = []
    for r in df.itertuples(index=False):
        sym = r.symbol.replace("'", "''")
        date_str = r.date
        val = lambda x: "NULL" if pd.isna(x) else f"{float(x)}"
        vol = "NULL" if pd.isna(r.volume) else f"{int(r.volume)}"
        rows.append(
            f"('{sym}','{date_str}',{val(r.open)},{val(r.high)},{val(r.low)},{val(r.close)},{val(r.adj_close)},{vol},current_timestamp())"
        )
    return rows

def merge_batch(cur, values_rows, batch_size=400):
    for i in range(0, len(values_rows), batch_size):
        chunk = ",".join(values_rows[i:i+batch_size])
        cur.execute(f"""
            MERGE INTO {CATALOG}.{SCHEMA}.{TABLE} AS t
            USING (SELECT * FROM VALUES {chunk}
              AS v(symbol, date, open, high, low, close, adj_close, volume, _ingest_ts)) s
            ON  t.symbol = s.symbol AND t.date = s.date
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *;
        """)

def main():
    with sql.connect(server_hostname=DATABRICKS_SERVER,
                     http_path=DATABRICKS_HTTP_PATH,
                     access_token=DATABRICKS_TOKEN) as conn:
        with conn.cursor() as cur:
            ensure_table(cur)
            start, end, mode = plan_date_window(cur)
            print(f"Ingest mode: {mode} | Range: {start} → {end} | Symbols: {len(SYMS)}")

            frames = []
            for s in SYMS:
                df = fetch(s, start, end)
                if not df.empty:
                    frames.append(df)

            if not frames:
                print("No new data fetched.")
                return

            all_df = pd.concat(frames, ignore_index=True)
            rows = to_values_rows(all_df)
            if not rows:
                print("No rows to upsert.")
                return

            merge_batch(cur, rows)
            print(f"Upserted {len(rows)} rows into {CATALOG}.{SCHEMA}.{TABLE}.")

if __name__ == "__main__":
    main()
