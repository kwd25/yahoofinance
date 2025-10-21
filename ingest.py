import os
from datetime import datetime, date, timedelta
import pandas as pd
import yfinance as yf
from databricks import sql

# -----------------------
# Config
# -----------------------
CATALOG = os.getenv("CATALOG", "workspace")
SCHEMA = os.getenv("SCHEMA", "yahoo")
TABLE = os.getenv("BRONZE_TABLE", "bronze_prices")

def env_int(name: str, default: int) -> int:
    v = os.getenv(name, "")
    try:
        return int(v)
    except (TypeError, ValueError):
        return default

def env_list(name: str, default_csv: str) -> list[str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        raw = default_csv
    return [s.strip().upper() for s in raw.split(",") if s.strip()]

DEFAULT_SYMBOLS = ",".join([
    "AAPL","MSFT","GOOGL","AMZN","META","TSLA","NVDA","AMD","INTC","AVGO",
    "NFLX","CRM","ORCL","IBM","PYPL","PEP","SHOP","ADBE","QCOM","CSCO",
    "TSM","JPM","BAC","GS","MS","V","MA","AXP","XOM","CVX",
    "WMT","COST","HD","NKE","MCD","SBUX","JNJ","MRK","UNH","KO"
])
SYMS = env_list("SYMBOLS", DEFAULT_SYMBOLS)
BACKFILL_YEARS = env_int("BACKFILL_YEARS", 10)
INCREMENTAL_BUFFER_DAYS = env_int("BUFFER_DAYS", 7)

def env_int(name: str, default: int) -> int:
    v = os.getenv(name, "")
    try: return int(v)
    except (TypeError, ValueError): return default

FORCE_RELOAD_DAYS = env_int("FORCE_RELOAD_DAYS", 1)  # 0 = disabled



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

def to_yyyy_mm_dd(obj) -> str | None:
    """Normalize anything (str/datetime/date) to 'YYYY-MM-DD' or None."""
    if obj is None:
        return None
    if isinstance(obj, date) and not isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, datetime):
        return obj.date().isoformat()
    if isinstance(obj, str):
        # Trim to first 10 chars if it's an ISO with time (YYYY-MM-DDTHH:MM:SS)
        if len(obj) >= 10 and obj[4] == "-" and obj[7] == "-":
            return obj[:10]
        # Fallback parse
        try:
            return datetime.fromisoformat(obj).date().isoformat()
        except Exception:
            pass
    # As a last resort, let pandas try
    try:
        import pandas as pd
        return pd.to_datetime(obj).date().isoformat()
    except Exception:
        return None

def get_current_max_date(cur) -> str | None:
    cur.execute(f"SELECT MAX(date) FROM {CATALOG}.{SCHEMA}.{TABLE}")
    row = cur.fetchone()
    return to_yyyy_mm_dd(row[0] if row else None)

def plan_date_window(cur):
    today = date.today()
    end_str = (today + timedelta(days=1)).isoformat()  # yfinance end is exclusive

    # Manual override
    if FORCE_RELOAD_DAYS > 0:
        start_str = (today - timedelta(days=FORCE_RELOAD_DAYS)).isoformat()
        return start_str, end_str, f"force_reload_{FORCE_RELOAD_DAYS}d"

    current_max_str = get_current_max_date(cur)  # existing helper returning 'YYYY-MM-DD' or None
    if current_max_str is None:
        # initial backfill
        start_str = (today - timedelta(days=BACKFILL_YEARS * 365)).isoformat()
        return start_str, end_str, f"initial_backfill_{BACKFILL_YEARS}y"

    # incremental with overlap buffer
    current_max_dt = datetime.fromisoformat(current_max_str).date()
    start_str = (current_max_dt - timedelta(days=INCREMENTAL_BUFFER_DAYS)).isoformat()
    return start_str, end_str, "incremental"


def fetch(symbol: str, start: str, end: str) -> pd.DataFrame:
    # yfinance prefers 'YYYY-MM-DD' strings or date objects
    try:
        df = yf.download(symbol, start=start, end=end, interval="1d",
                         auto_adjust=False, progress=False)
    except Exception as e:
        print(f"[WARN] fetch failed for {symbol}: {e}")
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.reset_index().rename(columns=str.lower)
    # yfinance sometimes returns 'date' as Timestamp with time -> force to date
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date']).dt.date.astype(str)

    df["symbol"] = symbol
    cols = ["symbol","date","open","high","low","close","adj close","volume"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols].rename(columns={"adj close": "adj_close"})
    return df


EXPECTED = ["symbol","date","open","high","low","close","adj_close","volume"]
def to_values_rows(df: pd.DataFrame):
    """Build VALUES tuples from exactly EXPECTED columns; robust to pd.NA/None."""
    rows = []
    # df MUST already be limited to EXPECTED columns and sanitized
    for symbol, date_str, open_v, high_v, low_v, close_v, adj_v, vol_v in df.itertuples(index=False, name=None):
        # symbol
        symbol = (symbol or "")
        symbol = str(symbol).replace("'", "''")

        # numbers
        def fnum(x):
            try:
                return "NULL" if pd.isna(x) else f"{float(x)}"
            except Exception:
                return "NULL"
        vol = "NULL"
        try:
            if not pd.isna(vol_v):
                vol = f"{int(float(vol_v))}"
        except Exception:
            vol = "NULL"

        rows.append(
            f"('{symbol}','{date_str}',"
            f"{fnum(open_v)},{fnum(high_v)},{fnum(low_v)},{fnum(close_v)},{fnum(adj_v)},{vol},"
            f"current_timestamp())"
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

def debug_where_am_i(cur):
    cur.execute("SELECT current_catalog(), current_schema(), current_user()")
    print("CTX:", cur.fetchone())
    cur.execute(f"SELECT COUNT(*), MIN(date), MAX(date) FROM {CATALOG}.{SCHEMA}.{TABLE}")
    print("BRONZE_BEFORE:", cur.fetchone())

def debug_after(cur):
    cur.execute(f"SELECT COUNT(*), MIN(date), MAX(date) FROM {CATALOG}.{SCHEMA}.{TABLE}")
    print("BRONZE_AFTER:", cur.fetchone())

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten tuple/MultiIndex columns and normalize to lowercase strings."""
    def flat(c):
        if isinstance(c, tuple):
            # choose the last non-empty part like ('AAPL','Adj Close') -> 'Adj Close'
            parts = [p for p in c if p not in (None, "")]
            name = parts[-1] if parts else ""
            return str(name).lower()
        return str(c).lower()

    df = df.copy()
    df.columns = [flat(c) for c in df.columns]
    # unify common variants
    if "adj close" in df.columns and "adj_close" not in df.columns:
        df = df.rename(columns={"adj close": "adj_close"})
    # sometimes date is 'datetime' or 'index'
    if "date" not in df.columns:
        if "datetime" in df.columns:
            df = df.rename(columns={"datetime": "date"})
        elif "index" in df.columns:
            df = df.rename(columns={"index": "date"})
    return df




def main():
    with sql.connect(server_hostname=DATABRICKS_SERVER,
                     http_path=DATABRICKS_HTTP_PATH,
                     access_token=DATABRICKS_TOKEN) as conn:
        with conn.cursor() as cur:
            ensure_table(cur)
            debug_where_am_i(cur)
            start, end, mode = plan_date_window(cur)
            print(f"Ingest mode: {mode} | Range: {start} → {end} | Symbols: {len(SYMS)}")

            frames, failed = [], []
            for s in SYMS:
                df = fetch(s, start, end)

                # === paste these debug prints right here ===
                try:
                    rows = 0 if df is None else len(df)
                    dmin = None if df is None or df.empty else df["date"].min()
                    dmax = None if df is None or df.empty else df["date"].max()
                    print(f"[FETCH] {s} window {start}→{end} rows={rows} min={dmin} max={dmax}")

                    if df is not None and not df.empty:
                        cols = [c for c in ["date","open","high","low","close","adj_close","volume"] if c in df.columns]
                        print(df[cols].tail(2).to_string(index=False))
                except Exception as e:
                    print(f"[FETCH] {s} print error: {e}")
                # === end debug ===
                
                if df.empty:
                    failed.append(s)
                else:
                    try:
                        dmin, dmax = df["date"].min(), df["date"].max()
                        print(f"[DATA] {s}: {dmin} → {dmax} ({len(df)} rows)")
                    except Exception:
                        print(f"[DATA] {s}: {len(df)} rows")
                    frames.append(df)

            if failed:
                # informational only, we still ingest what we have
                print(f"[INFO] {len(failed)} symbols returned no data or failed: {failed[:8]}{'...' if len(failed)>8 else ''}")

            if not frames:
                print("No new data fetched.")
                debug_after(cur)
                return

            all_df = pd.concat(frames, ignore_index=True, sort=False)
            all_df = normalize_cols(all_df)

            EXPECTED = ["symbol","date","open","high","low","close","adj_close","volume"]
            for col in EXPECTED:
                if col not in all_df.columns:
                    all_df[col] = pd.NA
            all_df = all_df[EXPECTED].copy()  

            all_df["symbol"] = all_df["symbol"].astype("string").fillna("").astype(str)
            all_df["date"]   = pd.to_datetime(all_df["date"]).dt.date.astype(str)


            print(f"[DEBUG] fetched_rows_total={len(all_df)}")
            rows = to_values_rows(all_df)
            print(f"[DEBUG] values_rows_prepared={len(rows)}")

            if not rows:
                print("No rows to upsert.")
                debug_after(cur)
                return

            merge_batch(cur, rows)
            debug_after(cur)
            print(f"Upserted {len(rows)} rows into {CATALOG}.{SCHEMA}.{TABLE}.")

if __name__ == "__main__":
    main()
