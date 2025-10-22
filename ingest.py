import os
import time
from datetime import datetime, date, timedelta
import pandas as pd
import yfinance as yf
from databricks import sql
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout

CATALOG = os.getenv("CATALOG", "workspace")
SCHEMA = os.getenv("SCHEMA", "yahoo")
TABLE = os.getenv("BRONZE_TABLE", "bronze_prices")
DATABRICKS_SERVER    = os.environ["DATABRICKS_SERVER"]
DATABRICKS_HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
DATABRICKS_TOKEN     = os.environ["DATABRICKS_TOKEN"]

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


def _normalize_yahoo_symbol(s: str) -> str:
    return s.strip().upper().replace('.', '-')  # e.g. BRK.B -> BRK-B

def load_symbols() -> list[str]:
    symbols: list[str] = []
    csv_path = os.getenv("SP500_CSV", "data/sp500.csv")
    try:
        df = pd.read_csv(csv_path)
        col = next((c for c in df.columns if c.lower().startswith("symbol")), df.columns[0])
        symbols = [_normalize_yahoo_symbol(x) for x in df[col].dropna().astype(str)]
        print(f"[SYMS] Loaded {len(symbols)} from CSV: {csv_path}")
    except Exception as e:
        print(f"[WARN] Could not load {csv_path}: {e}")

    if not symbols:
        try:
            import yfinance as yf
            raw = yf.tickers_sp500()
            symbols = [_normalize_yahoo_symbol(x) for x in raw] if raw else []
            if symbols:
                print(f"[SYMS] Loaded {len(symbols)} from yfinance.tickers_sp500()")
        except Exception as e:
            print(f"[WARN] yfinance.tickers_sp500 failed: {e}")

    if not symbols:
        try:
            import requests
            headers = {"User-Agent": "Mozilla/5.0 (ingest-bot)"}
            url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
            html = requests.get(url, headers=headers, timeout=20).text
            tables = pd.read_html(html) 
            df = tables[0]
            col = "Symbol" if "Symbol" in df.columns else df.columns[0]
            symbols = [_normalize_yahoo_symbol(x) for x in df[col].dropna().astype(str)]
            if symbols:
                print(f"[SYMS] Loaded {len(symbols)} from Wikipedia")
        except Exception as e:
            print(f"[WARN] Wikipedia scrape failed: {e}")

    if not symbols:
        symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]
        print("[SYMS] Using tiny fallback list (5 symbols)")

    seen, uniq = set(), []
    for s in symbols:
        if s and s not in seen:
            seen.add(s); uniq.append(s)

    limit = int(os.getenv("SYMBOL_LIMIT", "0"))
    if limit > 0:
        uniq = uniq[:limit]

    min_symbols = int(os.getenv("MIN_SYMBOLS", "0"))
    if min_symbols and len(uniq) < min_symbols:
        raise SystemExit(f"Only {len(uniq)} symbols loaded (<{min_symbols}); aborting run.")
    print(f"[SYMS] Final symbol count: {len(uniq)} | Sample: {', '.join(uniq[:10])}...")
    return uniq

SYMS = SYMS = load_symbols()
BACKFILL_YEARS = env_int("BACKFILL_YEARS", 10)
INCREMENTAL_BUFFER_DAYS = env_int("BUFFER_DAYS", 7)

def env_int(name: str, default: int) -> int:
    v = os.getenv(name, "")
    try: return int(v)
    except (TypeError, ValueError): return default

FORCE_RELOAD_DAYS = env_int("FORCE_RELOAD_DAYS", 1) 
FETCH_TIMEOUT = int(os.getenv("FETCH_TIMEOUT", "90")) 

def fetch_with_timeout(symbol, start, end):
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(fetch, symbol, start, end)
        try:
            return fut.result(timeout=FETCH_TIMEOUT)
        except FutTimeout:
            print(f"[TIMEOUT] {symbol} fetch exceeded {FETCH_TIMEOUT}s, skipping")
            return pd.DataFrame()
        except Exception as e:
            print(f"[WARN] {symbol} fetch exception: {e}")
            return pd.DataFrame()


SHARDS = int(os.getenv("SHARDS", "1"))         
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))  

if SHARDS > 1:
    SYMS = [s for i, s in enumerate(SYMS) if i % SHARDS == SHARD_INDEX]
    print(f"[SYMS] Shard {SHARD_INDEX+1}/{SHARDS}: {len(SYMS)} symbols")


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
    if obj is None:
        return None
    if isinstance(obj, date) and not isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, datetime):
        return obj.date().isoformat()
    if isinstance(obj, str):
        if len(obj) >= 10 and obj[4] == "-" and obj[7] == "-":
            return obj[:10]
        try:
            return datetime.fromisoformat(obj).date().isoformat()
        except Exception:
            pass
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

    if FORCE_RELOAD_DAYS > 0:
        start_str = (today - timedelta(days=FORCE_RELOAD_DAYS)).isoformat()
        return start_str, end_str, f"force_reload_{FORCE_RELOAD_DAYS}d"

    current_max_str = get_current_max_date(cur)
    if current_max_str is None:
        # initial backfill
        start_str = (today - timedelta(days=BACKFILL_YEARS * 365)).isoformat()
        return start_str, end_str, f"initial_backfill_{BACKFILL_YEARS}y"

    # incremental with overlap buffer
    current_max_dt = datetime.fromisoformat(current_max_str).date()
    start_str = (current_max_dt - timedelta(days=INCREMENTAL_BUFFER_DAYS)).isoformat()
    return start_str, end_str, "incremental"


def fetch(symbol: str, start: str, end: str) -> pd.DataFrame:
    try:
        df = yf.download(symbol,start=start,end=end, interval="1d", auto_adjust=False, progress=False, group_by="column",)
    except Exception as e:
        print(f"[WARN] fetch failed for {symbol}: {e}")
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    # Flatten possible MultiIndex columns
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [str(c[0]).lower() for c in df.columns]
    else:
        df = df.rename(columns=str.lower)

    if "date" not in df.columns:
        df = df.reset_index()
        df.columns = [str(c).lower() for c in df.columns]
        if "index" in df.columns and "date" not in df.columns:
            df = df.rename(columns={"index": "date"})

    if "adj close" in df.columns and "adj_close" not in df.columns:
        df = df.rename(columns={"adj close": "adj_close"})
    for c in ["open", "high", "low", "close", "adj_close", "volume"]:
        if c not in df.columns:
            df[c] = pd.NA

    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    df["symbol"] = symbol.upper()
    price_cols = ["open", "high", "low", "close", "adj_close", "volume"]
    df = df.dropna(how="all", subset=price_cols)
    df = df[df["close"].notna()]
    if df.empty:
        return pd.DataFrame()
    return df[["symbol", "date", "open", "high", "low", "close", "adj_close", "volume"]]


EXPECTED = ["symbol","date","open","high","low","close","adj_close","volume"]
def to_values_rows(df: pd.DataFrame):
    rows = []
    for symbol, date_str, open_v, high_v, low_v, close_v, adj_v, vol_v in df.itertuples(index=False, name=None):
        symbol = (symbol or "")
        symbol = str(symbol).replace("'", "''")

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
            parts = [p for p in c if p not in (None, "")]
            name = parts[-1] if parts else ""
            return str(name).lower()
        return str(c).lower()

    df = df.copy()
    df.columns = [flat(c) for c in df.columns]
    if "adj close" in df.columns and "adj_close" not in df.columns:
        df = df.rename(columns={"adj close": "adj_close"})
    if "date" not in df.columns:
        if "datetime" in df.columns:
            df = df.rename(columns={"datetime": "date"})
        elif "index" in df.columns:
            df = df.rename(columns={"index": "date"})
    return df

def connect_with_retry(max_seconds: int = 600, first_sleep: int = 5):
    server = os.environ["DATABRICKS_SERVER"].replace("https://","").replace("http://","").strip("/")
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    token = os.environ["DATABRICKS_TOKEN"]

    delay = first_sleep
    deadline = time.time() + max_seconds
    last_err = None

    while time.time() < deadline:
        try:
            conn = dbsql.connect(server_hostname=server, http_path=http_path, access_token=token)
            # warm-up query to trigger auto-start if needed
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchall()
            return conn
        except Exception as e:
            last_err = e
            print(f"[WARN] connect failed ({type(e).__name__}): {e}. Retrying in {delay}s...")
            time.sleep(delay)
            delay = min(delay * 2, 30)

    raise RuntimeError(f"Could not connect to Databricks within {max_seconds}s") from last_err



def main():
    with connect_with_retry() as conn, conn.cursor() as cur:

            ensure_table(cur)
            debug_where_am_i(cur)
            start, end, mode = plan_date_window(cur)
            print(f"Ingest mode: {mode} | Range: {start} → {end} | Symbols: {len(SYMS)}")

            frames, failed = [], []
            for s in SYMS:
                df = fetch_with_timeout(s, start, end)
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
                print(f"[INFO] {len(failed)} symbols returned no data or failed: {failed[:8]}{'...' if len(failed)>8 else ''}")
            if not frames:
                print("No new data fetched.")
                debug_after(cur)
                return

            all_df = pd.concat(frames, ignore_index=True, sort=False)
            all_df = normalize_cols(all_df)

            for col in EXPECTED:
                if col not in all_df.columns:
                    all_df[col] = pd.NA
            all_df = all_df[EXPECTED].copy()  
            all_df["symbol"] = all_df["symbol"].astype("string").fillna("").astype(str)
            all_df["date"]   = pd.to_datetime(all_df["date"]).dt.date.astype(str)

            print(f"[DEBUG] fetched_rows_total={len(all_df)}")
            rows = to_values_rows(all_df)
            print(f"[DEBUG] values_rows_prepared={len(rows)}")
            if rows:
                print("[MERGE] first_tuple:\n", rows[0])
            if not rows:
                print("No rows to upsert.")
                debug_after(cur)
                return

            merge_batch(cur, rows)
            debug_after(cur)
            print(f"Upserted {len(rows)} rows into {CATALOG}.{SCHEMA}.{TABLE}.")

if __name__ == "__main__":
    main()
