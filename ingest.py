#!/usr/bin/env python3

# === Imports ===
import os, math
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout
import pandas as pd
import yfinance as yf
import databricks.sql as dbsql

# === Config (env) ===
CATALOG  = os.getenv("CATALOG", "workspace")
SCHEMA   = os.getenv("SCHEMA",  "yahoo")
TABLE    = os.getenv("TABLE",   "bronze_prices")

SERVER   = os.environ["DATABRICKS_SERVER"]
HTTPPATH = os.environ["DATABRICKS_HTTP_PATH"]
TOKEN    = os.environ["DATABRICKS_TOKEN"]

# Window: choose ONE (prefer BACKFILL_YEARS for big runs)
BACKFILL_YEARS    = int(os.getenv("BACKFILL_YEARS", "0"))
FORCE_RELOAD_DAYS = int(os.getenv("FORCE_RELOAD_DAYS", "10"))

# Performance / resilience
WORKERS       = int(os.getenv("WORKERS", "8"))
FETCH_TIMEOUT = int(os.getenv("FETCH_TIMEOUT", "90"))
BATCH_ROWS    = int(os.getenv("BATCH_ROWS", "5000"))

# Symbols
SP500_CSV  = os.getenv("SP500_CSV", "data/sp500.csv")
INCLUDE_FILE = os.getenv("INCLUDE_SYMBOLS_FILE", "")
SYMBOL_LIMIT = int(os.getenv("SYMBOL_LIMIT", "0"))
MIN_SYMBOLS  = int(os.getenv("MIN_SYMBOLS", "0"))

# Sharding
SHARDS      = int(os.getenv("SHARDS", "1"))
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
MIN_SYMBOLS_SHARD = int(os.getenv("MIN_SYMBOLS_SHARD", "0"))

# Smart backfill for short-history symbols
SMART_BACKFILL = os.getenv("SMART_BACKFILL", "1") == "1"
SMART_MIN_ROWS = int(os.getenv("SMART_MIN_ROWS", "2520"))  # ~10y

# === DB helpers ===
def connect():
    return dbsql.connect(
        server_hostname=SERVER,
        http_path=HTTPPATH,
        access_token=TOKEN,
    )

def exec_one(cur, sql):
    cur.execute(sql)
    try:
        row = cur.fetchone()
        return row[0] if row else None
    except Exception:
        return None

def qfmt(s):  # single-quote escape
    return s.replace("'", "''")

# === Symbols ===
def _norm_symbol(s: str) -> str:
    return s.strip().upper().replace(".", "-")

def load_symbols() -> list[str]:
    syms: list[str] = []

    # 1) CSV (deterministic)
    try:
        df = pd.read_csv(SP500_CSV)
        col = next((c for c in df.columns if c.lower().startswith("symbol")), df.columns[0])
        syms = [_norm_symbol(x) for x in df[col].dropna().astype(str)]
        print(f"[SYMS] Loaded {len(syms)} from CSV")
    except Exception as e:
        print(f"[WARN] CSV load failed: {e}")

    # 2) yfinance.tickers_sp500 
    if not syms:
        try:
            lst = getattr(yf, "tickers_sp500", None)
            if callable(lst):
                raw = lst()
                syms = [_norm_symbol(x) for x in raw] if raw else []
                if syms:
                    print(f"[SYMS] Loaded {len(syms)} from yfinance.tickers_sp500()")
        except Exception as e:
            print(f"[WARN] yfinance.tickers_sp500 failed: {e}")

    # 3) Wikipedia (HTTP + read_html)
    if not syms:
        try:
            import requests
            headers = {"User-Agent": "Mozilla/5.0 (ingest-bot)"}
            html = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
                                headers=headers, timeout=20).text
            tables = pd.read_html(html)
            df = tables[0]
            col = "Symbol" if "Symbol" in df.columns else df.columns[0]
            syms = [_norm_symbol(x) for x in df[col].dropna().astype(str)]
            print(f"[SYMS] Loaded {len(syms)} from Wikipedia")
        except Exception as e:
            print(f"[WARN] Wikipedia scrape failed: {e}")

    # 4) Fallback tiny list
    if not syms:
        syms = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]
        print("[SYMS] Using tiny fallback (5 symbols)")

    # include-file override
    if INCLUDE_FILE and os.path.exists(INCLUDE_FILE):
        inc = {line.strip().upper() for line in open(INCLUDE_FILE) if line.strip()}
        syms = [s for s in syms if s in inc]
        print(f"[SYMS] Override include list: {len(syms)} from {INCLUDE_FILE}")

    # sharding, limit, de-dupe
    seen, uniq = set(), []
    for s in syms:
        if s and s not in seen:
            seen.add(s); uniq.append(s)
    if SYMBOL_LIMIT > 0:
        uniq = uniq[:SYMBOL_LIMIT]
    if SHARDS > 1:
        uniq = [s for i, s in enumerate(uniq) if i % SHARDS == SHARD_INDEX]
        print(f"[SYMS] Shard {SHARD_INDEX+1}/{SHARDS}: {len(uniq)} symbols")

    if MIN_SYMBOLS and len(uniq) < MIN_SYMBOLS:
        raise SystemExit(f"Only {len(uniq)} symbols (<{MIN_SYMBOLS}); aborting")
    if MIN_SYMBOLS_SHARD and len(uniq) < MIN_SYMBOLS_SHARD:
        raise SystemExit(f"Shard too small: {len(uniq)} < {MIN_SYMBOLS_SHARD}")

    sample = ", ".join(uniq[:10]) + (" ..." if len(uniq) > 10 else "")
    print(f"[SYMS] Final: {len(uniq)} | Sample: {sample}")
    return uniq

# === Window planning ===
def plan_window_global(cur):
    today = date.today()
    if BACKFILL_YEARS > 0:
        start = (today - timedelta(days=365*BACKFILL_YEARS)).isoformat()
        end   = (today + timedelta(days=1)).isoformat()
        mode  = f"backfill_{BACKFILL_YEARS}y"
    elif FORCE_RELOAD_DAYS > 0:
        start = (today - timedelta(days=FORCE_RELOAD_DAYS)).isoformat()
        end   = (today + timedelta(days=1)).isoformat()
        mode  = f"force_reload_{FORCE_RELOAD_DAYS}d"
    else:
        mx = exec_one(cur, f"SELECT MAX(CAST(date AS DATE)) FROM {CATALOG}.{SCHEMA}.{TABLE}")
        if mx:
            start = (pd.to_datetime(mx).date() + timedelta(days=1)).isoformat()
        else:
            start = (today - timedelta(days=3650)).isoformat()
        end  = (today + timedelta(days=1)).isoformat()
        mode = "incremental"
    return start, end, mode

def silver_count(cur, sym: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_prices WHERE symbol='{qfmt(sym)}'")
    return cur.fetchone()[0]

# === Fetch ===
def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[-1].lower() if isinstance(c, tuple) else str(c).lower() for c in df.columns]
    else:
        df.columns = [str(c).lower() for c in df.columns]
    return df

def fetch(symbol: str, start: str, end: str) -> pd.DataFrame:
    try:
        df = yf.download(symbol, start=start, end=end, interval="1d", auto_adjust=False, progress=False)
    except Exception as e:
        print(f"[WARN] fetch fail {symbol}: {e}")
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.reset_index()
    df = _flatten_columns(df)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    # handle yfinance column oddities
    rename_map = {"adj close": "adj_close"}
    for k in list(df.columns):
        if k.startswith("adj close"):
            rename_map[k] = "adj_close"
    df = df.rename(columns=rename_map)
    for c in ["open","high","low","close","adj_close","volume"]:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[["date","open","high","low","close","adj_close","volume"]]
    df["symbol"] = symbol
    df = df[["symbol","date","open","high","low","close","adj_close","volume"]]
    df = df[~df["close"].isna()]
    return df

def fetch_with_timeout(symbol, start, end):
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(fetch, symbol, start, end)
        try:
            return fut.result(timeout=FETCH_TIMEOUT)
        except FutTimeout:
            print(f"[TIMEOUT] {symbol} >{FETCH_TIMEOUT}s; skip")
            return pd.DataFrame()
        except Exception as e:
            print(f"[WARN] {symbol} exception: {e}")
            return pd.DataFrame()

# === MERGE ===
def to_values(rows):
    out = []
    for (sym, dt, o,h,l,c,adj,v) in rows:
        sym_s = f"'{qfmt(sym)}'"
        dt_s  = f"'{dt}'"
        def num(x):
            if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
                return "NULL"
            return str(x)
        o_s,h_s,l_s,c_s,adj_s,v_s = map(num, (o,h,l,c,adj,v))
        out.append(f"({sym_s},{dt_s},{o_s},{h_s},{l_s},{c_s},{adj_s},{v_s},current_timestamp())")
    return ",\n".join(out)

def merge_batch(cur, rows):
    if not rows:
        return
    vals = to_values(rows)
    sql = f"""
MERGE INTO {CATALOG}.{SCHEMA}.{TABLE} AS t
USING (
  VALUES
  {vals}
) AS s(symbol,date,open,high,low,close,adj_close,volume,_ingest_ts)
ON t.symbol = s.symbol AND CAST(t.date AS DATE) = CAST(s.date AS DATE)
WHEN MATCHED THEN UPDATE SET
  t.open = s.open, t.high = s.high, t.low = s.low, t.close = s.close,
  t.adj_close = s.adj_close, t.volume = s.volume, t._ingest_ts = s._ingest_ts
WHEN NOT MATCHED THEN INSERT
  (symbol,date,open,high,low,close,adj_close,volume,_ingest_ts)
  VALUES (s.symbol,s.date,s.open,s.high,s.low,s.close,s.adj_close,s.volume,s._ingest_ts)
"""
    cur.execute(sql)

# === Main ===
def main():
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT current_catalog(), current_schema(), current_user()")
        print("CTX:", cur.fetchone())

        syms = load_symbols()

        before = None
        cur.execute(f"SELECT COUNT(1), MIN(date), MAX(date) FROM {CATALOG}.{SCHEMA}.{TABLE}")
        before = cur.fetchone()
        print("BRONZE_BEFORE:", before)

        start_g, end_g, mode = plan_window_global(cur)
        print(f"Ingest mode: {mode} | Range: {start_g} → {end_g} | Symbols: {len(syms)}")

        frames, total_rows = [], 0

        def fetch_one(s):
            start, end = start_g, end_g
            if SMART_BACKFILL:
                try:
                    n = silver_count(cur, s)
                    if n < SMART_MIN_ROWS:
                        start = (date.today() - timedelta(days=3650)).isoformat()
                except Exception:
                    pass
            df = fetch_with_timeout(s, start, end)
            if df is not None and not df.empty:
                r = (df["date"].min(), df["date"].max(), len(df))
                print(f"[DATA] {s}: {r[0]} → {r[1]} ({r[2]} rows)")
            return df

        # parallel fetch
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futs = {pool.submit(fetch_one, s): s for s in syms}
            i, n = 0, len(futs)
            for fut in futs:
                df = fut.result()
                i += 1
                if i % 25 == 0 or i == n:
                    print(f"[PROGRESS] {i}/{n} symbols")
                if df is not None and not df.empty:
                    frames.append(df)
                    total_rows += len(df)

        if not frames:
            print("No rows to upsert.")
        else:
            all_df = pd.concat(frames, ignore_index=True)
            all_df = all_df[["symbol","date","open","high","low","close","adj_close","volume"]]
            rows = list(map(tuple, all_df.itertuples(index=False, name=None)))

            print(f"[DEBUG] fetched_rows_total={total_rows}")
            print(f"[DEBUG] values_rows_prepared={len(rows)}")

            for i in range(0, len(rows), BATCH_ROWS):
                if i == 0 and rows:
                    # show first tuple preview
                    preview = rows[0]
                    p = (preview[0], preview[1]) + tuple(
                        (None if (isinstance(x, float) and math.isnan(x)) else x) for x in preview[2:]
                    )
                    print("[MERGE] first_tuple:\n", f"('{p[0]}','{p[1]}',{p[2] if p[2] is not None else 'NULL'},{p[3] if p[3] is not None else 'NULL'},{p[4] if p[4] is not None else 'NULL'},{p[5] if p[5] is not None else 'NULL'},{p[6] if p[6] is not None else 'NULL'},{p[7] if p[7] is not None else 'NULL'},current_timestamp())")
                merge_batch(cur, rows[i:i+BATCH_ROWS])

        cur.execute(f"SELECT COUNT(1), MIN(date), MAX(date) FROM {CATALOG}.{SCHEMA}.{TABLE}")
        after = cur.fetchone()
        print("BRONZE_AFTER:", after)

        print(f"Upserted {total_rows if frames else 0} rows into {CATALOG}.{SCHEMA}.{TABLE}.")

if __name__ == "__main__":
    main()
