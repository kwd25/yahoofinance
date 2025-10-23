#!/usr/bin/env python3
"""
Backfill selected symbols for the last 10 years into workspace.yahoo.bronze_prices.

- Uses yfinance for daily OHLCV
- Uses Databricks SQL Connector (DB-API) with env secrets:
    DATABRICKS_SERVER   = adb-xxxxxxxx.azuredatabricks.net          (NO https://, no trailing slash)
    DATABRICKS_HTTP_PATH= /sql/1.0/warehouses/<WAREHOUSE_ID>
    DATABRICKS_TOKEN    = dapiXXXXXXXXXXXX

Run this via GitHub Actions (after your separate "start warehouse" action) or locally.
"""

import os
import re
import sys
import uuid
import time
from datetime import date, timedelta
from typing import List

import pandas as pd
import yfinance as yf
import requests
from databricks import sql
from databricks.sql.exc import RequestError

# ------------------------- Config -------------------------
CATALOG = "workspace"
SCHEMA  = "yahoo"
TABLE   = f"{CATALOG}.{SCHEMA}.bronze_prices"

SYMBOLS: List[str] = [
    "DGX","GWW","HOOD","LUV","MCO","MOS","MSCI","MSI","NCLH","NDAQ","NDSN","NEE","NEM","NI","NOC","NOW","NRG",
    "NSC","NTAP","NTRS","NUE","NVR","NWS","NWSA","NXPI","O","ODFL","OKE","OMC","ON","ORLY","OTIS","OXY","PANW",
    "PAYC","PAYX","PCAR","PCG","PEG","PFE","PFG","PGR","PH","PHM","PKG","PLD","PLTR","PM","PNC","PNR","PNW","POOL",
    "PPG","PPL","PRU","PSA","PSKY","PSX","PTC","PWR","PYPL","QCOM","RCL","REG","REGN","RF","RJF","RL","RMD","ROK",
    "ROL","ROP","ROST","RSG","RTX","RVTY","SBAC","SBUX","SHOP","SHW","SJM","SLB","SMCI","SNA","SNPS","SO","SOLV",
    "SPG","SPGI","SRE","STE","STLD","STT","STX","SW","SWK","SWKS","SYF","SYK","SYY","TDG","TDY","TEL","TER","TFC",
    "TGT","TJX","TKO","TMO","TMUS","TPL","TPR","TRGP","TRMB","TROW","TRV","TSCO","TSM","TSN","TT","TTD","TTWO",
    "TXN","TXT","TYL","UAL","UBER","UDR","UHS","ULTA","UNP","UPS","URI","USB","VICI","VLO","VLTO","VMC","VRSK",
    "VRSN","VRTX","VST","VTR","VTRS","VZ","WAB","WAT","WBD","WDAY","WDC","WEC","WELL","WM","WMB","WRB","WSM",
    "WST","WTW","WY","WYNN","XEL","XYL","YUM","ZBH","ZBRA","ZTS"
]

YEARS_BACK = 10
BATCH_SIZE = 25          # yahoo download batch
INSERT_CHUNK = 10_000    # DB executemany insert chunk for staging
CONNECT_RETRIES = 6

# --------------------- Env & Helpers ----------------------
SERVER    = os.getenv("DATABRICKS_SERVER", "").replace("https://","").replace("http://","").strip("/")
HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH", "")
TOKEN     = os.getenv("DATABRICKS_TOKEN", "")

if not SERVER or not HTTP_PATH or not TOKEN:
    print("ERROR: Missing one or more env vars: DATABRICKS_SERVER, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN", file=sys.stderr)
    sys.exit(2)

m = re.search(r"/warehouses/([A-Za-z0-9\-]+)", HTTP_PATH)
WAREHOUSE_ID = m.group(1) if m else None

def warehouse_state() -> str | None:
    """Return current warehouse state or None if unknown."""
    if not WAREHOUSE_ID:
        return None
    try:
        r = requests.get(
            f"https://{SERVER}/api/2.0/sql/warehouses/{WAREHOUSE_ID}",
            headers={"Authorization": f"Bearer {TOKEN}"},
            timeout=15
        )
        if r.ok:
            return (r.json().get("state") or "").upper()
    except Exception:
        pass
    return None

def wait_for_running(max_wait_s: int = 300):
    """Poll state until RUNNING or timeout. Does NOT start the warehouse."""
    print(f"Checking warehouse state for up to {max_wait_s}s…")
    start = time.time()
    last = None
    while time.time() - start < max_wait_s:
        s = warehouse_state()
        if s != last and s is not None:
            print(f"  state={s}")
            last = s
        if s == "RUNNING":
            return
        time.sleep(5)
    print("WARNING: Timed out waiting for RUNNING. Will still attempt to connect…")

def connect_with_backoff():
    """Connect to Databricks SQL with retries (covers warm-up)."""
    delay = 2
    for attempt in range(1, CONNECT_RETRIES + 1):
        try:
            conn = sql.connect(
                server_hostname=SERVER,
                http_path=HTTP_PATH,
                access_token=TOKEN,
            )
            # sanity query (also triggers auto-start if allowed)
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchall()
            return conn
        except RequestError as e:
            print(f"[WARN] connect attempt {attempt} failed: {e}")
            time.sleep(delay)
            delay = min(delay * 2, 30)
    raise RuntimeError("Failed to connect to Databricks SQL after retries.")

# -------------------- Data Download -----------------------
END = date.today()
START = END - timedelta(days=YEARS_BACK*365 + 5)  # cushion

def download_batch(tickers: list[str]) -> pd.DataFrame:
    df = yf.download(
        tickers=" ".join(tickers),
        start=START.isoformat(),
        end=(END + timedelta(days=1)).isoformat(),  # yfinance end is exclusive
        interval="1d",
        auto_adjust=False,
        group_by="ticker",
        threads=True,
        progress=False,     # ← hides the 0–100% bars per batch
    )
    if df is None or df.empty:
        return pd.DataFrame()

    frames = []
    # correct class name: pd.MultiIndex
    if isinstance(df.columns, pd.MultiIndex):
        # Multi-ticker case
        for t in tickers:
            if t in df.columns.levels[0]:
                sub = df[t].copy()
                sub["symbol"] = t
                frames.append(sub.reset_index())
    else:
        # Single-ticker fallback
        df = df.reset_index().copy()
        df["symbol"] = tickers[0]
        frames.append(df)

    out = (pd.concat(frames, ignore_index=True)
             .rename(columns={
                 "Date":"date","Open":"open","High":"high","Low":"low","Close":"close",
                 "Adj Close":"adj_close","Volume":"volume"
             })[["symbol","date","open","high","low","close","adj_close","volume"]])

    # normalize to midnight timestamps, no tz
    out["date"] = pd.to_datetime(out["date"]).dt.tz_localize(None).dt.normalize()

    # cast + clean
    for c in ["open","high","low","close","adj_close"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce").astype("Int64")
    out = out.dropna(subset=["date","close"])
    return out

def download_all() -> pd.DataFrame:
    all_parts: list[pd.DataFrame] = []
    print(f"Downloading {len(SYMBOLS)} symbols from {START} to {END} in batches of {BATCH_SIZE}…")
    for i in range(0, len(SYMBOLS), BATCH_SIZE):
        batch = SYMBOLS[i:i+BATCH_SIZE]
        tag = f"{batch[0]}…{batch[-1]}"
        for attempt in range(1, 4):
            try:
                part = download_batch(batch)
                print(f"  batch {i//BATCH_SIZE+1}: {tag} -> {len(part):,} rows")
                if not part.empty:
                    all_parts.append(part)
                break
            except Exception as e:
                print(f"[WARN] batch {tag} attempt {attempt} failed: {e}")
                time.sleep(2 * attempt)
    if not all_parts:
        raise RuntimeError("No data downloaded for any symbol.")
    df = pd.concat(all_parts, ignore_index=True)
    # dedupe on (symbol,date)
    before = len(df)
    df = df.drop_duplicates(subset=["symbol","date"], keep="last").sort_values(["symbol","date"])
    print(f"Downloaded rows: {before:,} → {len(df):,} after de-dup")
    return df

# ---------------------- Upload / Merge --------------------
def ensure_target_and_stage(cur, staging: str):
    # target (unchanged)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
          date TIMESTAMP,
          open DOUBLE,
          high DOUBLE,
          low DOUBLE,
          close DOUBLE,
          adj_close DOUBLE,
          volume BIGINT,
          symbol STRING
        )
        USING delta
        PARTITIONED BY (symbol)
    """)
    # staging (no table feature property)
    cur.execute(f"""
        CREATE OR REPLACE TABLE {staging} (
          symbol STRING,
          date TIMESTAMP,
          open DOUBLE,
          high DOUBLE,
          low DOUBLE,
          close DOUBLE,
          adj_close DOUBLE,
          volume BIGINT
        )
        USING delta
    """)
    print(f"Created staging table: {staging}")
    

def insert_into_staging(cur, staging: str, rows: list[tuple]):
    insert_sql = f"""
        INSERT INTO {staging} (symbol, date, open, high, low, close, adj_close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    total = len(rows)
    if total == 0:
        print("No rows to insert into staging.")
        return
    for i in range(0, total, INSERT_CHUNK):
        chunk = rows[i:i+INSERT_CHUNK]
        cur.executemany(insert_sql, chunk)
        print(f"  staged {min(i+INSERT_CHUNK, total):,}/{total:,} rows")

def merge_from_staging(cur, staging: str):
    print("Merging staging into bronze…")
    cur.execute(f"""
        MERGE INTO {TABLE} AS tgt
        USING {staging} AS src
          ON  tgt.symbol = src.symbol
          AND tgt.date   = src.date
        WHEN MATCHED THEN UPDATE SET
          tgt.open      = src.open,
          tgt.high      = src.high,
          tgt.low       = src.low,
          tgt.close     = src.close,
          tgt.adj_close = src.adj_close,
          tgt.volume    = src.volume
        WHEN NOT MATCHED THEN INSERT (date, open, high, low, close, adj_close, volume, symbol)
        VALUES (src.date, src.open, src.high, src.low, src.close, src.adj_close, src.volume, src.symbol)
    """)
    print("Merge complete.")

def main():
    # Wait (non-start) for warehouse to be RUNNING; then connect with backoff.
    wait_for_running(max_wait_s=300)
    conn = connect_with_backoff()

    try:
        data = download_all()
        # Build DB rows (keep timestamp at midnight)
        rows = [
            (
                r.symbol,
                r.date.strftime("%Y-%m-%d 00:00:00"),
                float(r.open) if pd.notna(r.open) else None,
                float(r.high) if pd.notna(r.high) else None,
                float(r.low)  if pd.notna(r.low)  else None,
                float(r.close) if pd.notna(r.close) else None,
                float(r.adj_close) if pd.notna(r.adj_close) else None,
                int(r.volume) if pd.notna(r.volume) else None,
            )
            for r in data.itertuples()
        ]
        print(f"Prepared {len(rows):,} rows for staging.")

        staging = f"{CATALOG}.{SCHEMA}._backfill_stage_{uuid.uuid4().hex[:8]}"
        with conn.cursor() as cur:
            ensure_target_and_stage(cur, staging)
            insert_into_staging(cur, staging, rows)
            merge_from_staging(cur, staging)
            cur.execute(f"DROP TABLE IF EXISTS {staging}")
            print("Dropped staging table.")
        conn.commit()
        print("Backfill complete!")
    finally:
        conn.close()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
