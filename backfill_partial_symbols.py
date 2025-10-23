"""
Backfill selected symbols for the last 10 years into workspace.yahoo.bronze_prices
Uses yfinance + Databricks SQL Connector
Run manually or as a GitHub Action.
"""

import os
import time
from datetime import date, timedelta
import pandas as pd
import yfinance as yf
from databricks import sql

# --- Read secrets from environment ---
SERVER = os.getenv("DATABRICKS_SERVER")
HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
TOKEN = os.getenv("DATABRICKS_TOKEN")

CATALOG = "workspace"
SCHEMA = "yahoo"
TABLE = f"{CATALOG}.{SCHEMA}.bronze_prices"

# --- Symbols to backfill ---
SYMBOLS = [
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

START = date.today() - timedelta(days=365*10 + 5)
END = date.today()

# --- Helper: download from Yahoo ---
def download_batch(tickers):
    df = yf.download(
        tickers=" ".join(tickers),
        start=START.isoformat(),
        end=(END + timedelta(days=1)).isoformat(),
        interval="1d",
        auto_adjust=False,
        group_by="ticker",
        threads=True
    )
    if df is None or df.empty:
        return pd.DataFrame()

    frames = []
    if isinstance(df.columns, pd.MultiIndex):
        for t in tickers:
            if t in df.columns.levels[0]:
                sub = df[t].copy()
                sub["symbol"] = t
                frames.append(sub.reset_index())
    else:
        df = df.reset_index()
        df["symbol"] = tickers[0]
        frames.append(df)

    pdf = pd.concat(frames, ignore_index=True)
    pdf = pdf.rename(columns={
        "Date":"date","Open":"open","High":"high","Low":"low","Close":"close",
        "Adj Close":"adj_close","Volume":"volume"
    })[["symbol","date","open","high","low","close","adj_close","volume"]]
    pdf["date"] = pd.to_datetime(pdf["date"]).dt.tz_localize(None)
    return pdf.dropna(subset=["date", "close"])

# --- Download all in batches ---
all_batches = []
for i in range(0, len(SYMBOLS), 25):
    batch = SYMBOLS[i:i+25]
    for attempt in range(3):
        try:
            df = download_batch(batch)
            if not df.empty:
                all_batches.append(df)
            break
        except Exception as e:
            print(f"[WARN] batch {batch[0]}-{batch[-1]} retry {attempt+1}: {e}")
            time.sleep(2 + 2*attempt)

if not all_batches:
    raise RuntimeError("No data downloaded!")

data = pd.concat(all_batches, ignore_index=True)
data = data.drop_duplicates(subset=["symbol","date"]).sort_values(["symbol","date"])

# --- Upload to Databricks ---
insert_values = [
    (
        row.symbol,
        row.date.strftime("%Y-%m-%d 00:00:00"),
        float(row.open) if pd.notna(row.open) else None,
        float(row.high) if pd.notna(row.high) else None,
        float(row.low) if pd.notna(row.low) else None,
        float(row.close) if pd.notna(row.close) else None,
        float(row.adj_close) if pd.notna(row.adj_close) else None,
        int(row.volume) if pd.notna(row.volume) else None
    )
    for row in data.itertuples()
]

# Batch insert to avoid payload limits
CHUNK = 500
print(f"Uploading {len(insert_values)} records to {TABLE}...")

with sql.connect(
    server_hostname=SERVER,
    http_path=HTTP_PATH,
    access_token=TOKEN,
) as conn:
    with conn.cursor() as cur:
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
        for i in range(0, len(insert_values), CHUNK):
            chunk = insert_values[i:i+CHUNK]
            cur.executemany(f"""
                MERGE INTO {TABLE} AS tgt
                USING (SELECT ? AS symbol, ? AS date, ? AS open, ? AS high, ? AS low,
                             ? AS close, ? AS adj_close, ? AS volume) src
                ON tgt.symbol = src.symbol AND tgt.date = src.date
                WHEN MATCHED THEN UPDATE SET
                    tgt.open = src.open,
                    tgt.high = src.high,
                    tgt.low = src.low,
                    tgt.close = src.close,
                    tgt.adj_close = src.adj_close,
                    tgt.volume = src.volume
                WHEN NOT MATCHED THEN INSERT *
            """, chunk)
        conn.commit()

print(" Backfill complete!")
