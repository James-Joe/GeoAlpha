"""
ingest_shipping.py

Pulls daily crude futures and tanker stock data from yfinance,
deduplicates against MongoDB on (ticker, date), and inserts new
data points into the geoalpha.shipping_signals collection.  A summary
run record is written to geoalpha.pipeline_runs at the end of each
execution.
"""

import os
from datetime import datetime, timezone

import yfinance as yf
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()

MONGODB_URI   = os.environ["MONGODB_URI"]
DB_NAME       = "geoalpha"
SIGNALS_COL   = "shipping_signals"
RUNS_COL      = "pipeline_runs"

# Tickers to ingest — each entry maps a symbol to a human-readable name
TICKERS = [
    {"symbol": "BZ=F", "name": "Brent Crude Futures"},
    {"symbol": "CL=F", "name": "WTI Crude Futures"},
    {"symbol": "FRO",  "name": "Frontline (Crude Tanker)"},
    {"symbol": "STNG", "name": "Scorpio Tankers"},
    {"symbol": "USO",  "name": "US Oil ETF"},
]

# How many calendar days of history to pull on each run
LOOKBACK_DAYS = 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_run_id() -> str:
    """Return a deterministic run identifier based on the current UTC time."""
    return datetime.now(timezone.utc).strftime("run_%Y%m%d_%H%M%S")


def fetch_history(symbol: str, lookback_days: int) -> list[dict]:
    """
    Download OHLCV history for *symbol* covering the last *lookback_days*
    calendar days via yfinance.  Returns a list of row dicts, each
    carrying the date as a UTC-aware datetime object.
    """
    ticker_obj = yf.Ticker(symbol)
    df = ticker_obj.history(period=f"{lookback_days}d")

    if df is None or df.empty:
        return []

    rows = []
    for ts, row in df.iterrows():
        # ts is a pandas Timestamp; normalise to midnight UTC so the
        # date field represents the trading day unambiguously
        date_utc = datetime(ts.year, ts.month, ts.day, tzinfo=timezone.utc)
        rows.append({
            "date":   date_utc,
            "open":   float(row["Open"])   if row["Open"]   is not None else None,
            "high":   float(row["High"])   if row["High"]   is not None else None,
            "low":    float(row["Low"])    if row["Low"]    is not None else None,
            "close":  float(row["Close"])  if row["Close"]  is not None else None,
            "volume": int(row["Volume"])   if row["Volume"] is not None else None,
        })
    return rows


def build_document(row: dict, symbol: str, ticker_name: str, run_id: str) -> dict:
    """
    Map a raw OHLCV row to the canonical shipping_signals document shape.
    """
    return {
        "source":       "yfinance",
        "ticker":       symbol,
        "ticker_name":  ticker_name,
        "date":         row["date"],
        "open":         row["open"],
        "high":         row["high"],
        "low":          row["low"],
        "close":        row["close"],
        "volume":       row["volume"],
        "ingested_at":  datetime.now(timezone.utc),
        "pipeline_run_id": run_id,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def ingest_shipping() -> dict:
    """
    Execute the full yfinance shipping-signals ingestion cycle.

    Downloads OHLCV history for every ticker in TICKERS, deduplicates
    against MongoDB on (ticker, date), inserts new rows, and writes a
    per-task record to pipeline_runs.

    Returns a result dict:
        task     — "shipping"
        run_id   — pipeline run identifier (run_YYYYMMDD_HHMMSS)
        inserted — total data points inserted this run
        errors   — list of error strings encountered
        status   — "success" | "partial" | "failed"
    """
    run_id     = build_run_id()
    started_at = datetime.now(timezone.utc)

    client = MongoClient(MONGODB_URI)
    db     = client[DB_NAME]

    signals_col = db[SIGNALS_COL]
    runs_col    = db[RUNS_COL]

    # Compound unique index enforces deduplication on (ticker, date)
    signals_col.create_index(
        [("ticker", ASCENDING), ("date", ASCENDING)],
        unique=True,
        background=True,
    )

    total_inserted  = 0
    tickers_queried = []
    errors: list[str] = []

    for entry in TICKERS:
        symbol      = entry["symbol"]
        ticker_name = entry["name"]

        print(f"[{run_id}] Fetching {symbol} ({ticker_name}) — last {LOOKBACK_DAYS} days")

        tickers_queried.append(symbol)

        try:
            rows = fetch_history(symbol, LOOKBACK_DAYS)
        except Exception as exc:
            msg = f"yfinance fetch failed for '{symbol}': {exc}"
            print(f"  ERROR: {msg}")
            errors.append(msg)
            continue

        print(f"  {len(rows)} data points returned")

        inserted_count = 0
        for row in rows:
            # Deduplication: skip if this (ticker, date) pair already exists
            if signals_col.find_one(
                {"ticker": symbol, "date": row["date"]},
                {"_id": 1},
            ):
                continue

            doc = build_document(row, symbol, ticker_name, run_id)
            try:
                signals_col.insert_one(doc)
                inserted_count += 1
            except Exception as exc:
                msg = f"Insert failed for {symbol} on {row['date'].date()}: {exc}"
                print(f"  ERROR: {msg}")
                errors.append(msg)

        print(f"  Inserted {inserted_count} new data points for {symbol}")
        total_inserted += inserted_count

    completed_at = datetime.now(timezone.utc)

    # Determine task-level status
    if not errors:
        task_status = "success"
    elif total_inserted > 0:
        task_status = "partial"
    else:
        task_status = "failed"

    # Write per-task pipeline run record (useful for standalone runs and debugging)
    run_record = {
        "run_id":           run_id,
        "started_at":       started_at,
        "completed_at":     completed_at,
        "status":           "completed" if not errors else "completed_with_errors",
        "tickers_queried":  tickers_queried,
        "signals_inserted": total_inserted,
        "errors":           errors,
    }
    runs_col.insert_one(run_record)

    print(
        f"\n[{run_id}] Done — {total_inserted} signals inserted. "
        f"Status: {run_record['status']}"
    )

    client.close()

    return {
        "task":     "shipping",
        "run_id":   run_id,
        "inserted": total_inserted,
        "errors":   errors,
        "status":   task_status,
    }


def main() -> None:
    """Standalone entrypoint — calls ingest_shipping() directly."""
    ingest_shipping()


if __name__ == "__main__":
    main()
