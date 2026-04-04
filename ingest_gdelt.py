"""
ingest_gdelt.py

Pulls articles from the GDELT DOC 2.0 API for a set of keywords,
deduplicates against MongoDB, and inserts new results into the
geoalpha.gdelt_articles collection.

Also runs a timelinetone query for each keyword and stores daily average
tone readings in the geoalpha.gdelt_tone_timelines collection, deduplicated
on (query_keyword, date).

A summary run record is written to geoalpha.pipeline_runs at the end of
each execution.
"""

import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from gdeltdoc import GdeltDoc, Filters
from gdeltdoc.errors import RateLimitError
from pymongo import MongoClient, ASCENDING

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()

MONGODB_URI   = os.environ["MONGODB_URI"]
DB_NAME       = "geoalpha"
ARTICLES_COL  = "gdelt_articles"
TONE_COL      = "gdelt_tone_timelines"
RUNS_COL      = "pipeline_runs"

# Keywords to query — Hormuz oil signal thesis
KEYWORDS = [
    "Strait of Hormuz",
    "Iran sanctions",
    "Iran nuclear",
    "Persian Gulf tanker",
    "Iran oil embargo",
    "Gulf oil supply",
    "Iran IRGC",
    "Iran United States",
    "Hormuz closure",
    "Iran missile",
]

# GDELT lookback window (days)
LOOKBACK_DAYS = 1

# Seconds to wait between keyword requests (GDELT enforces a per-minute rate limit)
REQUEST_DELAY_SECONDS = 15

# Retry config for 429 rate-limit responses
RETRY_MAX_ATTEMPTS = 5
RETRY_BACKOFF_SECONDS = 30  # doubles on each attempt: 30, 60, 120, 240


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_run_id() -> str:
    """Return a deterministic run identifier based on the current UTC time."""
    return datetime.now(timezone.utc).strftime("run_%Y%m%d_%H%M%S")


def fetch_articles(keyword: str, timespan_days: int) -> list[dict]:
    """
    Query GDELT DOC 2.0 for articles matching *keyword* within the
    last *timespan_days* days.  Retries with exponential backoff on
    429 rate-limit responses.  Returns a list of article dicts.
    """
    gd = GdeltDoc()
    filters = Filters(
        keyword=keyword,
        timespan=f"{timespan_days}d",
    )

    wait = RETRY_BACKOFF_SECONDS
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            # article_search returns a pandas DataFrame; convert to records
            df = gd.article_search(filters)
            if df is None or df.empty:
                return []
            return df.to_dict(orient="records")
        except (RateLimitError, requests.exceptions.ConnectionError, ConnectionError):
            if attempt == RETRY_MAX_ATTEMPTS:
                raise  # exhausted retries — let caller handle it
            print(f"  Network error (attempt {attempt}/{RETRY_MAX_ATTEMPTS}), waiting {wait}s before retry...")
            time.sleep(wait)
            wait *= 2  # exponential backoff: 30 → 60 → 120 → 240


def parse_seendate(raw: str | None) -> datetime | None:
    """
    Convert a GDELT seendate string (e.g. '20260320T143000Z') to a UTC datetime.
    Returns None if the value is missing or unparseable.
    """
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def fetch_tone_timeline(keyword: str, timespan_days: int) -> list[dict]:
    """
    Query GDELT DOC 2.0 for the daily average tone timeline matching *keyword*
    within the last *timespan_days* days.  Retries with exponential backoff on
    rate-limit and connection errors.

    Returns a list of row dicts with keys 'datetime' (pandas Timestamp) and
    'Average Tone' (float).  Returns an empty list when GDELT has no data.
    """
    gd = GdeltDoc()
    filters = Filters(
        keyword=keyword,
        timespan=f"{timespan_days}d",
    )

    wait = RETRY_BACKOFF_SECONDS
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            df = gd.timeline_search("timelinetone", filters)
            if df is None or df.empty:
                return []
            return df.to_dict(orient="records")
        except (RateLimitError, requests.exceptions.ConnectionError, ConnectionError):
            if attempt == RETRY_MAX_ATTEMPTS:
                raise
            print(f"  Network error (attempt {attempt}/{RETRY_MAX_ATTEMPTS}), waiting {wait}s before retry...")
            time.sleep(wait)
            wait *= 2


def build_tone_document(row: dict, keyword: str, run_id: str) -> dict:
    """
    Map a single timelinetone DataFrame row to the canonical gdelt_tone_timelines
    document shape.

    GDELT returns the datetime as a pandas Timestamp and the tone series under
    the column name 'Average Tone'.
    """
    # Convert pandas Timestamp to a timezone-aware Python datetime (UTC)
    raw_dt = row["datetime"]
    if hasattr(raw_dt, "to_pydatetime"):
        raw_dt = raw_dt.to_pydatetime()
    if raw_dt.tzinfo is None:
        raw_dt = raw_dt.replace(tzinfo=timezone.utc)

    return {
        "source":          "gdelt_tone",
        "query_keyword":   keyword,
        "date":            raw_dt,
        "tone_score":      float(row["Average Tone"]),
        "ingested_at":     datetime.now(timezone.utc),
        "pipeline_run_id": run_id,
    }


def build_document(article: dict, keyword: str, run_id: str) -> dict:
    """
    Map a raw GDELT article dict to the canonical MongoDB document shape.
    Sentiment fields are left null — they will be populated by a later
    enrichment stage in the pipeline.
    """
    return {
        "source":                 "gdelt",
        "ingested_at":            datetime.now(timezone.utc),
        "query_keyword":          keyword,
        # Core GDELT fields (use .get() so missing fields become None gracefully)
        "url":                    article.get("url"),
        "title":                  article.get("title"),
        "seendate":               parse_seendate(article.get("seendate")),
        "domain":                 article.get("domain"),
        "language":               article.get("language"),
        "sourcecountry":          article.get("sourcecountry"),
        # Sentiment — populated by the NLP enrichment stage
        "sentiment_score":        None,
        "sentiment_label":        None,
        "sentiment_processed_at": None,
        # Lineage
        "pipeline_run_id":        run_id,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def ingest_gdelt() -> dict:
    """
    Execute the full GDELT ingestion cycle.

    Queries every keyword in KEYWORDS, deduplicates against MongoDB, inserts
    new articles, and writes a per-task record to pipeline_runs.

    Returns a result dict:
        task              — "gdelt"
        run_id            — pipeline run identifier (run_YYYYMMDD_HHMMSS)
        inserted          — total articles inserted this run
        tone_inserted     — total tone timeline rows inserted this run
        errors            — list of error strings encountered
        status            — "success" | "partial" | "failed"
    """
    run_id     = build_run_id()
    started_at = datetime.now(timezone.utc)

    client = MongoClient(MONGODB_URI)
    db     = client[DB_NAME]

    articles_col = db[ARTICLES_COL]
    tone_col     = db[TONE_COL]
    runs_col     = db[RUNS_COL]

    # Unique index on URL for fast deduplication of articles
    articles_col.create_index([("url", ASCENDING)], unique=True, background=True)

    # Compound unique index on (query_keyword, date) for tone deduplication
    tone_col.create_index(
        [("query_keyword", ASCENDING), ("date", ASCENDING)],
        unique=True,
        background=True,
    )

    total_inserted      = 0
    total_tone_inserted = 0
    errors: list[str]   = []

    for i, keyword in enumerate(KEYWORDS):
        # GDELT DOC API enforces a ~5 second rate limit between requests
        if i > 0:
            time.sleep(REQUEST_DELAY_SECONDS)

        print(f"[{run_id}] Querying GDELT for: '{keyword}' (last {LOOKBACK_DAYS} days)")

        # ------------------------------------------------------------------
        # Article search
        # ------------------------------------------------------------------
        try:
            raw_articles = fetch_articles(keyword, LOOKBACK_DAYS)
        except Exception as exc:
            # Include exception type and HTTP status code (if present) so we can
            # distinguish rate limit (429) from bad request (400) from other errors
            status = getattr(getattr(exc, "response", None), "status_code", None)
            status_str = f" [HTTP {status}]" if status else ""
            msg = f"GDELT article fetch failed for '{keyword}': {type(exc).__name__}{status_str}: {exc}"
            print(f"  ERROR: {msg}")
            errors.append(msg)
            raw_articles = []

        print(f"  {len(raw_articles)} articles returned by GDELT")

        if not raw_articles:
            print(f"  No articles for '{keyword}' — skipping tone timeline query")
            continue

        inserted_count = 0
        for article in raw_articles:
            url = article.get("url")
            if not url:
                continue  # skip malformed records with no URL

            # Deduplication: skip if this URL is already in the collection
            if articles_col.find_one({"url": url}, {"_id": 1}):
                continue

            doc = build_document(article, keyword, run_id)
            try:
                articles_col.insert_one(doc)
                inserted_count += 1
            except Exception as exc:
                msg = f"Insert failed for URL '{url}': {exc}"
                print(f"  ERROR: {msg}")
                errors.append(msg)

        print(f"  Inserted {inserted_count} new articles for '{keyword}'")
        total_inserted += inserted_count

        # ------------------------------------------------------------------
        # Tone timeline search — runs after article search, with its own
        # rate-limit delay since it is a second API call for this keyword
        # ------------------------------------------------------------------
        time.sleep(REQUEST_DELAY_SECONDS)

        print(f"[{run_id}] Querying GDELT tone timeline for: '{keyword}'")

        try:
            raw_tone_rows = fetch_tone_timeline(keyword, LOOKBACK_DAYS)
        except Exception as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            status_str = f" [HTTP {status}]" if status else ""
            msg = f"GDELT tone fetch failed for '{keyword}': {type(exc).__name__}{status_str}: {exc}"
            print(f"  ERROR: {msg}")
            errors.append(msg)
            raw_tone_rows = []

        print(f"  {len(raw_tone_rows)} tone data points returned by GDELT")

        tone_inserted_count = 0
        for row in raw_tone_rows:
            doc = build_tone_document(row, keyword, run_id)

            # Deduplication: skip if this (keyword, date) pair already exists
            if tone_col.find_one(
                {"query_keyword": keyword, "date": doc["date"]}, {"_id": 1}
            ):
                continue

            try:
                tone_col.insert_one(doc)
                tone_inserted_count += 1
            except Exception as exc:
                msg = f"Tone insert failed for '{keyword}' on {doc['date']}: {exc}"
                print(f"  ERROR: {msg}")
                errors.append(msg)

        print(f"  Inserted {tone_inserted_count} new tone rows for '{keyword}'")
        total_tone_inserted += tone_inserted_count

    completed_at = datetime.now(timezone.utc)

    # Determine task-level status
    if not errors:
        task_status = "success"
    elif total_inserted > 0 or total_tone_inserted > 0:
        task_status = "partial"
    else:
        task_status = "failed"

    # Write per-task pipeline run record (useful for standalone runs and debugging)
    run_record = {
        "run_id":                       run_id,
        "started_at":                   started_at,
        "completed_at":                 completed_at,
        "status":                       "completed" if not errors else "completed_with_errors",
        "keywords_queried":             KEYWORDS,
        "gdelt_articles_inserted":      total_inserted,
        "gdelt_tone_rows_inserted":     total_tone_inserted,
        "errors":                       errors,
    }
    runs_col.insert_one(run_record)

    print(
        f"\n[{run_id}] Done — {total_inserted} articles, "
        f"{total_tone_inserted} tone rows inserted. "
        f"Status: {run_record['status']}"
    )

    client.close()

    return {
        "task":          "gdelt",
        "run_id":        run_id,
        "inserted":      total_inserted,
        "tone_inserted": total_tone_inserted,
        "errors":        errors,
        "status":        task_status,
    }


def main() -> None:
    """Standalone entrypoint — calls ingest_gdelt() directly."""
    ingest_gdelt()


if __name__ == "__main__":
    main()
