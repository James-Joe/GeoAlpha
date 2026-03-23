"""
ingest_gdelt.py

Pulls articles from the GDELT DOC 2.0 API for a set of keywords,
deduplicates against MongoDB, and inserts new results into the
geoalpha.gdelt_articles collection.  A summary run record is written
to geoalpha.pipeline_runs at the end of each execution.
"""

import os
import time
from datetime import datetime, timezone, timedelta

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
RUNS_COL      = "pipeline_runs"

# Keywords to query — extend this list to add more signals
KEYWORDS = [
    "Russia sanctions",
    "Strait of Hormuz tanker",
    "Red Sea shipping attack",
    "Suez Canal blocked",
    "South China Sea vessel",
    "Arctic shipping route",
    "Black Sea grain ship",
    "shipping lane disruption",
]

# GDELT lookback window (days)
LOOKBACK_DAYS = 7

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
        except RateLimitError:
            if attempt == RETRY_MAX_ATTEMPTS:
                raise  # exhausted retries — let caller handle it
            print(f"  Rate limited (attempt {attempt}/{RETRY_MAX_ATTEMPTS}), waiting {wait}s before retry...")
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

def main() -> None:
    run_id     = build_run_id()
    started_at = datetime.now(timezone.utc)

    client = MongoClient(MONGODB_URI)
    db     = client[DB_NAME]

    articles_col = db[ARTICLES_COL]
    runs_col     = db[RUNS_COL]

    # Ensure a unique index on URL so duplicate checks are fast
    articles_col.create_index([("url", ASCENDING)], unique=True, background=True)

    total_inserted = 0
    errors: list[str] = []

    for i, keyword in enumerate(KEYWORDS):
        # GDELT DOC API enforces a ~5 second rate limit between requests
        if i > 0:
            time.sleep(REQUEST_DELAY_SECONDS)

        print(f"[{run_id}] Querying GDELT for: '{keyword}' (last {LOOKBACK_DAYS} days)")

        try:
            raw_articles = fetch_articles(keyword, LOOKBACK_DAYS)
        except Exception as exc:
            # Include exception type and HTTP status code (if present) so we can
            # distinguish rate limit (429) from bad request (400) from other errors
            status = getattr(getattr(exc, "response", None), "status_code", None)
            status_str = f" [HTTP {status}]" if status else ""
            msg = f"GDELT fetch failed for '{keyword}': {type(exc).__name__}{status_str}: {exc}"
            print(f"  ERROR: {msg}")
            errors.append(msg)
            continue

        print(f"  {len(raw_articles)} articles returned by GDELT")

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

    completed_at = datetime.now(timezone.utc)

    # Write pipeline run summary
    run_record = {
        "run_id":                  run_id,
        "started_at":              started_at,
        "completed_at":            completed_at,
        "status":                  "completed" if not errors else "completed_with_errors",
        "keywords_queried":        KEYWORDS,
        "gdelt_articles_inserted": total_inserted,
        "errors":                  errors,
    }
    runs_col.insert_one(run_record)

    print(
        f"\n[{run_id}] Done — {total_inserted} articles inserted. "
        f"Status: {run_record['status']}"
    )

    client.close()


if __name__ == "__main__":
    main()
