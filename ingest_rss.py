"""
ingest_rss.py

Pulls articles from a set of RSS feeds, deduplicates against MongoDB,
and inserts new results into the geoalpha.rss_articles collection.
A summary run record is written to geoalpha.pipeline_runs at the end
of each execution.
"""

import os
from datetime import datetime, timezone
from urllib.parse import urlparse

import feedparser
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()

MONGODB_URI  = os.environ["MONGODB_URI"]
DB_NAME      = "geoalpha"
ARTICLES_COL = "rss_articles"
RUNS_COL     = "pipeline_runs"

# RSS feeds to ingest — add or remove entries here to change coverage
FEEDS = [
    {"name": "Wall Street Journal", "url": "https://feeds.a.dj.com/rss/RSSWorldNews.xml"},
    {"name": "BBC World News",     "url": "http://feeds.bbci.co.uk/news/world/rss.xml"},
    {"name": "Financial Times",    "url": "https://www.ft.com/rss/home/uk"},
    {"name": "Al Jazeera",         "url": "https://www.aljazeera.com/xml/rss/all.xml"},
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_run_id() -> str:
    """Return a deterministic run identifier based on the current UTC time."""
    return datetime.now(timezone.utc).strftime("run_%Y%m%d_%H%M%S")


def extract_domain(url: str | None) -> str | None:
    """Pull the netloc (e.g. 'www.reuters.com') out of a URL string."""
    if not url:
        return None
    parsed = urlparse(url)
    return parsed.netloc or None


def parse_published_at(entry) -> datetime | None:
    """
    Convert a feedparser entry's published_parsed struct_time (UTC) to a
    timezone-aware datetime.  Falls back to updated_parsed if published
    is absent.  Returns None if neither field is available.
    """
    time_struct = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
    if not time_struct:
        return None
    # struct_time from feedparser is always normalised to UTC
    return datetime(*time_struct[:6], tzinfo=timezone.utc)


def fetch_feed(feed_url: str) -> list:
    """
    Download and parse an RSS feed.  Returns the list of entries on
    success, or raises an exception that the caller should handle.
    """
    parsed = feedparser.parse(feed_url)
    # feedparser never raises — check bozo flag for hard parse errors
    if parsed.bozo and not parsed.entries:
        raise ValueError(f"Feed parse error: {parsed.bozo_exception}")
    return parsed.entries


def build_document(entry, feed_name: str, run_id: str) -> dict:
    """
    Map a raw feedparser entry to the canonical MongoDB document shape.
    Sentiment fields are left null — they will be populated by a later
    enrichment stage in the pipeline.
    """
    url = entry.get("link")
    return {
        "source":                 "rss",
        "feed_name":              feed_name,
        "ingested_at":            datetime.now(timezone.utc),
        # Core article fields
        "title":                  entry.get("title"),
        "summary":                entry.get("summary"),
        "url":                    url,
        "published_at":           parse_published_at(entry),
        "domain":                 extract_domain(url),
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

    for feed in FEEDS:
        feed_name = feed["name"]
        feed_url  = feed["url"]

        print(f"[{run_id}] Fetching feed: '{feed_name}'")

        try:
            entries = fetch_feed(feed_url)
        except Exception as exc:
            msg = f"Feed fetch failed for '{feed_name}': {exc}"
            print(f"  ERROR: {msg}")
            errors.append(msg)
            continue

        print(f"  {len(entries)} entries returned")

        inserted_count = 0
        for entry in entries:
            url = entry.get("link")
            if not url:
                continue  # skip malformed entries with no URL

            # Deduplication: skip if this URL is already in the collection
            if articles_col.find_one({"url": url}, {"_id": 1}):
                continue

            doc = build_document(entry, feed_name, run_id)
            try:
                articles_col.insert_one(doc)
                inserted_count += 1
            except Exception as exc:
                msg = f"Insert failed for URL '{url}': {exc}"
                print(f"  ERROR: {msg}")
                errors.append(msg)

        print(f"  Inserted {inserted_count} new articles from '{feed_name}'")
        total_inserted += inserted_count

    completed_at = datetime.now(timezone.utc)

    # Write pipeline run summary
    run_record = {
        "run_id":                 run_id,
        "started_at":             started_at,
        "completed_at":           completed_at,
        "status":                 "completed" if not errors else "completed_with_errors",
        "feeds_queried":          [f["name"] for f in FEEDS],
        "rss_articles_inserted":  total_inserted,
        "errors":                 errors,
    }
    runs_col.insert_one(run_record)

    print(
        f"\n[{run_id}] Done — {total_inserted} articles inserted. "
        f"Status: {run_record['status']}"
    )

    client.close()


if __name__ == "__main__":
    main()
