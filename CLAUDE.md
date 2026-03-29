# GeoAlpha

A geopolitical sentiment pipeline that ingests news and shipping data,
enriches it with NLP, and surfaces signals correlated with crude oil
price movements driven by Middle East conflict and Hormuz disruption.

---

## Core Thesis — Hormuz Oil Signal

**Hypothesis:** Negative sentiment around Middle East conflict and Iran
tensions precedes rises in Brent Crude and WTI futures within a 48-72
hour window, mediated by tanker traffic disruption through the Strait
of Hormuz.

**Signal chain:**
```
Middle East/Iran news sentiment (GDELT + RSS)
        ↓
Hormuz tanker disruption (AIS — future)
        ↓
Brent Crude (BZ=F) + WTI (CL=F) price movement
        ↓
Tanker operator stocks: FRO, STNG
```

---

## Stack
- Python 3
- MongoDB Atlas (pymongo)
- GDELT DOC 2.0 API via gdeltdoc library
- Prefect Cloud for orchestration (deployed, running daily at 6am UTC)
- FinBERT/VADER for NLP enrichment (Month 3)
- yfinance for shipping and commodity price signals

---

## Project Structure
```
/ingest_gdelt.py          — GDELT article + tone timeline ingestion
/ingest_rss.py            — RSS feed ingestion with Hormuz relevance filter
/ingest_shipping.py       — yfinance shipping and crude price signals
/pipeline.py              — Prefect flow orchestrating all collectors
/requirements.txt
/.env                     — never commit this
/CLAUDE.md
/README.md
```

---

## MongoDB Collections
- `gdelt_articles` — GDELT articles, sentiment fields null until enriched
- `gdelt_tone_timelines` — daily average GDELT tone per keyword;
  unique index on (query_keyword, date)
- `rss_articles` — RSS articles filtered for Hormuz relevance,
  sentiment fields null until enriched
- `shipping_signals` — daily yfinance price data for crude and tanker tickers
- `pipeline_runs` — logs every pipeline execution with per-task results

---

## GDELT Keywords (Hormuz focused)
```python
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
```

---

## RSS Feeds
```python
FEEDS = [
    {"name": "Wall Street Journal",
     "url": "https://feeds.a.dj.com/rss/RSSWorldNews.xml"},
    {"name": "BBC World News",
     "url": "http://feeds.bbci.co.uk/news/world/rss.xml"},
    {"name": "Financial Times",
     "url": "https://www.ft.com/rss/home/uk"},
    {"name": "Al Jazeera",
     "url": "https://www.aljazeera.com/xml/rss/all.xml"},
]
```

**Relevance filter** — only articles containing these keywords are stored:
```python
HORMUZ_KEYWORDS = [
    "iran", "hormuz", "persian gulf", "gulf oil",
    "tanker", "crude oil", "irgc", "sanctions", "opec"
]
```

Each stored document includes an `is_relevant: True` boolean field.

---

## Tickers (yfinance)
```python
TICKERS = [
    {"symbol": "BZ=F",  "name": "Brent Crude Futures"},
    {"symbol": "CL=F",  "name": "WTI Crude Futures"},
    {"symbol": "FRO",   "name": "Frontline (Crude Tanker)"},
    {"symbol": "STNG",  "name": "Scorpio Tankers"},
    {"symbol": "USO",   "name": "US Oil ETF"},
]
```

---

## Conventions
- All timestamps stored as proper UTC datetime objects, never strings
- Environment variables loaded from .env via python-dotenv
- pipeline_run_id format: run_YYYYMMDD_HHMMSS
- Deduplication: find_one check before insert + unique index on url
- All timestamps in UTC
- Code should be clean and well commented
- Sentiment fields default to null at ingestion, enriched in Month 3

---

## Sentiment Enrichment Approach (Month 3)
- VADER for RSS articles — works well on short headline text
- FinBERT for GDELT articles — financially aware model for geopolitical news
- Both models run on the **title field** — avoids scraping complexity,
  captures 80-90% of sentiment signal
- Fallback: use summary field if title is empty (RSS articles)
- Validation: compare FinBERT scores against gdelt_tone_timelines baseline
  — significant divergence warrants investigation

---

## Prefect Deployment
- Flow: `geoalpha_daily_ingestion` in `pipeline.py`
- All four collectors run in parallel via `.submit()` and `wait()`
- Each task has `retries=2`, `retry_delay_seconds=30`
- Schedule: `0 6 * * *` (6am UTC daily)
- Credentials stored as Prefect Secret blocks, not in .env

**Useful commands:**
```bash
# Deploy
uvx prefect-cloud deploy pipeline.py:geoalpha_daily_ingestion \
    --from James-Joe/GeoAlpha \
    --name geoalpha-daily \
    --with-requirements requirements.txt \
    --secret MONGODB_URI=your_connection_string

# Schedule
uvx prefect-cloud schedule geoalpha_daily_ingestion/geoalpha-daily "0 6 * * *"

# Run manually
uvx prefect-cloud run geoalpha_daily_ingestion/geoalpha-daily

# Remove schedule
uvx prefect-cloud unschedule geoalpha_daily_ingestion/geoalpha-daily
```

---

## GDELT BigQuery (Future Enhancement)

The DOC 2.0 API caps results at 250 articles and returns 8 fields.
GDELT's full dataset is available free via Google BigQuery — far richer.

**Three tables updated every 15 minutes:**
- `gdelt-bq.gdeltv2.events` — structured CAMEO events, Goldstein Scale,
  Actor1/Actor2 codes, geolocation
- `gdelt-bq.gdeltv2.mentions` — every article mentioning an event
- `gdelt-bq.gdeltv2.gkg` — per-article themes, named entities,
  full 7-dimension tone vector

**Why it matters:** Goldstein scores (pre-computed conflict signal),
CAMEO codes (structured event classification), no 250-article cap,
full tone vector vs single average tone from DOC API.

**Access:** Google Cloud account required (free). 1TB free query
scans/month — a single GDELT query typically scans a few GB.

---

## Future Research Directions

### AIS Chokepoint Monitoring
Track daily vessel counts through Strait of Hormuz as the mediating
variable between sentiment and crude price movement.

Provider options: Datalastic, AISHub (community-based, free with
data sharing), VesselFinder.

Collection: `shipping_signals` (already exists)

### Shadow Fleet Tracking
Shadow tankers move Iranian and Russian oil outside the sanctions
regime. When seizures or sanctions tightening reduce illicit supply,
legitimate tanker rates and crude prices rise.

**Hypothesis:** Negative shadow fleet news sentiment predicts FRO
and STNG price rises within 48 hours.

Keywords to add when ready:
```python
"shadow fleet tanker",
"sanctions evasion ship",
"AIS dark vessel",
"ship to ship transfer sanctions",
"Iranian oil tanker seized"
```

Future: AIS gap detection (vessels going dark) via satellite
imagery cross-referencing — requires paid provider.

### Rust Scraping Layer
If article volume scales beyond ~1,000/day, consider replacing
Python scraper with Rust implementation using `reqwest`, `scraper`,
and `tokio`. Current Python + title-only approach is sufficient
at current scale.

### Thesis 2 — Black Sea Grain Signal (future)
```
Russia Ukraine conflict sentiment
        ↓
Black Sea shipping disruption
        ↓
Wheat futures (ZW=F) + Corn futures (ZC=F)
        ↓
Dry bulk shipping rates (BDRY)
```

### Thesis 3 — Red Sea Container Signal (future)
```
Houthi attack sentiment
        ↓
Red Sea / Suez disruption
        ↓
Container shipping rates
        ↓
Inflation expectations
```

---

## Out of Scope for Now
- Reddit/PRAW (API access too restrictive)
- MarineTraffic API (enterprise pricing only since Kpler acquisition)
- Arctic routes / Greenland (Thesis 4 — future)
- NSIDC sea ice data (revisit with Arctic thesis)