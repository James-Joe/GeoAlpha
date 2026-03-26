# GeoAlpha

A geopolitical sentiment pipeline that ingests news and social data, 
enriches it with NLP, and surfaces signals correlated with market movements.

## Stack
- Python 3
- MongoDB Atlas (pymongo)
- GDELT DOC 2.0 API via gdeltdoc library
- Prefect for orchestration (coming Month 2)
- FinBERT/VADER for NLP enrichment (coming Month 3)

## Collections
- `gdelt_articles` — raw ingested articles with sentiment fields null until enriched
- `gdelt_tone_timelines` — daily average GDELT tone per keyword; unique index on (query_keyword, date)
- `rss_articles` — raw RSS feed articles with sentiment fields null until enriched
- `pipeline_runs` — logs every pipeline execution

## Conventions
- All timestamps in UTC
- Environment variables loaded from .env via python-dotenv
- pipeline_run_id format: run_YYYYMMDD_HHMMSS
- Never insert duplicate URLs — check before insert, unique index as safety net
- Keep code well commented

## Project structure
- /ingest_gdelt.py — GDELT ingestion script
- /requirements.txt
- /.env — never commit this

## Future Phase — Shipping Intelligence Layer
- AIS vessel tracking data for key chokepoints
  (Hormuz, Red Sea, Suez, South China Sea, Arctic)
- Baltic Dry Index and Freightos Baltic Index via yfinance
- Sea ice extent data from NSIDC for Arctic routes
- New collection: `shipping_signals`
- Hypothesis: shipping lane anomalies mediate between 
  geopolitical sentiment and commodity price movements

  ## Shipping Data Sources

### Active
- Baltic Dry Index and shipping indices via `yfinance`
  - Tickers: BDI (Baltic Dry Index), plus shipping proxies
  - Collection: `shipping_signals`
  - Daily ingestion

- GDELT shipping keywords (extension of existing gdelt collector)
  - Additional keywords focused on maritime/shipping events
  - Same collection as gdelt_articles: `gdelt_articles`
  - Same pipeline_runs logging pattern

### Pending
- MarineTraffic AIS API (account registration in progress)
  - Will track vessel counts through: Hormuz, Red Sea, 
    Suez, South China Sea, Arctic
  - Collection: `shipping_signals`

  ## Future Research Directions

### Shadow Fleet Tracking
The shadow fleet is a collection of tankers that operate outside normal 
international shipping to evade sanctions — primarily moving Russian, 
Iranian, and Venezuelan oil to buyers in India, China, and Turkey.

**Why it matters for GeoAlpha:**
Shadow fleet disruptions affect legitimate tanker stocks (FRO, STNG) 
and oil prices. When seizures or sanctions tightening reduce illicit 
supply, legitimate tanker rates rise.

**Hypothesis:**
When shadow fleet news sentiment spikes negatively (seizures, sanctions 
tightening), do FRO and STNG rise within 48 hours as markets price in 
reduced illicit supply?

**Already partially tracked via existing keywords.**
Add these to GDELT collector to make it explicit:
- "shadow fleet tanker"
- "sanctions evasion ship"
- "AIS dark vessel"
- "ship to ship transfer sanctions"
- "Russian oil tanker seized"

**Future enhancement — AIS gap detection:**
Vessels that disable AIS transponders ("going dark") are a signal in 
themselves. Cross-referencing AIS gaps with satellite imagery identifies 
shadow fleet movements. Requires paid AIS provider or satellite imagery 
API — beyond current free tier but worth revisiting.

### Arctic Shipping & Greenland
As Arctic ice retreats the Northern Sea Route between Europe and Asia 
becomes commercially viable, cutting Rotterdam-Shanghai journey by ~40%. 
US interest in Greenland is directly related to controlling Arctic 
chokepoints.

**Hypothesis:**
Does Arctic shipping sentiment lead Baltic Dry Index movements as 
northern routes become commercially relevant?

**Data source to add:**
Sea ice extent data from NSIDC (National Snow and Ice Data Center) — 
free, no API key required. Correlate ice extent with Arctic shipping 
news sentiment and BDI movements.

### AIS Chokepoint Monitoring
Once a paid AIS provider is in place (Datalastic or AISHub), track 
daily vessel counts through:
- Strait of Hormuz (oil tankers → crude prices)
- Red Sea / Suez Canal (container ships → goods inflation)
- South China Sea (broad trade flow indicator)
- Arctic routes (emerging — correlate with ice extent data)

**Collection:** `shipping_signals` (already created)

## Future Technical Enhancements

### Rust Scraping Layer (Future)
If article volume scales beyond ~1000/day, consider 
replacing Python scraper with Rust implementation using:
- `reqwest` for async HTTP
- `scraper` for HTML parsing  
- `tokio` for concurrency

Current approach (Python + trafilatura with title fallback) 
is sufficient for current scale. Revisit when enrichment 
pipeline becomes a bottleneck.