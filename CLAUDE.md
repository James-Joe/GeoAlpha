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