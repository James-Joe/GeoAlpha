# GeoAlpha

A geopolitical sentiment pipeline that ingests news and social data, 
enriches it with NLP, and surfaces signals correlated with market movements.

## Stack
- Python 3
- MongoDB Atlas (pymongo)
- GDELT DOC 2.0 API via gdeltdoc library
- Reddit via PRAW (coming Month 2)
- Prefect for orchestration (coming Month 2)
- FinBERT/VADER for NLP enrichment (coming Month 3)

## Collections
- `gdelt_articles` — raw ingested articles with sentiment fields null until enriched
- `reddit_posts` — raw reddit posts (coming Month 2)
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