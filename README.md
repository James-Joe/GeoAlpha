# GeoAlpha

**A geopolitical sentiment pipeline that ingests news and shipping data, enriches it with NLP, and surfaces signals correlated with commodity and FX futures price movements.**

![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-Atlas-green?logo=mongodb&logoColor=white)
![Prefect](https://img.shields.io/badge/Prefect-Cloud-blue?logo=prefect&logoColor=white)
![Status](https://img.shields.io/badge/Status-Active-brightgreen)

---

## Overview

GeoAlpha is a personal data engineering project that builds a full signal-generation pipeline from raw geopolitical events to tradeable market signals.

The core hypothesis: **world events leave a measurable footprint across news sentiment, physical shipping behaviour, and commodity prices — in that order, with a lag.** GeoAlpha ingests data from multiple sources, enriches it with NLP sentiment models, and produces a signal layer that can be correlated against futures market data.

The pipeline runs automatically every day at 06:00 UTC via Prefect Cloud, pulling from GDELT, RSS feeds, and financial data APIs, storing everything in MongoDB Atlas for downstream analysis.

---

## Signal Chain

```
World event happens
        ↓
Global news sentiment shifts (GDELT + RSS)
        ↓
Physical shipping behaviour changes (AIS vessel anomalies)
        ↓
Commodity prices move (futures markets)
```

The pipeline is being built in layers, each month adding one more stage of this chain.

---

## Architecture

### Data Sources

| Source | What it provides | Collection |
|---|---|---|
| [GDELT DOC 2.0 API](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/) | Geopolitical news articles, queried by keyword | `gdelt_articles` |
| RSS Feeds | News articles from WSJ, BBC World News, Financial Times, Al Jazeera | `rss_articles` |
| yfinance | Daily OHLCV for shipping indices: BDI, BDRY, EDRY, STNG, FRO | `shipping_signals` |
| MarineTraffic AIS *(coming Month 4)* | Live vessel tracking through key chokepoints | `shipping_signals` |

### MongoDB Collections

| Collection | Contents |
|---|---|
| `gdelt_articles` | Raw GDELT articles with sentiment fields (null until enriched) |
| `rss_articles` | Raw RSS articles with sentiment fields (null until enriched) |
| `shipping_signals` | Daily shipping index data and AIS vessel counts |
| `pipeline_runs` | Execution log for every pipeline run (per-task + consolidated flow records) |
| `signal_outputs` | Derived sentiment and correlation signals *(coming Month 5)* |

### Pipeline Flow

```
                    ┌─────────────────────────────────────────┐
                    │         Prefect Cloud (06:00 UTC)        │
                    │       geoalpha_daily_ingestion flow       │
                    └──────────────┬──────────────────────────┘
                                   │ parallel tasks
              ┌────────────────────┼────────────────────┐
              ▼                    ▼                    ▼
      run_gdelt_task        run_rss_task       run_shipping_task
      (GDELT DOC API)      (RSS Feeds)        (yfinance)
              │                    │                    │
              └────────────────────┼────────────────────┘
                                   │
                                   ▼
                          MongoDB Atlas
                    (gdelt_articles, rss_articles,
                     shipping_signals, pipeline_runs)
```

---

## Local Development Setup

### Prerequisites

- Python 3.11+
- A [MongoDB Atlas](https://www.mongodb.com/atlas) account with a free-tier cluster

### Steps

**1. Clone the repository**

```bash
git clone https://github.com/James-Joe/GeoAlpha.git
cd GeoAlpha
```

**2. Create and activate a virtual environment**

```bash
python3 -m venv .venv
source .venv/bin/activate        # macOS / Linux
.venv\Scripts\activate           # Windows
```

**3. Install dependencies**

```bash
pip install -r requirements.txt
```

**4. Create a `.env` file**

Copy the template below and fill in your values. This file is gitignored and must never be committed.

```bash
# .env
MONGODB_URI=mongodb+srv://<user>:<password>@<cluster>.mongodb.net/?retryWrites=true&w=majority
```

**5. Run individual scripts**

Each script can be run standalone for testing:

```bash
python ingest_gdelt.py          # ingest GDELT news articles
python ingest_rss.py            # ingest RSS feed articles
python ingest_shipping.py       # ingest yfinance shipping signals
python pipeline.py              # run the full orchestrated flow locally
```

---

## Prefect Cloud Deployment

The pipeline is deployed to Prefect Cloud via GitHub — no Docker, no server. Prefect pulls the repo, installs dependencies, and runs the flow on schedule.

### Prerequisites

- A [Prefect Cloud](https://app.prefect.cloud) account (free tier is sufficient)
- [`uv`](https://docs.astral.sh/uv/) installed: `curl -LsSf https://astral.sh/uv/install.sh | sh`

### Deploy

**1. Log in to Prefect Cloud**

```bash
uvx prefect-cloud login
```

**2. Connect your GitHub account**

```bash
uvx prefect-cloud github setup
```

**3. Deploy the pipeline**

```bash
uvx prefect-cloud deploy pipeline.py:geoalpha_daily_ingestion \
  --from James-Joe/GeoAlpha \
  --name geoalpha-daily \
  --with-requirements requirements.txt \
  --secret MONGODB_URI=your_connection_string
```

This registers the deployment and stores `MONGODB_URI` as a Prefect Secret block — it is never stored in the repository.

**4. Schedule daily at 06:00 UTC**

```bash
uvx prefect-cloud schedule geoalpha_daily_ingestion/geoalpha-daily "0 6 * * *"
```

**5. View runs in the Prefect Cloud dashboard**

[https://app.prefect.cloud](https://app.prefect.cloud)

### Useful Commands

```bash
# Trigger a manual run
uvx prefect-cloud run geoalpha_daily_ingestion/geoalpha-daily

# Stream logs from the latest run
uvx prefect-cloud logs geoalpha_daily_ingestion/geoalpha-daily

# Pause the schedule
uvx prefect-cloud schedule geoalpha_daily_ingestion/geoalpha-daily --unset

# Delete the deployment
uvx prefect-cloud delete deployment geoalpha_daily_ingestion/geoalpha-daily
```

---

## Environment Variables

| Variable | Required | Description | Where to get it |
|---|---|---|---|
| `MONGODB_URI` | Yes | MongoDB Atlas connection string | Atlas dashboard → Connect → Drivers |

> **Local development:** add variables to a `.env` file in the project root — it is loaded automatically by `python-dotenv`.
>
> **Prefect Cloud:** variables are injected via Prefect Secret blocks at runtime (see deployment instructions above). Do not use a `.env` file in production.

---

## Roadmap

| Month | Status | Milestone |
|---|---|---|
| Month 1 | ✅ Complete | GDELT DOC API ingestion — geopolitical keyword articles |
| Month 2 | ✅ Complete | RSS ingestion + yfinance shipping signals + Prefect orchestration |
| Month 3 | 🔄 Next | NLP sentiment enrichment — VADER (fast) + FinBERT (finance-tuned) |
| Month 4 | Planned | Shipping intelligence layer — MarineTraffic AIS vessel tracking through Hormuz, Red Sea, Suez, South China Sea, Arctic |
| Month 5 | Planned | Signal layer — cross-source sentiment aggregation, shipping anomaly detection, commodity price correlation |
| Month 6 | Planned | Streamlit dashboard + SLO tracking and alerting |

---

## Tech Stack

**Ingestion**
- [`gdeltdoc`](https://github.com/alex9smith/gdelt-doc-api) — GDELT DOC 2.0 API client
- [`feedparser`](https://feedparser.readthedocs.io/) — RSS feed parsing
- [`yfinance`](https://github.com/ranaroussi/yfinance) — financial market data

**Storage**
- [MongoDB Atlas](https://www.mongodb.com/atlas) — cloud document database
- [`pymongo`](https://pymongo.readthedocs.io/) — Python MongoDB driver

**Orchestration**
- [Prefect Cloud](https://www.prefect.io/) — pipeline scheduling and observability
- [`prefect`](https://docs.prefect.io/) — flow and task definitions

**NLP** *(Month 3)*
- [VADER](https://github.com/cjhutto/vaderSentiment) — rule-based sentiment (fast, general)
- [FinBERT](https://huggingface.co/ProsusAI/finbert) — transformer-based sentiment (finance-tuned)

**Utilities**
- [`python-dotenv`](https://github.com/theskumar/python-dotenv) — local environment variable loading

---

*Built as a personal data engineering project to develop skills in pipeline orchestration, NLP enrichment, and financial signal generation.*
