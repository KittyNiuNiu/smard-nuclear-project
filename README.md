# The Nuclear Phase-Out Effect: A Data Pipeline for the German Electricity Market

**Data Engineering Zoomcamp 2026 — Final Project**


## Problem Statement

On **April 15, 2023**, Germany shut down its last three nuclear reactors (Isar 2, Emsland, Neckarwestheim 2), completing the *Atomausstieg* (nuclear phase-out) that had been planned since 2011. This project quantifies the impact of that decision on the German electricity market by answering two concrete questions:

1. **How did the electricity generation mix shift?** Specifically, which sources (lignite, natural gas, renewables) replaced the ~33 TWh of annual nuclear output?
2. **How did wholesale electricity prices change relative to France?** France remained heavily nuclear-dependent, providing a natural comparison baseline.

The pipeline ingests daily (after a one time ingestion of historical data) electricity market data from SMARD (Bundesnetzagentur's official transparency platform), which sources data from ENTSO-E and covers 2015 to present. The dashboard shows a clear before/after comparison centered on the April 15, 2023 cutoff.

**Data source**: [SMARD API](https://smard.api.bund.dev/). Attribution: *Bundesnetzagentur | SMARD.de*.

## Architecture

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  SMARD API   │───▶│  Python      │───▶│   GCS (raw   │───▶│   BigQuery   │───▶│  Looker      │
│  (REST/JSON) │    │  fetcher     │    │   NDJSON)    │    │  (dbt models)│    │  Studio      │
└──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
                           ▲                                        ▲
                           │                                        │
                           └Kestra orchestrates the daily ingestion─┘
```

### Technology 
| Layer | Tool | 
|---|---|
| Cloud | GCP (`europe-west3`) | 
| IaC | Terraform | 
| Orchestration | Kestra (self-hosted) | 
| Data Lake | GCS | 
| Warehouse | BigQuery | 
| Transform | dbt Cloud | 
| Dashboard | Looker Studio | 


## Pipeline Steps

The pipeline runs in two phases:

**One-time backfill** (run manually):
1. **`fetch_smard.py`** — Python script fetches SMARD API for all 15 filters → uploads NDJSON to GCS
2. **BigQuery load** — load data manually from GCS into `stg_smard_timeseries_raw` via BigQuery UI

**Daily batch** (automated via Kestra, scheduled at 12:00 UTC):
1. **`fetch_and_upload`** — Python task fetches yesterday's data → GCS
2. **`load_to_bigquery`** — Kestra loads GCS files → BigQuery staging table
3. **`trigger_dbt_cloud`** — HTTP request triggers dbt Cloud job (seed → run → build)

---

## Reproducibility

### Prerequisites
- Google Cloud account with billing enabled
- Docker + Docker Compose
- Python 3.10+
- Terraform ≥ 1.5
- Service account JSON key with roles: `BigQuery Admin`, `Storage Admin`
- dbt Cloud account (free Developer plan)

### Step-by-step

```bash
# 1. Clone repo
git clone <your-repo-url>
cd smard-nuclear-project

# 2. Provision infrastructure
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
# Edit terraform.tfvars — set project_id = "your-gcp-project-id"
cd terraform
terraform init
terraform apply
cd ..

# 3. Set up credentials
mkdir -p ~/.gcp
cp /path/to/your/sa-key.json ~/.gcp/credentials.json
export GOOGLE_APPLICATION_CREDENTIALS=~/.gcp/credentials.json

# 4. Run one-time backfill
python3 -m venv .venv
source .venv/bin/activate
pip install requests google-cloud-storage

export GCS_BUCKET=smard-raw-your-project-id
python fetch_smard.py --all-filters --resolution day --start 2022-01-01 --end 2026-04-19

# 5. Load GCS → BigQuery (run in BigQuery UI query editor)
# LOAD DATA INTO `your-project.smard.stg_smard_timeseries_raw`
# FROM FILES (format='JSON', uris=['gs://smard-raw-your-project/raw/filter=.../...jsonl'])
# See docs/architecture.md for exact URIs

# 6. Set up dbt Cloud
# - Create free account at cloud.getdbt.com
# - Connect BigQuery (region: europe-west3)
# - Connect this GitHub repo (subdirectory: dbt)
# - Create job with commands: dbt seed, dbt run, dbt build
# - Run job once to verify all 5 models pass

# 7. Start Kestra
cd kestra
docker compose up -d
# Open http://localhost:8080
# Add KV store keys: DBT_CLOUD_ACCOUNT_ID, DBT_CLOUD_JOB_ID, DBT_CLOUD_TOKEN
# Upload fetch_smard.py to Namespace Files (smard.nuclear)
# Import kestra/flows/smard_daily.yaml
# Daily schedule activates automatically at 12:00 UTC


### Expected outputs
- GCS bucket `smard-raw-{project}` — NDJSON files partitioned by filter and date
- BigQuery `smard.stg_smard_timeseries_raw` — raw partitioned table (~21,500 rows)
- BigQuery `smard_staging.stg_smard_timeseries` — cleaned view
- BigQuery `smard_marts.fct_daily_generation` — partitioned by date, clustered by filter_id
- BigQuery `smard_marts.fct_daily_prices_base` — partitioned by date
- BigQuery `smard_marts.fct_generation_mix_periods` — 26 rows (Tile 1 source)
- BigQuery `smard_marts.fct_daily_prices` — 1,569 rows (Tile 2 source)
```

## Dashboard
![Dashboard](/dashboard.png)

It can also be view [here](https://datastudio.google.com/reporting/eab150bb-9346-4676-bd8d-b797288a6248)


## Findings

**Generation mix (Tile 1):**
- Nuclear generation dropped from ~81 GWh/day to almost 0 after April 15, 2023
  as expected following the Atomausstieg.
- Contrary to fears, lignite and hard coal also declined in the year after
  the phase-out — suggesting renewables absorbed more of the load than fossil fuels.
- Wind and solar grew to compensate, consistent with Germany's Energiewende targets.

**Wholesale prices (Tile 2):**
- Before the phase-out, German and French day-ahead prices tracked closely,
  reflecting their interconnected grid. Both markets are affected by the gas crisis in 2022 by showing a peak of prices.
- After April 2023, Germany consistently trades at a premium over France.
- This divergence likely reflects France's continued reliance on cheap nuclear
  baseload, while Germany depends more on variable renewables and gas peakers.

**Caveat:** These findings show correlation, not causation. The 2022 gas crisis,
weather patterns, and demand changes also influence both generation mix and prices.





