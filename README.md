# The Nuclear Phase-Out Effect: A Data Pipeline for the German Electricity Market

**Data Engineering Zoomcamp 2026 — Final Project**

> 🔗 **Live Dashboard**: [Looker Studio link — ADD YOUR LINK HERE]
> 🎥 **Demo video**: [Loom/YouTube link — ADD YOUR LINK HERE]

## Problem Statement

On **April 15, 2023**, Germany shut down its last three nuclear reactors (Isar 2, Emsland, Neckarwestheim 2), completing the *Atomausstieg* (nuclear phase-out) that had been planned since 2011. This project quantifies the impact of that decision on the German electricity market by answering two concrete questions:

1. **How did the electricity generation mix shift?** Specifically, which sources (lignite, natural gas, renewables) replaced the ~33 TWh of annual nuclear output?
2. **How did wholesale electricity prices change relative to France?** France remained heavily nuclear-dependent, providing a natural comparison baseline.

The pipeline ingests dailiy electricity market data from SMARD (Bundesnetzagentur's official transparency platform), which sources data from ENTSO-E and covers 2015 to present. The dashboard shows a clear before/after comparison centered on the April 15, 2023 cutoff.

**Data source**: [SMARD API](https://smard.api.bund.dev/) — free, no authentication, CC BY 4.0 license. Attribution: *Bundesnetzagentur | SMARD.de*.

## Architecture

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  SMARD API   │───▶│  Python      │───▶│   GCS (raw   │───▶│   BigQuery   │───▶│  Looker      │
│  (REST/JSON) │    │  fetcher     │    │   NDJSON)    │    │  (dbt models)│    │  Studio      │
└──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
                           ▲                                        ▲
                           │                                        │
                           └────── Kestra orchestrates the DAG ─────┘
```

### Technology choices
| Layer | Tool | Rationale |
|---|---|---|
| Cloud | GCP (`europe-west3`) | Data sovereignty (German data, EU region) |
| IaC | Terraform | Reproducible infrastructure; `terraform apply` brings everything up |
| Orchestration | Kestra (self-hosted) | YAML-based DAG, 5+ distinct tasks, works offline for graders |
| Data Lake | GCS | Partitioned by ingestion date, raw JSON preserved for reprocessing |
| Warehouse | BigQuery | Serverless, free tier covers this workload, native dbt support |
| Transform | dbt Core (BigQuery adapter) | Industry standard, shows lineage, enables testing |
| Dashboard | Looker Studio | Free, native BigQuery integration, public sharing |

## Warehouse Design (Partitioning & Clustering)

The main fact table `fct_hourly_generation` is:

- **Partitioned by `date` (DAY)** — all dashboard queries filter on date ranges (e.g., "year before phase-out vs year after"). Partition pruning reduces scanned bytes from ~50 MB per query to ~1 MB, cutting cost and latency.
- **Clustered by `filter_id`** — queries group by energy source (nuclear, wind, solar, etc.). Clustering co-locates rows of the same source within each partition, further reducing scan cost on GROUP BY queries.

For `fct_nuclear_phaseout_daily` (aggregated down to daily grain): partitioned by `date` only, since the table is small (<10k rows) and always fully scanned by the dashboard.

## Pipeline Steps

The Kestra flow `smard_pipeline` runs these steps in sequence:

1. **`fetch_smard`** — Python task queries SMARD API for each (filter_id, region) combination
2. **`upload_to_gcs`** — writes raw NDJSON to `gs://smard-raw-{project}/raw/filter={id}/dt={date}.jsonl`
3. **`load_to_bigquery`** — BigQuery load job appends into `stg_smard_timeseries`
4. **`dbt_run`** — runs staging → marts transformations
5. **`dbt_test`** — runs data quality tests (not_null, unique, accepted_values)

Scheduled to run daily at 06:00 UTC via a Kestra cron trigger. Backfill (2022-01-01 → today) is a separate one-shot flow (`smard_backfill.yaml`).

## Reproducibility — How to Run

### Prerequisites
- Google Cloud account with billing enabled
- `gcloud` CLI installed and authenticated
- Docker + Docker Compose
- Python 3.10+
- Terraform ≥ 1.5
- A service account JSON key with roles: `BigQuery Admin`, `Storage Admin` (create via GCP Console)

### Step-by-step

```bash
# 1. Clone and configure
git clone <your-repo-url>
cd smard-nuclear-project
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
# Edit terraform.tfvars with your GCP project ID

export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/sa-key.json

# 2. Provision infrastructure
cd terraform
terraform init
terraform apply
cd ..

# 3. Start Kestra locally
docker compose -f kestra/docker-compose.yml up -d
# Open http://localhost:8080

# 4. Load SA key into Kestra KV store (Kestra UI → Namespaces → KV Store)
# Key: GCP_SA_KEY, Value: contents of your sa-key.json

# 5. Import and run the backfill flow
# In Kestra UI: Flows → Create → paste kestra/flows/smard_backfill.yaml
# Click "Execute"

# 6. Verify data in BigQuery
bq query --use_legacy_sql=false \
  'SELECT filter_id, COUNT(*) AS rows FROM `smard.stg_smard_timeseries` GROUP BY filter_id'

# 7. Run dbt manually (first time; afterwards Kestra handles it)
cd dbt
cp profiles.yml.example ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your project ID
dbt deps
dbt seed
dbt run
dbt test

# 8. View dashboard
# Open the Looker Studio link at the top of this README
```

### Expected outputs
- GCS bucket `smard-raw-{project}` contains partitioned NDJSON files
- BigQuery dataset `smard` contains: `stg_smard_timeseries`, `dim_filters`, `fct_hourly_generation`, `fct_hourly_prices`, `fct_generation_mix_periods`, `fct_nuclear_phaseout_daily`
- All `fct_*` tables are partitioned and clustered (verify in BigQuery console → table details)

## Evaluation Rubric Mapping

| Criterion | How this project scores |
|---|---|
| Problem description | Specific question + dataset + timeframe stated above |
| Cloud | GCP + Terraform IaC (`terraform/` folder) |
| Batch orchestration | 5-task Kestra DAG with clear separation of concerns |
| Data warehouse | Partitioned by date, clustered by filter_id, rationale above |
| Transformations | dbt Core with staging + marts layers, 5 models, `ref()` lineage |
| Dashboard | 2 Looker Studio tiles, link above |
| Reproducibility | This README + `terraform apply` + one Kestra flow execution |

## Findings (fill in after you run the pipeline)

*TODO after running: summarize the actual before/after shift you observe. Example bullets:*
- Nuclear share dropped from ~X% to 0% (by design)
- [Dominant replacement source] picked up the slack
- Wholesale prices in Germany diverged/converged with France by X €/MWh
- Anything surprising you found in the data

## Limitations & Future Work
- The SMARD API uses weekly-bucketed timestamps, requiring ~104 calls for 2 years of backfill — parallelization via Kestra subflows could speed this up.
- Prices are day-ahead market prices only; intraday/balancing market prices are not included.
- The "phase-out effect" cannot be fully isolated from confounding factors (gas crisis, weather, demand changes) — this dashboard shows correlation, not causation.

## Credits
- Data: [Bundesnetzagentur | SMARD.de](https://www.smard.de/) (CC BY 4.0)
- API wrapper inspiration: [bundesAPI/smard-api](https://github.com/bundesAPI/smard-api)
- Built as the final project for [DataTalksClub Data Engineering Zoomcamp 2026](https://github.com/DataTalksClub/data-engineering-zoomcamp)
