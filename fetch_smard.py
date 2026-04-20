"""
SMARD API ingestion script.

Fetches dailiy timeseries data from the Bundesnetzagentur SMARD API
and writes to GCS as newline-delimited JSON (NDJSON), one line per data point.

The SMARD API has two endpoints:
  1. /chart_data/{filter}/{region}/index_{resolution}.json
     Returns available timestamps. Each timestamp represents the start of a
     weekly bucket (Monday 00:00 Berlin time in practice, but API returns UTC ms).
  2. /chart_data/{filter}/{region}/{filterCopy}_{regionCopy}_{resolution}_{timestamp}.json
     Returns the actual timeseries for that week: [[ts_ms, value], ...]

Note: filterCopy and regionCopy must equal filter and region — this is
documented as "Kaputtes API-Design" (broken API design) by the API maintainers.

Usage:
    python fetch_smard.py --filter 1224 --region DE --start 2022-01-01 --end 2026-04-19
    python fetch_smard.py --all-filters --start 2022-01-01 --end 2026-04-19

Environment:
    GOOGLE_APPLICATION_CREDENTIALS  path to service account JSON
    GCS_BUCKET                      target bucket name 
    GCP_PROJECT                     project ID 
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

import requests

try:
    from google.cloud import storage  # type: ignore
except ImportError:
    storage = None  # Allows --local-only runs without gcloud libs installed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("fetch_smard")

SMARD_BASE = "https://www.smard.de/app"


GENERATION_FILTERS = {
    1223: "Braunkohle",           # Lignite
    1224: "Kernenergie",          # Nuclear
    1225: "Wind Offshore",
    1226: "Wasserkraft",          # Hydro
    1227: "Sonstige Konventionelle",  # Other conventional
    1228: "Sonstige Erneuerbare",     # Other renewable
    4066: "Biomasse",
    4067: "Wind Onshore",
    4068: "Photovoltaik",
    4069: "Steinkohle",           # Hard coal
    4070: "Pumpspeicher",         # Pumped storage
    4071: "Erdgas",               # Natural gas
}

CONSUMPTION_FILTERS = {
    410: "Stromverbrauch Gesamt",  # Total grid load
}

PRICE_FILTERS = {
    4169: "Marktpreis DE/LU",     # Germany/Luxembourg
    254:  "Marktpreis Frankreich", # France
}

ALL_FILTERS = {**GENERATION_FILTERS, **CONSUMPTION_FILTERS, **PRICE_FILTERS}


@dataclass(frozen=True)
class FetchJob:
    filter_id: int
    region: str
    resolution: str = "day"


def get_indices(job: FetchJob, session: requests.Session) -> list[int]:
    """Fetch the list of available weekly-bucket timestamps for a given filter/region."""
    url = f"{SMARD_BASE}/chart_data/{job.filter_id}/{job.region}/index_{job.resolution}.json"
    r = session.get(url, timeout=30)
    if r.status_code == 404:
        log.warning(f"No index data for filter={job.filter_id} region={job.region}")
        return []
    r.raise_for_status()
    return r.json().get("timestamps", [])


def get_timeseries(job: FetchJob, timestamp_ms: int, session: requests.Session) -> list[list[float]]:
    """Fetch one week of timeseries data.

    Note the kaputt design: filter and region must appear twice in the URL.
    """
    url = (
        f"{SMARD_BASE}/chart_data/{job.filter_id}/{job.region}/"
        f"{job.filter_id}_{job.region}_{job.resolution}_{timestamp_ms}.json"
    )
    r = session.get(url, timeout=30)
    if r.status_code == 404:
        log.warning(f"No data for filter={job.filter_id} ts={timestamp_ms}")
        return []
    r.raise_for_status()
    return r.json().get("series", [])


def fetch_job(
    job: FetchJob,
    start_ms: int,
    end_ms: int,
    session: requests.Session,
) -> Iterable[dict]:
    """Yield one dict per data point for a given filter/region within [start_ms, end_ms]."""
    timestamps = get_indices(job, session)
    # Weekly buckets overlap the range if bucket_start <= end AND (bucket_start + 1 week) >= start
    # We fetch generously and let downstream filter; one week padding is fine.
    bucket_ms = {
    "quarterhour": 7  * 24 * 60 * 60 * 1000,   # weekly
    "hour":        7  * 24 * 60 * 60 * 1000,   # weekly
    "day":         366 * 24 * 60 * 60 * 1000,  # yearly
    "week":        366 * 24 * 60 * 60 * 1000,  # yearly
    "month":       366 * 24 * 60 * 60 * 1000,  # yearly
    }.get(job.resolution, 7 * 24 * 60 * 60 * 1000)
    relevant = [t for t in timestamps if (t + bucket_ms) >= start_ms and t <= end_ms]
    log.info(
        f"filter={job.filter_id} region={job.region}: "
        f"{len(relevant)} weekly buckets to fetch "
        f"(out of {len(timestamps)} total)"
    )

    fetched_at = datetime.now(timezone.utc).isoformat()
    for i, ts in enumerate(relevant, start=1):
        if i % 20 == 0:
            log.info(f"  progress: {i}/{len(relevant)} buckets fetched")
        series = get_timeseries(job, ts, session)
        for row in series:
            if not row or len(row) < 2:
                continue
            point_ts, value = row[0], row[1]
            # Filter to date range
            if point_ts < start_ms or point_ts > end_ms:
                continue
            yield {
                "filter_id": job.filter_id,
                "region": job.region,
                "resolution": job.resolution,
                "timestamp_ms": int(point_ts),
                "value": float(value) if value is not None else None,
                "ingestion_date": date.today().isoformat(),
                "fetched_at": fetched_at,
            }
        # Be polite to the API
        time.sleep(0.1)


def write_local_ndjson(path: Path, records: Iterable[dict]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
            n += 1
    log.info(f"Wrote {n} records to {path}")
    return n


def upload_to_gcs(bucket_name: str, blob_path: str, local_path: Path) -> None:
    if storage is None:
        raise RuntimeError("google-cloud-storage not installed; pip install google-cloud-storage")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(str(local_path), content_type="application/x-ndjson")
    log.info(f"Uploaded → gs://{bucket_name}/{blob_path}")


def to_ms(date_str: str) -> int:
    return int(datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--filter", type=int, help="Single filter ID to fetch")
    p.add_argument("--all-filters", action="store_true", help="Fetch all configured filters")
    p.add_argument("--region", default="DE", help="Region code (default: DE)")
    p.add_argument("--resolution", default="day", choices=["hour", "quarterhour", "day"])
    p.add_argument("--start", required=True, help="Start date YYYY-MM-DD (inclusive)")
    p.add_argument("--end", required=True, help="End date YYYY-MM-DD (inclusive)")
    p.add_argument("--local-dir", default="./data", help="Local working directory for NDJSON files")
    p.add_argument("--local-only", action="store_true", help="Skip GCS upload")
    p.add_argument("--dry-run", action="store_true", help="Print first 3 records, don't write anything")
    args = p.parse_args()

    if not args.filter and not args.all_filters:
        p.error("Specify --filter or --all-filters")

    filter_ids = list(ALL_FILTERS.keys()) if args.all_filters else [args.filter]
    start_ms = to_ms(args.start)
    end_ms = to_ms(args.end) + 24 * 60 * 60 * 1000 - 1  # include end date

    bucket_name = os.environ.get("GCS_BUCKET")
    if not args.local_only and not args.dry_run and not bucket_name:
        log.error("GCS_BUCKET env var required unless --local-only or --dry-run")
        return 2


    session = requests.Session()
    # SMARD's WAF blocks simple User-Agents with 403, use a browser-like one
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
    })

    total_records = 0
    for fid in filter_ids:
        region = args.region
        job = FetchJob(filter_id=fid, region=region, resolution=args.resolution)
        records = list(fetch_job(job, start_ms, end_ms, session))

        if args.dry_run:
            print(f"\nFilter {fid} ({ALL_FILTERS.get(fid)}) — first 3 of {len(records)} records:")
            for r in records:
                print(json.dumps(r, indent=2))
            continue

        if not records:
            log.warning(f"No records for filter {fid}; skipping write")
            continue

        local_path = Path(args.local_dir) / f"filter={fid}" / f"region={region}" / f"{args.start}_{args.end}.jsonl"
        write_local_ndjson(local_path, iter(records))
        total_records += len(records)

        if not args.local_only:
            blob_path = f"raw/filter={fid}/region={region}/resolution={args.resolution}/dt={args.start}/{args.start}_{args.end}.jsonl"
            upload_to_gcs(bucket_name, blob_path, local_path)

    log.info(f"Done. Total records: {total_records}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
