terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Data lake: raw JSON from SMARD API
resource "google_storage_bucket" "smard_raw" {
  name          = "smard-raw-${var.project_id}"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = false
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    project = "smard-nuclear"
    env     = "dev"
  }
}

# Data warehouse: BigQuery dataset
resource "google_bigquery_dataset" "smard" {
  dataset_id                 = "smard"
  friendly_name              = "SMARD German electricity market data"
  description                = "Electricity generation and prices from Bundesnetzagentur SMARD platform"
  location                   = var.region
  delete_contents_on_destroy = true

  labels = {
    project = "smard-nuclear"
    env     = "dev"
  }
}

# Raw/staging table — created empty; Kestra's load job appends into it
resource "google_bigquery_table" "stg_smard_timeseries" {
  dataset_id          = google_bigquery_dataset.smard.dataset_id
  table_id            = "stg_smard_timeseries_raw"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "ingestion_date"
  }

  clustering = ["filter_id", "region"]

  schema = jsonencode([
    { name = "filter_id",     type = "INT64",     mode = "REQUIRED" },
    { name = "region",        type = "STRING",    mode = "REQUIRED" },
    { name = "resolution",    type = "STRING",    mode = "REQUIRED" },
    { name = "timestamp_ms",  type = "INT64",     mode = "REQUIRED" },
    { name = "value",         type = "FLOAT64",   mode = "NULLABLE" },
    { name = "ingestion_date",type = "DATE",      mode = "REQUIRED" },
    { name = "fetched_at",    type = "TIMESTAMP", mode = "REQUIRED" }
  ])
}

# Outputs for reference in other tools
output "bucket_name" {
  value = google_storage_bucket.smard_raw.name
}

output "dataset_id" {
  value = google_bigquery_dataset.smard.dataset_id
}
