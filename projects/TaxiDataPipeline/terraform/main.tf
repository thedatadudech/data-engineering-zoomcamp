provider "google" {
  project = var.project_id
  region  = var.region
}

# BigQuery Dataset
resource "google_bigquery_dataset" "taxi_data" {
  dataset_id                  = "nyc_taxi_data"
  friendly_name              = "NYC Taxi Data"
  location                   = var.region
  delete_contents_on_destroy = false

  access {
    role          = "OWNER"
    user_by_email = var.owner_email
  }
}

# Raw data table
resource "google_bigquery_table" "raw_taxi_data" {
  dataset_id = google_bigquery_dataset.taxi_data.dataset_id
  table_id   = "raw_taxi_rides"

  time_partitioning {
    type = "DAY"
    field = "pickup_datetime"
  }

  schema = <<EOF
[
  {
    "name": "pickup_datetime",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  },
  {
    "name": "dropoff_datetime",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  },
  {
    "name": "trip_distance",
    "type": "FLOAT",
    "mode": "REQUIRED"
  },
  {
    "name": "fare_amount",
    "type": "FLOAT",
    "mode": "REQUIRED"
  },
  {
    "name": "passenger_count",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "payment_type",
    "type": "STRING",
    "mode": "REQUIRED"
  }
]
EOF
}

# Storage bucket for data files
resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-taxi-data-lake"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
}

# Cloud Function service account
resource "google_service_account" "function_account" {
  account_id   = "taxi-data-processor"
  display_name = "Taxi Data Processor"
}

# Grant BigQuery access to the service account
resource "google_project_iam_member" "bigquery_access" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.function_account.email}"
}

# Grant Storage access to the service account
resource "google_project_iam_member" "storage_access" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.function_account.email}"
}
