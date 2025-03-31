output "bigquery_dataset" {
  value = google_bigquery_dataset.taxi_data.dataset_id
}

output "storage_bucket" {
  value = google_storage_bucket.data_lake.name
}

output "function_service_account" {
  value = google_service_account.function_account.email
}
