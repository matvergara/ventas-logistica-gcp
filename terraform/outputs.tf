output "bucket_name" {
  description = "Nombre del bucket de GCS para datos RAW"
  value       = google_storage_bucket.raw_data.name
}

output "bucket_url" {
  description = "URL del bucket de GCS"
  value       = google_storage_bucket.raw_data.url
}

output "bq_datasets" {
  description = "IDs de los datasets de BigQuery creados"
  value = {
    raw       = google_bigquery_dataset.raw.dataset_id
    dwh       = google_bigquery_dataset.dwh.dataset_id
    datamarts = google_bigquery_dataset.datamarts.dataset_id
    infra     = google_bigquery_dataset.infra.dataset_id
  }
}
