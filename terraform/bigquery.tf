resource "google_bigquery_dataset" "raw" {
  dataset_id  = "raw"
  location    = var.bq_location
  description = "Capa RAW: copia fiel de archivos provenientes de Cloud Storage"
}

resource "google_bigquery_dataset" "dwh" {
  dataset_id  = "dwh"
  location    = var.bq_location
  description = "Data Warehouse: modelo dimensional (esquema estrella)"
}

resource "google_bigquery_dataset" "datamarts" {
  dataset_id  = "datamarts"
  location    = var.bq_location
  description = "Datamarts analíticos para consumo BI (Looker Studio)"
}

resource "google_bigquery_dataset" "infra" {
  dataset_id  = "infra"
  location    = var.bq_location
  description = "Infraestructura: control de cargas incrementales"
}
