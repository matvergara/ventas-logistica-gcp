variable "project_id" {
  description = "ID del proyecto de Google Cloud"
  type        = string
}

variable "region" {
  description = "Región de GCP para los recursos"
  type        = string
  default     = "us-central1"
}

variable "bq_location" {
  description = "Ubicación de los datasets de BigQuery"
  type        = string
  default     = "US"
}
