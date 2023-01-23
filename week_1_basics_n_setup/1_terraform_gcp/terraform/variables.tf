locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "project" {
  description = "zoomcamp-374917"

}

variable "region" {
  description = "Region for GCP resources."
  default     = "us-central1"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "trips_data_all"
}

variable "TABLE_NAME" {
  description = "Biguery table"
  type        = string
  default     = "ny_trips"
}
