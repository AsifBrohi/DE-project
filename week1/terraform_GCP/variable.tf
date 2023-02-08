locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "project" {
    description = "dtc-de-ab"
}

variable "region" {
    description = "Region for GCP resources"
    default = "europe-west2"
    type = string
  
}
variable "storage_class" {
    description = "Storage class type for your bucket"
    default = "STANDARD"

}

variable "BQ_DATASET" {
    description = "Bigquery Dataset that raw data from GCS will be writtten"
    type = string
    default = "trips_data_all"
}

variable "TABLE_NAME" {
    description = "BigQuery Table"
    type = string
    default = "ny_trips"
  
}