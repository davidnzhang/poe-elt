locals {
  data_lake_bucket = "poe_data_lake"
}

variable "project" {
  description = "Your GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources"
  default = "australia-southeast2"
  type = string
}

variable "storage_class" {
  description = "Storage class type for bucket"
  default = "STANDARD"
}

variable "role" {
  description = "User role for Snowflake"
}

variable "snowflake_region" {
  description = "Region for Snowflake (free tier)"
  default = "us-central1.gcp"
}

variable "database" {
  description = "Database for Snowflake"
  default = "POE_ELT"
}

variable "warehouse" {
  description = "Warehouse for Snowflake"
  default = "POE_ELT"
}

variable "warehouse_size" {
    description: "Warehouse size for Snowflake"
    default = "x-small"
}