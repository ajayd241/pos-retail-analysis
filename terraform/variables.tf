variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "pubsub_topic_name" {
  description = "Pub/Sub topic name for POS transactions"
  type        = string
  default     = "pos-transactions"
}

variable "pubsub_subscription_name" {
  description = "Pub/Sub subscription for Dataflow to read from"
  type        = string
  default     = "pos-transactions-sub"
}

variable "bq_dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
  default     = "retail_analytics"
}

variable "bq_table_id" {
  description = "BigQuery table ID for POS transactions"
  type        = string
  default     = "pos_transactions"
}