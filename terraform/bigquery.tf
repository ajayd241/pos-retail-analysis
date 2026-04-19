resource "google_bigquery_dataset" "retail_analytics" {
  dataset_id  = var.bq_dataset_id
  project     = var.project_id
  location    = var.region
  description = "Retail analytics dataset for POS transactions"

  labels = {
    env     = "dev"
    project = "pos-retail-analysis"
  }
}

resource "google_bigquery_table" "pos_transactions" {
  dataset_id = google_bigquery_dataset.retail_analytics.dataset_id
  table_id   = var.bq_table_id
  project    = var.project_id

  deletion_protection = false

  schema = jsonencode([
    {
      name        = "transaction_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Unique transaction identifier"
    },
    {
      name        = "store_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Store identifier"
    },
    {
      name        = "terminal_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "POS terminal identifier"
    },
    {
      name        = "transaction_timestamp"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "When the transaction occurred"
    },
    {
      name        = "total_amount"
      type        = "FLOAT"
      mode        = "REQUIRED"
      description = "Total transaction amount"
    },
    {
      name        = "payment_method"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Payment method used"
    },
    {
      name        = "items"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "JSON string of items purchased"
    }
  ])

  labels = {
    env     = "dev"
    project = "pos-retail-analysis"
  }
}