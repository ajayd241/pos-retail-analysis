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
  dataset_id          = google_bigquery_dataset.retail_analytics.dataset_id
  table_id            = var.bq_table_id
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "transaction_id",        type = "STRING",    mode = "REQUIRED", description = "Unique transaction identifier" },
    { name = "store_id",              type = "STRING",    mode = "REQUIRED", description = "Store identifier" },
    { name = "terminal_id",           type = "STRING",    mode = "REQUIRED", description = "POS terminal identifier" },
    { name = "transaction_timestamp", type = "TIMESTAMP", mode = "REQUIRED", description = "When the transaction occurred" },
    { name = "total_amount",          type = "FLOAT",     mode = "REQUIRED", description = "Total transaction amount" },
    { name = "payment_method",        type = "STRING",    mode = "NULLABLE", description = "Payment method used" },
    { name = "items",                 type = "STRING",    mode = "NULLABLE", description = "JSON string of items purchased" },
    { name = "transaction_hour",      type = "INTEGER",   mode = "NULLABLE", description = "Hour of transaction for time analytics" },
    { name = "transaction_date",      type = "STRING",    mode = "NULLABLE", description = "Date of transaction YYYY-MM-DD" },
    { name = "is_high_value",         type = "BOOLEAN",   mode = "NULLABLE", description = "True if total_amount over 100" }
  ])

  labels = {
    env     = "dev"
    project = "pos-retail-analysis"
  }
}

resource "google_bigquery_table" "pos_transactions_dead_letter" {
  dataset_id          = google_bigquery_dataset.retail_analytics.dataset_id
  table_id            = "pos_transactions_dead_letter"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "raw_message", type = "STRING", mode = "NULLABLE", description = "Raw unparseable message" },
    { name = "error",       type = "STRING", mode = "NULLABLE", description = "Error message from pipeline" },
    { name = "timestamp",   type = "STRING", mode = "NULLABLE", description = "When the error occurred" }
  ])

  labels = {
    env     = "dev"
    project = "pos-retail-analysis"
  }
}