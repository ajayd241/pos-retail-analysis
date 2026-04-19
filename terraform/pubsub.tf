resource "google_pubsub_topic" "pos_transactions" {
  name    = var.pubsub_topic_name
  project = var.project_id

  message_retention_duration = "86400s"

  labels = {
    env     = "dev"
    project = "pos-retail-analysis"
  }
}

resource "google_pubsub_subscription" "pos_transactions_sub" {
  name    = var.pubsub_subscription_name
  topic   = google_pubsub_topic.pos_transactions.name
  project = var.project_id

  ack_deadline_seconds = 60

  expiration_policy {
    ttl = "604800s"
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  labels = {
    env     = "dev"
    project = "pos-retail-analysis"
  }
}