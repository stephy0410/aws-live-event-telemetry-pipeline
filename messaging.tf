# --- Alerts topic: the stream processor publishes here, subscribers fan out ---

resource "aws_sns_topic" "alerts" {
  name = "${var.name}-alerts"
}

# Durable buffer between the topic and the alert worker: if the worker is down or
# throttled, alerts wait here instead of being lost.
resource "aws_sqs_queue" "alerts" {
  name                       = "${var.name}-alerts"
  visibility_timeout_seconds = 6 * local.alert_worker_timeout # AWS guidance for Lambda consumers
  message_retention_seconds  = 345600                         # 4 days
  receive_wait_time_seconds  = 20                             # long polling
  sqs_managed_sse_enabled    = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.alerts_dlq.arn
    maxReceiveCount     = 3
  })
}

# Alerts the worker failed to store 3 times (e.g. a malformed message) end up here.
resource "aws_sqs_queue" "alerts_dlq" {
  name                      = "${var.name}-alerts-dlq"
  message_retention_seconds = 1209600 # 14 days
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue_policy" "alerts" {
  queue_url = aws_sqs_queue.alerts.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowAlertsTopic"
      Effect    = "Allow"
      Principal = { Service = "sns.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.alerts.arn
      Condition = { ArnEquals = { "aws:SourceArn" = aws_sns_topic.alerts.arn } }
    }]
  })
}

# Raw delivery: the queue gets the alert JSON itself, not the SNS envelope around it.
resource "aws_sns_topic_subscription" "alerts_queue" {
  topic_arn            = aws_sns_topic.alerts.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.alerts.arn
  raw_message_delivery = true

  depends_on = [aws_sqs_queue_policy.alerts]
}

# Optional human channel: only critical alerts, thanks to a subscription filter policy
# on the message attributes the processor sets.
resource "aws_sns_topic_subscription" "critical_email" {
  count = var.alert_email != "" ? 1 : 0

  topic_arn     = aws_sns_topic.alerts.arn
  protocol      = "email"
  endpoint      = var.alert_email
  filter_policy = jsonencode({ severity = ["critical"] })
}

# --- Stream consumer failures ---

# On-failure destination of every Kinesis event source mapping: after the retries are
# exhausted, Lambda skips the batch (so it does not block the shard) and records here
# which consumer, shard and sequence-number range failed, for replay or debugging.
resource "aws_sqs_queue" "stream_dlq" {
  name                      = "${var.name}-stream-dlq"
  message_retention_seconds = 1209600 # 14 days
  sqs_managed_sse_enabled   = true
}
