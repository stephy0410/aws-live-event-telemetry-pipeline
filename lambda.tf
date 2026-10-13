# One package for every function (they share src/common.py, including the record
# contract). No third-party dependencies: boto3 ships with the Python runtime, so there
# is no build step.
data "archive_file" "lambda" {
  type        = "zip"
  source_dir  = "${path.module}/src"
  output_path = "${path.module}/build/lambda.zip"
  excludes    = ["__pycache__", "**/__pycache__/**"]
}

locals {
  lambda_runtime       = "python3.13"
  alert_worker_timeout = 30

  lambda_common_env = {
    TABLE_NAME        = aws_dynamodb_table.ops.name
    LOG_LEVEL         = "INFO"
    METRICS_NAMESPACE = local.metrics_namespace
    PIPELINE_NAME     = var.name
  }

  # The processor alerts on these; the dashboard colors zones with the same limits.
  alert_rule_env = {
    OCCUPANCY_WARNING_RATIO = tostring(var.occupancy_warning_ratio)
    COLD_ROOM_MAX_C         = tostring(var.cold_room_max_celsius)
    COMFORT_MAX_C           = tostring(var.comfort_max_celsius)
    NOISE_WARNING_DB        = tostring(var.noise_warning_db)
    NOISE_CRITICAL_DB       = tostring(var.noise_critical_db)
  }

  lambda_functions = concat(
    ["stream-processor", "alert-worker", "dashboard-api"],
    var.enable_timestream ? ["timeseries-writer"] : [],
    local.redshift_enabled ? ["warehouse-loader"] : [],
  )

  # Custom metrics the stream consumers emit through CloudWatch Embedded Metric Format.
  metrics_namespace = "EventOps/Pipeline"
}

resource "aws_cloudwatch_log_group" "lambda" {
  for_each = toset(local.lambda_functions)

  name              = "/aws/lambda/${var.name}-${each.key}"
  retention_in_days = var.log_retention_days
}

# --- Kinesis -> stream processor ---

resource "aws_lambda_function" "processor" {
  function_name = "${var.name}-stream-processor"
  description   = "Decodes telemetry batches from Kinesis, updates live state and per-minute aggregates in DynamoDB, publishes alerts to SNS"
  role          = data.aws_iam_role.lambda.arn

  runtime       = local.lambda_runtime
  architectures = ["arm64"]
  handler       = "processor.handler"
  memory_size   = 512
  timeout       = 60

  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256

  environment {
    variables = merge(local.lambda_common_env, local.alert_rule_env, {
      ALERTS_TOPIC_ARN       = aws_sns_topic.alerts.arn
      ARCHIVE_BUCKET         = var.archive_mode == "lambda" ? aws_s3_bucket.lake[0].bucket : ""
      AGGREGATE_TTL_DAYS     = tostring(var.aggregate_ttl_days)
      ALERT_COOLDOWN_SECONDS = tostring(var.alert_cooldown_seconds)
    })
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.lambda["stream-processor"].name
  }
}

resource "aws_lambda_event_source_mapping" "telemetry" {
  event_source_arn = aws_kinesis_stream.telemetry.arn
  function_name    = aws_lambda_function.processor.arn

  # TRIM_HORIZON: on first deploy, also process whatever producers sent before the
  # mapping existed (nothing is skipped).
  starting_position                  = "TRIM_HORIZON"
  batch_size                         = var.processor_batch_size
  maximum_batching_window_in_seconds = var.processor_batching_window_seconds
  parallelization_factor             = var.processor_parallelization_factor

  # A failing batch is retried, then split in half to isolate the bad record(s); after
  # that it is skipped and its location goes to the DLQ, so one poison record cannot
  # stall the shard (records older than an hour are not worth processing live anyway).
  bisect_batch_on_function_error = true
  maximum_retry_attempts         = 3
  maximum_record_age_in_seconds  = 3600

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.stream_dlq.arn
    }
  }
}

# --- Kinesis -> time-series writer -> Timestream ---
# Same stream, independent consumer: its own shard iterators, retries and DLQ entries.

resource "aws_lambda_function" "timeseries_writer" {
  count = var.enable_timestream ? 1 : 0

  function_name = "${var.name}-timeseries-writer"
  description   = "Writes every sensor reading from Kinesis to Amazon Timestream (live Grafana dashboard)"
  role          = data.aws_iam_role.lambda.arn

  runtime       = local.lambda_runtime
  architectures = ["arm64"]
  handler       = "timeseries_writer.handler"
  memory_size   = 256
  timeout       = 60

  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256

  environment {
    variables = merge(local.lambda_common_env, {
      TIMESTREAM_DATABASE = aws_timestreamwrite_database.telemetry[0].database_name
      TIMESTREAM_TABLE    = aws_timestreamwrite_table.telemetry[0].table_name
    })
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.lambda["timeseries-writer"].name
  }
}

resource "aws_lambda_event_source_mapping" "timeseries" {
  count = var.enable_timestream ? 1 : 0

  event_source_arn = aws_kinesis_stream.telemetry.arn
  function_name    = aws_lambda_function.timeseries_writer[0].arn

  starting_position                  = "TRIM_HORIZON"
  batch_size                         = 500
  maximum_batching_window_in_seconds = 1 # near real time: Grafana's live view
  bisect_batch_on_function_error     = true
  maximum_retry_attempts             = 3
  maximum_record_age_in_seconds      = 3600

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.stream_dlq.arn
    }
  }
}

# --- Kinesis -> warehouse loader -> Redshift (micro-batches) ---

resource "aws_lambda_function" "warehouse_loader" {
  count = local.redshift_enabled ? 1 : 0

  function_name = "${var.name}-warehouse-loader"
  description   = "Loads per-minute aggregates from Kinesis into Amazon Redshift in micro-batches (analytics Grafana dashboard)"
  role          = data.aws_iam_role.lambda.arn

  runtime       = local.lambda_runtime
  architectures = ["arm64"]
  handler       = "warehouse_loader.handler"
  memory_size   = 512
  timeout       = 300 # Redshift Serverless may need a moment to resume after being idle

  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256

  environment {
    variables = merge(local.lambda_common_env, local.redshift_env)
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.lambda["warehouse-loader"].name
  }
}

resource "aws_lambda_event_source_mapping" "warehouse" {
  count = local.redshift_enabled ? 1 : 0

  event_source_arn = aws_kinesis_stream.telemetry.arn
  function_name    = aws_lambda_function.warehouse_loader[0].arn

  # A warehouse wants few, large loads: wait up to 5 minutes or 10,000 records.
  starting_position                  = "TRIM_HORIZON"
  batch_size                         = 10000
  maximum_batching_window_in_seconds = var.warehouse_batching_window_seconds
  bisect_batch_on_function_error     = true
  maximum_retry_attempts             = 3
  maximum_record_age_in_seconds      = -1 # keep trying for the whole stream retention: history matters here

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.stream_dlq.arn
    }
  }

  # Start reading only once the tables exist.
  depends_on = [aws_lambda_invocation.warehouse_schema]
}

# --- SNS -> SQS -> alert worker ---

resource "aws_lambda_function" "alert_worker" {
  function_name = "${var.name}-alert-worker"
  description   = "Consumes alerts from SQS, enriches them with a recommended action and stores them in DynamoDB (idempotent)"
  role          = data.aws_iam_role.lambda.arn

  runtime       = local.lambda_runtime
  architectures = ["arm64"]
  handler       = "alert_worker.handler"
  memory_size   = 256
  timeout       = local.alert_worker_timeout

  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256

  environment {
    variables = merge(local.lambda_common_env, {
      AGGREGATE_TTL_DAYS = tostring(var.aggregate_ttl_days)
    })
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.lambda["alert-worker"].name
  }
}

resource "aws_lambda_event_source_mapping" "alerts_queue" {
  event_source_arn = aws_sqs_queue.alerts.arn
  function_name    = aws_lambda_function.alert_worker.arn

  batch_size                         = 10
  maximum_batching_window_in_seconds = 1

  # Only the messages that failed go back to the queue, not the whole batch.
  function_response_types = ["ReportBatchItemFailures"]

  # Caps how many workers the queue can spin up (no reserved concurrency needed, which
  # Learner Lab's 10-execution account limit would not allow).
  scaling_config {
    maximum_concurrency = 2
  }
}
