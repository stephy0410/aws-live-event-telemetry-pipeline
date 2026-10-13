# Batch layer: every raw record is kept in S3 as GZIP'd JSON lines, partitioned by hour,
# and queried with SQL through Athena. DynamoDB answers "what is happening now";
# the data lake answers "what happened" across all events and all time.

resource "aws_s3_bucket" "lake" {
  count = local.data_lake_enabled ? 1 : 0

  bucket        = local.data_lake_bucket
  force_destroy = true # lab: `terraform destroy` also empties it
}

resource "aws_s3_bucket_public_access_block" "lake" {
  count = local.data_lake_enabled ? 1 : 0

  bucket                  = aws_s3_bucket.lake[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  count = local.data_lake_enabled ? 1 : 0

  bucket = aws_s3_bucket.lake[0].id

  rule {
    id     = "expire-raw"
    status = "Enabled"
    filter {
      prefix = "raw/"
    }
    expiration {
      days = var.data_lake_retention_days
    }
  }

  rule {
    id     = "expire-scratch"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = 7
    }
  }

  rule {
    id     = "expire-errors"
    status = "Enabled"
    filter {
      prefix = "errors/"
    }
    expiration {
      days = 7
    }
  }
}

# --- Firehose: stream -> S3 (archive_mode = "firehose") ---

resource "aws_cloudwatch_log_group" "firehose" {
  count = var.archive_mode == "firehose" ? 1 : 0

  name              = "/aws/kinesisfirehose/${var.name}-archive"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_stream" "firehose" {
  count = var.archive_mode == "firehose" ? 1 : 0

  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose[0].name
}

# A second, independent consumer of the same stream: the processor and Firehose each
# read every record at their own pace (that is the point of a stream vs. a queue).
resource "aws_kinesis_firehose_delivery_stream" "archive" {
  count = var.archive_mode == "firehose" ? 1 : 0

  name        = "${var.name}-archive"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.telemetry.arn
    role_arn           = data.aws_iam_role.lambda.arn
  }

  extended_s3_configuration {
    role_arn   = data.aws_iam_role.lambda.arn
    bucket_arn = aws_s3_bucket.lake[0].arn

    # Same layout the processor uses in archive_mode = "lambda", so Athena works with both.
    prefix              = "raw/dt=!{timestamp:yyyy-MM-dd-HH}/"
    error_output_prefix = "errors/!{firehose:error-output-type}/dt=!{timestamp:yyyy-MM-dd-HH}/"

    # Flush every 60 s or 5 MB, whichever comes first.
    buffering_interval = 60
    buffering_size     = 5
    compression_format = "GZIP"

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose[0].name
      log_stream_name = aws_cloudwatch_log_stream.firehose[0].name
    }
  }
}

# --- Glue catalog + Athena ---

resource "aws_glue_catalog_database" "lake" {
  count = local.data_lake_enabled ? 1 : 0

  name        = local.glue_database
  description = "Raw live-event telemetry archived from Kinesis"
}

# Partition projection: Athena computes the partition paths from the dt filter instead
# of needing a crawler or MSCK REPAIR to discover new hours.
resource "aws_glue_catalog_table" "telemetry_raw" {
  count = local.data_lake_enabled ? 1 : 0

  name          = "telemetry_raw"
  database_name = aws_glue_catalog_database.lake[0].name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"              = "json"
    "compressionType"             = "gzip"
    "projection.enabled"          = "true"
    "projection.dt.type"          = "date"
    "projection.dt.format"        = "yyyy-MM-dd-HH"
    "projection.dt.range"         = "NOW-${var.data_lake_retention_days}DAYS,NOW"
    "projection.dt.interval"      = "1"
    "projection.dt.interval.unit" = "HOURS"
    "storage.location.template"   = "s3://${aws_s3_bucket.lake[0].bucket}/raw/dt=$${dt}/"
  }

  partition_keys {
    name    = "dt"
    type    = "string"
    comment = "UTC hour the record reached Kinesis, yyyy-MM-dd-HH"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake[0].bucket}/raw/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
      parameters = {
        "ignore.malformed.json" = "true"
      }
    }

    # Both record kinds share the table; columns a kind does not have are NULL.
    columns {
      name    = "kind"
      type    = "string"
      comment = "sensor.reading | event.registered"
    }
    columns {
      name = "event_id"
      type = "string"
    }
    columns {
      name = "planner_id"
      type = "string"
    }
    columns {
      name = "ts"
      type = "string"
    }
    columns {
      name = "reading_id"
      type = "string"
    }
    columns {
      name = "zone"
      type = "string"
    }
    columns {
      name = "sensor_id"
      type = "string"
    }
    columns {
      name    = "metric"
      type    = "string"
      comment = "occupancy | temperature_c | noise_db | checkin"
    }
    columns {
      name = "value"
      type = "double"
    }
    columns {
      name = "unit"
      type = "string"
    }
    columns {
      name    = "capacity"
      type    = "int"
      comment = "Zone capacity, on occupancy readings only"
    }
    columns {
      name = "planner_name"
      type = "string"
    }
    columns {
      name = "event_name"
      type = "string"
    }
    columns {
      name = "event_type"
      type = "string"
    }
    columns {
      name = "venue"
      type = "string"
    }
    columns {
      name = "city"
      type = "string"
    }
    columns {
      name = "guests_expected"
      type = "int"
    }
    columns {
      name = "starts_at"
      type = "string"
    }
    columns {
      name = "zones"
      type = "array<struct<zone:string,capacity:int>>"
    }
  }
}

resource "aws_athena_workgroup" "lake" {
  count = local.data_lake_enabled ? 1 : 0

  name          = var.name
  force_destroy = true

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake[0].bucket}/athena-results/"
    }
  }
}

# Ready-made queries, listed under Athena > Saved queries in the workgroup.
locals {
  athena_queries = {
    "readings-per-event-and-metric" = {
      description = "How much telemetry each event produced in the last 24 h"
      query       = <<-SQL
        SELECT event_id, metric, count(*) AS readings, round(avg(value), 1) AS avg_value
        FROM telemetry_raw
        WHERE kind = 'sensor.reading'
          AND dt >= date_format(current_timestamp - interval '1' day, '%Y-%m-%d-%H')
        GROUP BY event_id, metric
        ORDER BY event_id, metric;
      SQL
    }
    "peak-occupancy-by-zone" = {
      description = "Fullest each zone got, as a share of its capacity"
      query       = <<-SQL
        SELECT event_id, zone, max(value) AS peak_people, max(capacity) AS capacity,
               round(100 * max(value) / max(capacity), 1) AS peak_pct
        FROM telemetry_raw
        WHERE metric = 'occupancy'
          AND dt >= date_format(current_timestamp - interval '1' day, '%Y-%m-%d-%H')
        GROUP BY event_id, zone
        ORDER BY peak_pct DESC;
      SQL
    }
    "cold-chain-incidents" = {
      description = "Cold-room readings above the food-safety limit"
      query       = <<-SQL
        SELECT event_id, sensor_id, ts, value AS celsius
        FROM telemetry_raw
        WHERE zone = 'cold-room' AND metric = 'temperature_c' AND value > ${var.cold_room_max_celsius}
          AND dt >= date_format(current_timestamp - interval '1' day, '%Y-%m-%d-%H')
        ORDER BY ts DESC
        LIMIT 200;
      SQL
    }
    "ingestion-per-minute" = {
      description = "Pipeline throughput: records per minute over the last 6 h"
      query       = <<-SQL
        SELECT date_trunc('minute', from_iso8601_timestamp(ts)) AS minute, count(*) AS records
        FROM telemetry_raw
        WHERE dt >= date_format(current_timestamp - interval '6' hour, '%Y-%m-%d-%H')
        GROUP BY 1
        ORDER BY 1;
      SQL
    }
  }
}

resource "aws_athena_named_query" "lake" {
  for_each = local.data_lake_enabled ? local.athena_queries : {}

  name        = each.key
  description = each.value.description
  workgroup   = aws_athena_workgroup.lake[0].id
  database    = aws_glue_catalog_database.lake[0].name
  query       = each.value.query
}
