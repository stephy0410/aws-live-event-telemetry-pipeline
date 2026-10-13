output "aws_account_id" {
  description = "Account the stack is deployed in (check it after switching AWS Academy accounts)"
  value       = local.account_id
}

output "dashboard_url" {
  description = "Planner-facing live operations page (public, read-only)"
  value       = aws_apigatewayv2_stage.dashboard.invoke_url
}

output "grafana_url" {
  description = "Grafana (anonymous read-only; sign in as admin to edit). Reachable only from grafana_allowed_cidrs. Takes ~3 minutes after the first apply to install."
  value       = local.grafana_url
}

output "grafana_admin_password" {
  description = "Password of the Grafana admin user (terraform output -raw grafana_admin_password)"
  value       = random_password.grafana_admin.result
  sensitive   = true
}

output "timestream_table" {
  description = "Timestream database.table with every reading (null when enable_timestream = false)"
  value       = var.enable_timestream ? "${aws_timestreamwrite_database.telemetry[0].database_name}.${aws_timestreamwrite_table.telemetry[0].table_name}" : null
}

output "redshift_target" {
  description = "Where the warehouse loader writes (Redshift Serverless workgroup or cluster), null when redshift_mode = off"
  value = !local.redshift_enabled ? null : (
    local.redshift_serverless
    ? "serverless workgroup ${aws_redshiftserverless_workgroup.warehouse[0].workgroup_name}, database ${local.redshift_database}"
    : "cluster ${aws_redshift_cluster.warehouse[0].cluster_identifier}, database ${local.redshift_database}"
  )
}

output "kinesis_stream_name" {
  description = "Stream the sensor simulator writes to"
  value       = aws_kinesis_stream.telemetry.name
}

output "alerts_topic_arn" {
  description = "SNS topic the processor publishes alerts to"
  value       = aws_sns_topic.alerts.arn
}

output "alerts_queue_url" {
  description = "SQS queue subscribed to the alerts topic"
  value       = aws_sqs_queue.alerts.url
}

output "dynamodb_table_name" {
  description = "Single table holding live state, aggregates and alerts"
  value       = aws_dynamodb_table.ops.name
}

output "data_lake_bucket" {
  description = "S3 bucket with the raw telemetry archive (null when archive_mode = off)"
  value       = local.data_lake_enabled ? aws_s3_bucket.lake[0].bucket : null
}

output "athena_workgroup" {
  description = "Athena workgroup with the saved queries (null when archive_mode = off)"
  value       = local.data_lake_enabled ? aws_athena_workgroup.lake[0].name : null
}

output "glue_database" {
  description = "Glue database holding the telemetry_raw table (null when archive_mode = off)"
  value       = local.data_lake_enabled ? aws_glue_catalog_database.lake[0].name : null
}

output "simulate_cmd" {
  description = "Start the sensor fleet against this stream"
  value       = "make simulate"
}
