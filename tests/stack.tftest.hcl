# `terraform test` (part of `make test`): plans the whole stack in each mode against mocked
# providers, so the switches (Timestream, Redshift, data lake) are checked without an
# AWS account. Only the real archive provider runs (it zips src/).

mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = { account_id = "123456789012" }
  }
  mock_data "aws_iam_role" {
    defaults = { arn = "arn:aws:iam::123456789012:role/LabRole" }
  }
  mock_data "aws_vpc" {
    defaults = { id = "vpc-0123456789abcdef0" }
  }
  mock_data "aws_subnets" {
    defaults = { ids = ["subnet-0000000000000000a", "subnet-0000000000000000b", "subnet-0000000000000000c"] }
  }
  mock_data "aws_ssm_parameter" {
    defaults = { insecure_value = "ami-0123456789abcdef0" }
  }
}

mock_provider "http" {
  mock_data "http" {
    defaults = { response_body = "203.0.113.10\n" }
  }
}

mock_provider "random" {}

run "defaults_build_every_store" {
  command = plan

  assert {
    condition     = length(aws_kinesis_firehose_delivery_stream.archive) == 1 && length(aws_glue_catalog_table.telemetry_raw) == 1
    error_message = "archive_mode = firehose should create Firehose and the Glue table"
  }
  assert {
    condition     = length(aws_timestreamwrite_table.telemetry) == 1 && length(aws_lambda_function.timeseries_writer) == 1
    error_message = "Timestream and its writer should be on by default"
  }
  assert {
    condition     = length(aws_redshiftserverless_workgroup.warehouse) == 1 && length(aws_redshift_cluster.warehouse) == 0
    error_message = "Redshift Serverless is the default warehouse"
  }
  assert {
    condition     = length(aws_lambda_event_source_mapping.warehouse) == 1 && aws_lambda_event_source_mapping.warehouse[0].batch_size == 10000
    error_message = "the warehouse loader should read the stream in large micro-batches"
  }
  assert {
    condition     = one(aws_security_group.grafana.ingress).cidr_blocks == tolist(["203.0.113.10/32"])
    error_message = "Grafana should only be reachable from the detected public IP"
  }
  assert {
    condition     = aws_lambda_function.processor.environment[0].variables["ARCHIVE_BUCKET"] == ""
    error_message = "with Firehose, the processor must not archive to S3 itself"
  }
  assert {
    condition     = length(local.grafana_datasources) == 4 && length(local.grafana_dashboards) == 3
    error_message = "Grafana should get CloudWatch, Timestream, Redshift and Athena, plus three dashboards"
  }
  assert {
    condition     = strcontains(local.grafana_dashboards["eventops-pipeline.json"], "eventops-stream-processor") && length(regexall("__[A-Z][A-Z_]+__", join("", values(local.grafana_dashboards)))) == 0
    error_message = "every dashboard placeholder should be replaced"
  }
}

run "minimal_without_optional_stores" {
  command = plan

  variables {
    enable_timestream = false
    redshift_mode     = "off"
    archive_mode      = "off"
  }

  assert {
    condition     = length(aws_timestreamwrite_database.telemetry) == 0 && length(aws_lambda_function.timeseries_writer) == 0
    error_message = "Timestream off should remove the database and its writer"
  }
  assert {
    condition     = length(aws_lambda_function.warehouse_loader) == 0 && length(aws_redshiftserverless_namespace.warehouse) == 0
    error_message = "redshift_mode = off should remove the warehouse and its loader"
  }
  assert {
    condition     = length(aws_s3_bucket.lake) == 0 && length(aws_athena_workgroup.lake) == 0
    error_message = "archive_mode = off should remove the data lake"
  }
  assert {
    condition     = length(local.grafana_datasources) == 1 && keys(local.grafana_dashboards) == ["eventops-pipeline.json"]
    error_message = "Grafana keeps CloudWatch and the pipeline dashboard"
  }
  assert {
    condition     = local.grafana_home_dashboard == "eventops-pipeline.json"
    error_message = "without Timestream the home dashboard is the pipeline one"
  }
}

run "provisioned_redshift_and_lambda_archive" {
  command = plan

  variables {
    redshift_mode = "provisioned"
    archive_mode  = "lambda"
    name          = "eventops-b"
  }

  assert {
    condition     = length(aws_redshift_cluster.warehouse) == 1 && aws_redshift_cluster.warehouse[0].node_type == "ra3.large"
    error_message = "provisioned mode should create a single ra3.large cluster"
  }
  assert {
    condition     = aws_lambda_function.warehouse_loader[0].environment[0].variables["REDSHIFT_DB_USER"] == "eventops_b_admin"
    error_message = "the loader needs a database user for a provisioned cluster"
  }
  assert {
    condition     = length(aws_kinesis_firehose_delivery_stream.archive) == 0 && aws_lambda_function.processor.environment[0].variables["ARCHIVE_BUCKET"] == "eventops-b-datalake-123456789012-us-east-1"
    error_message = "archive_mode = lambda should make the processor write to an account-specific bucket"
  }
  assert {
    condition     = aws_glue_catalog_database.lake[0].name == "eventops_b_lake"
    error_message = "Glue names cannot contain dashes"
  }
}

run "rejects_unknown_modes" {
  command = plan

  variables {
    redshift_mode = "dc2"
  }

  expect_failures = [var.redshift_mode]
}
