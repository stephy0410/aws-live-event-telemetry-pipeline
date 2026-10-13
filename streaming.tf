# Ingestion backbone. Every sensor reading and every "event registered" message lands
# here first. Producers use event_id as the partition key, so all records of one event
# go to the same shard and are read in order; different events spread across shards.
resource "aws_kinesis_stream" "telemetry" {
  count = var.academy_compat ? 0 : 1

  name             = "${var.name}-telemetry"
  shard_count      = var.kinesis_shard_count
  retention_period = var.kinesis_retention_hours

  # Encryption at rest with the AWS-managed key: no key policy to manage, and any
  # principal allowed to use the stream can use the key through Kinesis.
  encryption_type = "KMS"
  kms_key_id      = "alias/aws/kinesis"

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}

# Same stream (24 h retention, KMS-encrypted) created with the AWS CLI, because AWS
# Academy denies a call the provider makes on create (see variable academy_compat).
# Changing the shard count replaces the stream.
resource "terraform_data" "telemetry_stream" {
  count = var.academy_compat ? 1 : 0

  triggers_replace = [var.name, var.kinesis_shard_count, var.region]
  input = {
    name    = "${var.name}-telemetry"
    shards  = tostring(var.kinesis_shard_count)
    region  = var.region
    profile = var.profile
  }

  provisioner "local-exec" {
    command = "scripts/academy/kinesis-stream.sh create"
    environment = {
      STREAM     = self.input.name
      SHARDS     = self.input.shards
      TF_REGION  = self.input.region
      TF_PROFILE = self.input.profile
    }
  }

  provisioner "local-exec" {
    when    = destroy
    command = "scripts/academy/kinesis-stream.sh delete"
    environment = {
      STREAM     = self.input.name
      TF_REGION  = self.input.region
      TF_PROFILE = self.input.profile
    }
  }
}

locals {
  stream_name = var.academy_compat ? terraform_data.telemetry_stream[0].input.name : aws_kinesis_stream.telemetry[0].name
  stream_arn  = "arn:aws:kinesis:${var.region}:${local.account_id}:stream/${local.stream_name}"
}
