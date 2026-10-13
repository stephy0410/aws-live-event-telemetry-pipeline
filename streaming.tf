# Ingestion backbone. Every sensor reading and every "event registered" message lands
# here first. Producers use event_id as the partition key, so all records of one event
# go to the same shard and are read in order; different events spread across shards.
resource "aws_kinesis_stream" "telemetry" {
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
