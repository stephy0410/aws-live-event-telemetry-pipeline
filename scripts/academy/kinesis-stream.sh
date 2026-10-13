#!/usr/bin/env bash
# Creates (idempotently) or deletes the telemetry stream with the AWS CLI.
# Used when academy_compat = true: AWS Academy denies kinesis:IncreaseStreamRetentionPeriod,
# which the Terraform AWS provider calls on every stream it creates, even for the default 24 h.
# Env: STREAM, SHARDS, TF_REGION, TF_PROFILE (optional).
set -euo pipefail
export AWS_REGION="$TF_REGION"
if [ -n "${TF_PROFILE:-}" ]; then export AWS_PROFILE="$TF_PROFILE"; fi

case "$1" in
  create)
    if ! aws kinesis describe-stream-summary --stream-name "$STREAM" >/dev/null 2>&1; then
      aws kinesis create-stream --stream-name "$STREAM" --shard-count "$SHARDS" --stream-mode-details StreamMode=PROVISIONED
    fi
    aws kinesis wait stream-exists --stream-name "$STREAM"
    encryption="$(aws kinesis describe-stream-summary --stream-name "$STREAM" --query StreamDescriptionSummary.EncryptionType --output text)"
    if [ "$encryption" = "NONE" ]; then
      aws kinesis start-stream-encryption --stream-name "$STREAM" --encryption-type KMS --key-id alias/aws/kinesis
      aws kinesis wait stream-exists --stream-name "$STREAM"
    fi
    ;;
  delete)
    aws kinesis delete-stream --stream-name "$STREAM" --enforce-consumer-deletion || true
    aws kinesis wait stream-not-exists --stream-name "$STREAM" || true
    ;;
  *) echo "usage: $0 create|delete" >&2; exit 2 ;;
esac
