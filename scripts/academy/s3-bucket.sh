#!/usr/bin/env bash
# Creates (idempotently) or empties and deletes the data lake bucket with the AWS CLI.
# Used when academy_compat = true: AWS Academy denies s3:GetBucketObjectLockConfiguration,
# which the Terraform AWS provider reads on every aws_s3_bucket. Settings on the bucket
# (public access block, lifecycle) stay regular Terraform resources.
# Env: BUCKET, TF_REGION, TF_PROFILE (optional).
set -euo pipefail
export AWS_REGION="$TF_REGION"
if [ -n "${TF_PROFILE:-}" ]; then export AWS_PROFILE="$TF_PROFILE"; fi

case "$1" in
  create)
    if ! aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
      if [ "$AWS_REGION" = "us-east-1" ]; then
        aws s3api create-bucket --bucket "$BUCKET" >/dev/null
      else
        aws s3api create-bucket --bucket "$BUCKET" --create-bucket-configuration "LocationConstraint=$AWS_REGION" >/dev/null
      fi
      aws s3api wait bucket-exists --bucket "$BUCKET"
    fi
    ;;
  delete)
    aws s3 rb "s3://$BUCKET" --force || true
    ;;
  *) echo "usage: $0 create|delete" >&2; exit 2 ;;
esac
