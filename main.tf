data "aws_caller_identity" "current" {}

# AWS Academy Learner Lab accounts cannot create IAM roles; every Lambda (and Firehose)
# runs as the pre-provisioned LabRole. Looked up by name, so no account ID is hardcoded.
data "aws_iam_role" "lambda" {
  name = var.lambda_role_name
}

locals {
  account_id = data.aws_caller_identity.current.account_id

  # S3 bucket names are global, so they embed the account ID and region: the same code
  # deploys into any account without name collisions or edits.
  data_lake_enabled = var.archive_mode != "off"
  data_lake_bucket  = "${var.name}-datalake-${local.account_id}-${var.region}"

  # Glue/Athena identifiers only allow lowercase letters, digits and underscores.
  glue_database = "${replace(var.name, "-", "_")}_lake"
}
