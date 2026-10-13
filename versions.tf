terraform {
  required_version = ">= 1.11"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7"
    }
    http = {
      source  = "hashicorp/http"
      version = "~> 3.4"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Partial backend configuration: nothing here is tied to one AWS account.
  # `make init` (scripts/bootstrap-backend.sh) creates a state bucket named after the
  # account the current credentials belong to and passes it in through backend.hcl.
  # Moving to a new AWS Academy account is: new credentials -> `make init` -> `make deploy`.
  # Native S3 locking (use_lockfile) instead of a DynamoDB lock table.
  backend "s3" {
    key          = "aws-live-event-telemetry-pipeline/terraform.tfstate"
    encrypt      = true
    use_lockfile = true
  }
}
