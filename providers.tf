provider "aws" {
  region = var.region
  # Empty profile = default credential chain (env vars, SSO, ...).
  profile = var.profile != "" ? var.profile : null

  default_tags {
    tags = {
      Project   = "aws-live-event-telemetry-pipeline"
      ManagedBy = "Terraform"
      Owner     = var.owner
    }
  }
}
