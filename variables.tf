variable "region" {
  description = "AWS region. AWS Academy Learner Lab only allows us-east-1 and us-west-2."
  type        = string
  default     = "us-east-1"
}

variable "profile" {
  description = "AWS CLI profile with the AWS Academy Learner Lab credentials. Empty string = default credential chain."
  type        = string
  default     = "academy"
}

variable "owner" {
  description = "Value of the Owner tag on every resource"
  type        = string
  default     = "stephanie.borrego"
}

variable "name" {
  description = "Name prefix for every resource in this project"
  type        = string
  default     = "eventops"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,20}$", var.name))
    error_message = "Use 3-21 lowercase letters, digits or dashes (it is part of S3, Kinesis, SNS and SQS names)."
  }
}

variable "academy_compat" {
  description = "AWS Academy's policies deny two calls the Terraform AWS provider makes unconditionally: kinesis:IncreaseStreamRetentionPeriod (on every new stream) and s3:GetBucketObjectLockConfiguration (on every bucket read). true = create the stream and the data lake bucket with the AWS CLI (scripts/academy/) instead. Set false in a regular AWS account."
  type        = bool
  default     = true
}

variable "lambda_role_name" {
  description = "Existing IAM role every Lambda (and Firehose) runs as. AWS Academy accounts cannot create roles, so this is the pre-provisioned LabRole."
  type        = string
  default     = "LabRole"
}

variable "instance_profile_name" {
  description = "Existing instance profile for the Grafana EC2 instance (AWS Academy: LabInstanceProfile, which carries LabRole). Grafana's AWS data sources use it, so no access keys are stored anywhere."
  type        = string
  default     = "LabInstanceProfile"
}

variable "availability_zone_ids" {
  description = "AZ IDs whose default subnets host Redshift Serverless (needs 3) and Grafana. AZ IDs, unlike names, mean the same physical AZ in every account. use1-az3 is skipped because several services do not support it."
  type        = list(string)
  default     = ["use1-az1", "use1-az2", "use1-az4"]
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention for every log group in the project"
  type        = number
  default     = 7
}

# --- Streaming ---

variable "kinesis_shard_count" {
  description = "Shards in the telemetry stream. Each shard ingests up to 1,000 records/s or 1 MB/s, and gets its own processor Lambda."
  type        = number
  default     = 2
}

variable "kinesis_retention_hours" {
  description = "How long records stay in the stream (replay window). 24 is the minimum and the cheapest."
  type        = number
  default     = 24
}

variable "processor_batch_size" {
  description = "Max records per stream-processor invocation. Bigger batches = fewer, larger DynamoDB writes (readings are pre-aggregated per batch)."
  type        = number
  default     = 1000
}

variable "processor_batching_window_seconds" {
  description = "How long Lambda waits to fill a batch before invoking. Trades a little latency for far fewer invocations."
  type        = number
  default     = 2
}

variable "processor_parallelization_factor" {
  description = "Concurrent processor Lambdas per shard (1-10). Ordering is still kept per partition key (event_id). New AWS accounts can have a concurrency quota as low as 10 for the whole account, so keep shards x factor small there."
  type        = number
  default     = 1
}

# --- Alert rules (evaluated by the stream processor) ---

variable "occupancy_warning_ratio" {
  description = "Occupancy / capacity ratio that raises a 'near capacity' warning (>= 1.0 is always critical)"
  type        = number
  default     = 0.9
}

variable "cold_room_max_celsius" {
  description = "Food-safety limit for the catering cold room"
  type        = number
  default     = 5
}

variable "comfort_max_celsius" {
  description = "Guest-comfort limit for every other zone"
  type        = number
  default     = 28
}

variable "noise_warning_db" {
  description = "Noise level that raises a warning"
  type        = number
  default     = 95
}

variable "noise_critical_db" {
  description = "Noise level that raises a critical alert (typical venue limit)"
  type        = number
  default     = 105
}

variable "alert_cooldown_seconds" {
  description = "Minimum time between two alerts of the same type and severity for the same zone (sensors report every few seconds)"
  type        = number
  default     = 300
}

variable "alert_email" {
  description = "Optional email subscribed to the alerts topic for CRITICAL alerts only (SNS filter policy). AWS sends a confirmation email first. Empty = no email."
  type        = string
  default     = ""
}

# --- Time-series store: Amazon Timestream (live Grafana dashboard) ---

variable "enable_timestream" {
  description = "Write every reading to Amazon Timestream for LiveAnalytics and add the live Grafana dashboard. Off by default: AWS Academy's lab policy denies every timestream:* action (verified 2026-10-13), and AWS closed LiveAnalytics to new customers on 2025-06-20. Set true in an account that has access."
  type        = bool
  default     = false
}

variable "timestream_memory_hours" {
  description = "Hours readings stay in Timestream's memory store (fast queries, accepts writes)"
  type        = number
  default     = 24
}

variable "timestream_magnetic_days" {
  description = "Days readings stay in Timestream's magnetic store after the memory store"
  type        = number
  default     = 7
}

# --- Warehouse: Amazon Redshift (analytics Grafana dashboard) ---

variable "redshift_mode" {
  description = <<-EOT
    Where the warehouse loader writes per-minute aggregates:
      provisioned - a single-node cluster of redshift_node_type (default: the only option AWS
                    Academy allows), billed every hour it exists
      serverless  - Redshift Serverless: compute billed only while queries run (denied in AWS Academy)
      off         - no warehouse
  EOT
  type        = string
  default     = "provisioned"

  validation {
    condition     = contains(["serverless", "provisioned", "off"], var.redshift_mode)
    error_message = "redshift_mode must be serverless, provisioned or off."
  }
}

variable "redshift_base_capacity" {
  description = "Redshift Serverless base capacity in RPUs (8 is the smallest accepted everywhere)"
  type        = number
  default     = 8
}

variable "redshift_node_type" {
  description = "Node type when redshift_mode = provisioned. DC2 is retired for new clusters, so the smallest option is ra3.large."
  type        = string
  default     = "ra3.large"
}

variable "warehouse_batching_window_seconds" {
  description = "How long Lambda buffers stream records before each warehouse load (max 300). Fewer, larger loads are cheaper and easier on Redshift."
  type        = number
  default     = 300
}

# --- Web app API (API Gateway -> dashboard Lambda) ---

variable "api_cache_ttl_seconds" {
  description = "Seconds the dashboard Lambda keeps each API response in memory (0 = no cache). The page refreshes every 5 s, so 5 keeps it live."
  type        = number
  default     = 5

  validation {
    condition     = var.api_cache_ttl_seconds >= 0 && var.api_cache_ttl_seconds <= 60
    error_message = "api_cache_ttl_seconds must be between 0 and 60."
  }
}

variable "api_throttle_rate_limit" {
  description = "Steady-state requests per second API Gateway lets through (above it: 429). Caps what a stray script can cost."
  type        = number
  default     = 100
}

variable "api_throttle_burst_limit" {
  description = "Requests API Gateway accepts at once before throttling"
  type        = number
  default     = 200
}

# --- Grafana ---

variable "grafana_instance_type" {
  description = "EC2 instance type for Grafana OSS"
  type        = string
  default     = "t3.small"
}

variable "grafana_allowed_cidrs" {
  description = "CIDRs allowed to reach Grafana on port 3000. Empty = only the public IP of the machine running terraform (detected automatically)."
  type        = list(string)
  default     = []
}

# --- Storage ---

variable "aggregate_ttl_days" {
  description = "Days DynamoDB keeps per-minute aggregates and platform stats before TTL deletes them (the data lake keeps the raw history)"
  type        = number
  default     = 7
}

variable "archive_mode" {
  description = <<-EOT
    How raw records reach the S3 data lake (queried with Athena):
      firehose - Kinesis Data Firehose reads the stream and writes GZIP files (the managed way; Firehose must be able to assume lambda_role_name)
      lambda   - the stream processor writes each batch to S3 itself (fallback when the account does not let Firehose assume LabRole)
      off      - no data lake at all
  EOT
  type        = string
  default     = "firehose"

  validation {
    condition     = contains(["firehose", "lambda", "off"], var.archive_mode)
    error_message = "archive_mode must be firehose, lambda or off."
  }
}

variable "data_lake_retention_days" {
  description = "Days raw files stay in the data lake bucket (also the Athena partition-projection window)"
  type        = number
  default     = 30
}
