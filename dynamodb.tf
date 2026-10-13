# Single-table design: the key layout (pk/sk/gsi1) is documented in README.md and
# built in src/common.py. On-demand billing, so it absorbs ingestion bursts without
# capacity planning.
resource "aws_dynamodb_table" "ops" {
  name         = "${var.name}-ops"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  attribute {
    name = "gsi1pk"
    type = "S"
  }

  attribute {
    name = "gsi1sk"
    type = "S"
  }

  # Overloaded index: events by planner (tenant view) and alerts by day.
  global_secondary_index {
    name            = "gsi1"
    projection_type = "ALL"

    key_schema {
      attribute_name = "gsi1pk"
      key_type       = "HASH"
    }

    key_schema {
      attribute_name = "gsi1sk"
      key_type       = "RANGE"
    }
  }

  # Per-minute aggregates, platform stats, alerts and alert cooldowns expire on their own.
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
}
