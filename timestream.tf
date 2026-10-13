# Time-series store for the live Grafana dashboard. Recent data sits in the memory
# store (fast, accepts writes); older data moves to the cheaper magnetic store.
resource "aws_timestreamwrite_database" "telemetry" {
  count = var.enable_timestream ? 1 : 0

  database_name = var.name
}

resource "aws_timestreamwrite_table" "telemetry" {
  count = var.enable_timestream ? 1 : 0

  database_name = aws_timestreamwrite_database.telemetry[0].database_name
  table_name    = "telemetry"

  retention_properties {
    memory_store_retention_period_in_hours  = var.timestream_memory_hours
    magnetic_store_retention_period_in_days = var.timestream_magnetic_days
  }
}
