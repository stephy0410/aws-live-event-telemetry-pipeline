# Warehouse for the analytics Grafana dashboard. Nothing connects to it over the network:
# the loader Lambda and Grafana both use the Redshift Data API (HTTPS + IAM), so the
# security group has no inbound rules and the warehouse is not publicly accessible.

locals {
  redshift_enabled    = var.redshift_mode != "off"
  redshift_serverless = var.redshift_mode == "serverless"
  redshift_database   = replace(var.name, "-", "_")
  redshift_admin_user = "${local.redshift_database}_admin"
}

resource "random_password" "redshift_admin" {
  count = local.redshift_enabled ? 1 : 0

  # Redshift: 8-64 characters with upper, lower and digits; no quotes, slashes, @ or spaces.
  length      = 24
  special     = false
  min_upper   = 2
  min_lower   = 2
  min_numeric = 2
}

resource "aws_security_group" "redshift" {
  count = local.redshift_enabled ? 1 : 0

  name        = "${var.name}-redshift"
  description = "Redshift for ${var.name}: no inbound access, only reached through the Data API"
  vpc_id      = data.aws_vpc.default.id

  egress {
    description = "Outbound (AWS service endpoints)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --- Serverless (default) ---

resource "aws_redshiftserverless_namespace" "warehouse" {
  count = local.redshift_serverless ? 1 : 0

  namespace_name      = var.name
  db_name             = local.redshift_database
  admin_username      = local.redshift_admin_user
  admin_user_password = random_password.redshift_admin[0].result
}

resource "aws_redshiftserverless_workgroup" "warehouse" {
  count = local.redshift_serverless ? 1 : 0

  namespace_name      = aws_redshiftserverless_namespace.warehouse[0].namespace_name
  workgroup_name      = var.name
  base_capacity       = var.redshift_base_capacity
  subnet_ids          = local.subnet_ids
  security_group_ids  = [aws_security_group.redshift[0].id]
  publicly_accessible = false
}

# --- Provisioned (redshift_mode = provisioned) ---

resource "aws_redshift_subnet_group" "warehouse" {
  count = var.redshift_mode == "provisioned" ? 1 : 0

  name       = var.name
  subnet_ids = local.subnet_ids
}

resource "aws_redshift_cluster" "warehouse" {
  count = var.redshift_mode == "provisioned" ? 1 : 0

  cluster_identifier        = var.name
  node_type                 = var.redshift_node_type
  cluster_type              = "single-node"
  database_name             = local.redshift_database
  master_username           = local.redshift_admin_user
  master_password           = random_password.redshift_admin[0].result
  cluster_subnet_group_name = aws_redshift_subnet_group.warehouse[0].name
  vpc_security_group_ids    = [aws_security_group.redshift[0].id]
  publicly_accessible       = false
  encrypted                 = true
  skip_final_snapshot       = true # lab: nothing to keep after destroy
}

locals {
  # Data API target, shared by the loader Lambda and the Grafana data source.
  redshift_env = !local.redshift_enabled ? {} : merge(
    { REDSHIFT_DATABASE = local.redshift_database },
    local.redshift_serverless
    ? { REDSHIFT_WORKGROUP = aws_redshiftserverless_workgroup.warehouse[0].workgroup_name }
    : { REDSHIFT_CLUSTER_ID = aws_redshift_cluster.warehouse[0].cluster_identifier, REDSHIFT_DB_USER = local.redshift_admin_user },
  )
}

# Tables and the Grafana view are created by the loader itself (as LabRole, the same
# identity Grafana uses), once the warehouse is up.
resource "aws_lambda_invocation" "warehouse_schema" {
  count = local.redshift_enabled ? 1 : 0

  function_name = aws_lambda_function.warehouse_loader[0].function_name
  input         = jsonencode({ action = "init_schema" })

  triggers = {
    warehouse = local.redshift_serverless ? aws_redshiftserverless_workgroup.warehouse[0].id : aws_redshift_cluster.warehouse[0].id
    code      = data.archive_file.lambda.output_base64sha256
  }
}
