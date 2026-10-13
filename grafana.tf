# Grafana OSS on EC2. Amazon Managed Grafana needs IAM Identity Center, which AWS
# Academy accounts do not have, so Grafana runs on a small instance instead, fully
# configured at boot by cloud-init: data sources and dashboards are provisioned from
# files, and the AWS data sources authenticate with the instance profile (no keys).

data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

resource "random_password" "grafana_admin" {
  length  = 20
  special = false
}

resource "aws_security_group" "grafana" {
  name        = "${var.name}-grafana"
  description = "Grafana for ${var.name}: port 3000 from allowed CIDRs only"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "Grafana UI"
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = local.grafana_cidrs
  }

  egress {
    description = "Package repos, plugin downloads and AWS APIs"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

locals {
  grafana_plugins = compact([
    var.enable_timestream ? "grafana-timestream-datasource" : "",
    local.redshift_enabled ? "grafana-redshift-datasource" : "",
    local.data_lake_enabled ? "grafana-athena-datasource" : "",
  ])

  grafana_datasources = concat(
    [{
      name      = "CloudWatch"
      type      = "cloudwatch"
      uid       = "cloudwatch"
      isDefault = !var.enable_timestream
      jsonData  = { authType = "default", defaultRegion = var.region }
    }],
    var.enable_timestream ? [{
      name      = "Timestream"
      type      = "grafana-timestream-datasource"
      uid       = "timestream"
      isDefault = true
      jsonData = {
        authType        = "default"
        defaultRegion   = var.region
        defaultDatabase = "\"${aws_timestreamwrite_database.telemetry[0].database_name}\""
        defaultTable    = "\"${aws_timestreamwrite_table.telemetry[0].table_name}\""
      }
    }] : [],
    local.redshift_enabled ? [{
      name = "Redshift"
      type = "grafana-redshift-datasource"
      uid  = "redshift"
      jsonData = merge(
        { authType = "default", defaultRegion = var.region, database = local.redshift_database },
        local.redshift_serverless
        ? { useServerless = true, workgroupName = aws_redshiftserverless_workgroup.warehouse[0].workgroup_name }
        : { clusterIdentifier = aws_redshift_cluster.warehouse[0].cluster_identifier, dbUser = local.redshift_admin_user },
      )
    }] : [],
    local.data_lake_enabled ? [{
      name = "Athena"
      type = "grafana-athena-datasource"
      uid  = "athena"
      jsonData = {
        authType      = "default"
        defaultRegion = var.region
        catalog       = "AwsDataCatalog"
        database      = local.glue_database
        workgroup     = aws_athena_workgroup.lake[0].name
      }
    }] : [],
  )

  # Dashboards are plain Grafana JSON (grafana/dashboards/), with a few placeholders
  # filled in here so names and alert limits always match this deployment.
  grafana_dashboard_files = concat(
    ["eventops-pipeline.json"],
    var.enable_timestream ? ["eventops-live.json"] : [],
    local.redshift_enabled ? ["eventops-warehouse.json"] : [],
  )
  grafana_dashboards = {
    for f in local.grafana_dashboard_files : f => replace(replace(replace(replace(replace(
      file("${path.module}/grafana/dashboards/${f}"),
      "__NAME__", var.name),
      "__COLD_ROOM_MAX_C__", tostring(var.cold_room_max_celsius)),
      "__NOISE_WARNING_DB__", tostring(var.noise_warning_db)),
      "__NOISE_CRITICAL_DB__", tostring(var.noise_critical_db)),
    "__OCCUPANCY_WARNING_PCT__", tostring(floor(var.occupancy_warning_ratio * 100 + 0.5)))
  }
  grafana_home_dashboard = var.enable_timestream ? "eventops-live.json" : "eventops-pipeline.json"

  grafana_cloud_config = {
    write_files = concat(
      [
        {
          path    = "/etc/yum.repos.d/grafana.repo"
          content = <<-EOT
            [grafana]
            name=grafana
            baseurl=https://rpm.grafana.com
            repo_gpgcheck=1
            enabled=1
            gpgcheck=1
            gpgkey=https://rpm.grafana.com/gpg.key
            sslverify=1
            sslcacert=/etc/pki/tls/certs/ca-bundle.crt
          EOT
        },
        {
          # Settings as environment variables in a systemd drop-in, so the package's own
          # grafana.ini stays untouched.
          path        = "/etc/systemd/system/grafana-server.service.d/eventops.conf"
          permissions = "0600"
          content     = <<-EOT
            [Service]
            Environment=GF_SECURITY_ADMIN_PASSWORD=${random_password.grafana_admin.result}
            Environment=GF_AUTH_ANONYMOUS_ENABLED=true
            Environment=GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer
            Environment=GF_USERS_DEFAULT_THEME=dark
            Environment=GF_ANALYTICS_REPORTING_ENABLED=false
            Environment=GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/var/lib/grafana/dashboards/${local.grafana_home_dashboard}
          EOT
        },
        {
          path    = "/etc/grafana/provisioning/datasources/eventops.yaml"
          content = yamlencode({ apiVersion = 1, datasources = local.grafana_datasources })
        },
        {
          path = "/etc/grafana/provisioning/dashboards/eventops.yaml"
          content = yamlencode({
            apiVersion = 1
            providers = [{
              name                  = "eventops"
              folder                = "EventOps"
              type                  = "file"
              allowUiUpdates        = true
              options               = { path = "/var/lib/grafana/dashboards" }
              updateIntervalSeconds = 60
            }]
          })
        },
      ],
      [for f, body in local.grafana_dashboards : { path = "/var/lib/grafana/dashboards/${f}", content = body }],
    )
    runcmd = concat(
      ["dnf install -y grafana"],
      [for p in local.grafana_plugins : "grafana cli plugins install ${p} || grafana-cli plugins install ${p}"],
      [
        "chown -R grafana:grafana /var/lib/grafana /etc/grafana/provisioning",
        "systemctl daemon-reload",
        "systemctl enable --now grafana-server",
      ],
    )
  }
}

resource "aws_instance" "grafana" {
  ami                    = data.aws_ssm_parameter.al2023.insecure_value
  instance_type          = var.grafana_instance_type
  subnet_id              = local.subnet_ids[0]
  vpc_security_group_ids = [aws_security_group.grafana.id]
  iam_instance_profile   = var.instance_profile_name

  # Compressed so the dashboards fit in the 16 KB user-data limit; cloud-init unzips it.
  user_data_base64            = base64gzip("#cloud-config\n${yamlencode(local.grafana_cloud_config)}")
  user_data_replace_on_change = true

  metadata_options {
    http_tokens                 = "required" # IMDSv2 only
    http_put_response_hop_limit = 1
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = 10
    encrypted   = true
  }

  tags = {
    Name = "${var.name}-grafana"
  }

  lifecycle {
    ignore_changes = [ami] # a newer AL2023 image alone is no reason to rebuild Grafana
  }
}

# Stable address: Learner Lab stops instances when the session ends, and a stopped
# instance would otherwise come back with a different public IP.
resource "aws_eip" "grafana" {
  instance = aws_instance.grafana.id
  domain   = "vpc"

  tags = {
    Name = "${var.name}-grafana"
  }
}

locals {
  grafana_url = "http://${aws_eip.grafana.public_ip}:3000"
}
