# Default VPC and its default subnets: present in every account (including each new
# AWS Academy account), so nothing here is hardcoded.
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }

  filter {
    name   = "default-for-az"
    values = ["true"]
  }

  filter {
    name   = "availability-zone-id"
    values = var.availability_zone_ids
  }
}

# Public IP of whoever runs terraform: Grafana is reachable only from there by default.
data "http" "my_ip" {
  url = "https://checkip.amazonaws.com"
}

locals {
  subnet_ids    = sort(data.aws_subnets.default.ids)
  grafana_cidrs = length(var.grafana_allowed_cidrs) > 0 ? var.grafana_allowed_cidrs : ["${chomp(data.http.my_ip.response_body)}/32"]
}
