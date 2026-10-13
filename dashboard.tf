# --- Planner-facing web app: API Gateway (HTTP API) -> Lambda -> DynamoDB ---
# (The operations and analytics dashboards live in Grafana: grafana.tf.)

resource "aws_lambda_function" "dashboard" {
  function_name = "${var.name}-dashboard-api"
  description   = "Serves the planner-facing live operations page (HTML) and its read-only JSON API over DynamoDB"
  role          = data.aws_iam_role.lambda.arn

  runtime       = local.lambda_runtime
  architectures = ["arm64"]
  handler       = "dashboard_api.handler"
  memory_size   = 256
  timeout       = 10

  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256

  environment {
    variables = merge(local.lambda_common_env, local.alert_rule_env, {
      GRAFANA_URL = local.grafana_url
    })
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.lambda["dashboard-api"].name
  }
}

resource "aws_apigatewayv2_api" "dashboard" {
  name          = "${var.name}-dashboard"
  protocol_type = "HTTP"
  description   = "Live event operations dashboard"
}

resource "aws_apigatewayv2_integration" "dashboard" {
  api_id                 = aws_apigatewayv2_api.dashboard.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.dashboard.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "dashboard" {
  for_each = toset([
    "GET /",
    "GET /api/overview",
    "GET /api/planners/{planner_id}/events",
    "GET /api/events/{event_id}",
  ])

  api_id    = aws_apigatewayv2_api.dashboard.id
  route_key = each.key
  target    = "integrations/${aws_apigatewayv2_integration.dashboard.id}"
}

resource "aws_apigatewayv2_stage" "dashboard" {
  api_id      = aws_apigatewayv2_api.dashboard.id
  name        = "$default"
  auto_deploy = true

  # The dashboard is public and read-only; throttling caps what a stray script can cost.
  default_route_settings {
    throttling_burst_limit = 50
    throttling_rate_limit  = 20
  }
}

resource "aws_lambda_permission" "dashboard_apigw" {
  statement_id  = "AllowApiGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.dashboard.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.dashboard.execution_arn}/*/*"
}
