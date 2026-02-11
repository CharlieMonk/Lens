output "ecr_repository_url" {
  description = "ECR repository URL for webapp"
  value       = aws_ecr_repository.webapp.repository_url
}

output "api_gateway_url" {
  description = "API Gateway endpoint URL"
  value       = aws_apigatewayv2_stage.main.invoke_url
}

output "api_gateway_id" {
  description = "API Gateway ID"
  value       = aws_apigatewayv2_api.main.id
}

output "service_discovery_arn" {
  description = "Cloud Map service ARN"
  value       = aws_service_discovery_service.webapp.arn
}

output "ecs_service_name" {
  description = "ECS service name"
  value       = aws_ecs_service.webapp.name
}

output "log_group_name" {
  description = "CloudWatch log group name"
  value       = aws_cloudwatch_log_group.webapp.name
}
