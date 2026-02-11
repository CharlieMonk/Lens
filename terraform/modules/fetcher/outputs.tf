output "ecr_repository_url" {
  description = "ECR repository URL"
  value       = aws_ecr_repository.fetcher.repository_url
}

output "ecs_cluster_name" {
  description = "ECS cluster name"
  value       = aws_ecs_cluster.fetcher.name
}

output "ecs_cluster_arn" {
  description = "ECS cluster ARN"
  value       = aws_ecs_cluster.fetcher.arn
}

output "task_definition_arn" {
  description = "Task definition ARN"
  value       = aws_ecs_task_definition.fetcher.arn
}

output "log_group_name" {
  description = "CloudWatch log group name"
  value       = aws_cloudwatch_log_group.fetcher.name
}

output "schedule_rule_name" {
  description = "EventBridge schedule rule name"
  value       = aws_cloudwatch_event_rule.fetcher.name
}
