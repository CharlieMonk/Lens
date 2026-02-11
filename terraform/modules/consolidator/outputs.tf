output "ecs_cluster_arn" {
  description = "ECS cluster ARN"
  value       = aws_ecs_cluster.main.arn
}

output "ecs_cluster_name" {
  description = "ECS cluster name"
  value       = aws_ecs_cluster.main.name
}

output "task_definition_arn" {
  description = "Consolidator task definition ARN"
  value       = aws_ecs_task_definition.consolidator.arn
}

output "ecr_repository_url" {
  description = "ECR repository URL for consolidator"
  value       = aws_ecr_repository.consolidator.repository_url
}

output "log_group_name" {
  description = "CloudWatch log group name"
  value       = aws_cloudwatch_log_group.consolidator.name
}
