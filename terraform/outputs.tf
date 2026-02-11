output "vpc_id" {
  description = "VPC ID"
  value       = module.network.vpc_id
}

output "public_subnet_ids" {
  description = "Public subnet IDs"
  value       = module.network.public_subnet_ids
}

output "efs_file_system_id" {
  description = "EFS filesystem ID"
  value       = module.storage.efs_file_system_id
}

output "efs_access_point_id" {
  description = "EFS access point ID"
  value       = module.storage.efs_access_point_id
}

output "ecr_repository_url" {
  description = "ECR repository URL for fetcher image"
  value       = module.fetcher.ecr_repository_url
}

output "ecs_cluster_name" {
  description = "ECS cluster name"
  value       = module.fetcher.ecs_cluster_name
}

output "fetcher_task_definition_arn" {
  description = "Fetcher task definition ARN"
  value       = module.fetcher.task_definition_arn
}

output "fetcher_security_group_id" {
  description = "Fetcher security group ID"
  value       = module.network.fetcher_security_group_id
}

output "eb_environment_url" {
  description = "Elastic Beanstalk environment URL"
  value       = module.elastic_beanstalk.environment_url
}

output "eb_environment_name" {
  description = "Elastic Beanstalk environment name"
  value       = module.elastic_beanstalk.environment_name
}
