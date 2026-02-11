terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = var.tags
  }
}

data "aws_caller_identity" "current" {}

# Network Module
module "network" {
  source = "../../modules/network"

  app_name            = var.app_name
  environment         = var.environment
  vpc_cidr            = var.vpc_cidr
  availability_zones  = var.availability_zones
  public_subnet_cidrs = var.public_subnet_cidrs
}

# Storage Module
module "storage" {
  source = "../../modules/storage"

  app_name           = var.app_name
  environment        = var.environment
  aws_account_id     = data.aws_caller_identity.current.account_id
  subnet_ids         = module.network.public_subnet_ids
  efs_security_group = module.network.efs_security_group_id
}

# Fetcher Lambda Module
module "fetcher" {
  source = "../../modules/fetcher"

  app_name        = var.app_name
  environment     = var.environment
  s3_bucket_name  = module.storage.s3_bucket_name
  s3_bucket_arn   = module.storage.s3_bucket_arn
  lambda_zip_path = var.fetcher_lambda_zip_path
}

# Consolidator Module
module "consolidator" {
  source = "../../modules/consolidator"

  app_name            = var.app_name
  environment         = var.environment
  aws_region          = var.aws_region
  s3_bucket_name      = module.storage.s3_bucket_name
  s3_bucket_arn       = module.storage.s3_bucket_arn
  efs_file_system_id  = module.storage.efs_file_system_id
  efs_file_system_arn = module.storage.efs_file_system_arn
  efs_access_point_id = module.storage.efs_access_point_id
  cpu                 = var.consolidator_cpu
  memory              = var.consolidator_memory
}

# Orchestrator Module
module "orchestrator" {
  source = "../../modules/orchestrator"

  app_name                    = var.app_name
  environment                 = var.environment
  fetcher_lambda_arn          = module.fetcher.lambda_function_arn
  ecs_cluster_arn             = module.consolidator.ecs_cluster_arn
  consolidator_task_arn       = module.consolidator.task_definition_arn
  consolidator_security_group = module.network.consolidator_security_group_id
  subnet_ids                  = module.network.public_subnet_ids
  schedule_expression         = var.schedule_expression
  max_concurrency             = var.fetcher_max_concurrency
  historical_years            = var.historical_years
}

# Web App Module
module "webapp" {
  source = "../../modules/webapp"

  app_name                = var.app_name
  environment             = var.environment
  aws_region              = var.aws_region
  vpc_id                  = module.network.vpc_id
  subnet_ids              = module.network.public_subnet_ids
  webapp_security_group   = module.network.webapp_security_group_id
  vpc_link_security_group = module.network.vpc_link_security_group_id
  ecs_cluster_arn         = module.consolidator.ecs_cluster_arn
  ecs_cluster_name        = module.consolidator.ecs_cluster_name
  efs_file_system_id      = module.storage.efs_file_system_id
  efs_file_system_arn     = module.storage.efs_file_system_arn
  efs_access_point_id     = module.storage.efs_access_point_id
  cpu                     = var.webapp_cpu
  memory                  = var.webapp_memory
  desired_count           = var.webapp_desired_count
  min_count               = var.webapp_min_count
  max_count               = var.webapp_max_count
}

# Outputs
output "api_gateway_url" {
  description = "API Gateway URL"
  value       = module.webapp.api_gateway_url
}

output "webapp_ecr_repository_url" {
  description = "ECR repository URL for webapp"
  value       = module.webapp.ecr_repository_url
}

output "consolidator_ecr_repository_url" {
  description = "ECR repository URL for consolidator"
  value       = module.consolidator.ecr_repository_url
}

output "step_functions_arn" {
  description = "Step Functions state machine ARN"
  value       = module.orchestrator.state_machine_arn
}

output "s3_bucket_name" {
  description = "S3 bucket for intermediate data"
  value       = module.storage.s3_bucket_name
}

output "efs_file_system_id" {
  description = "EFS filesystem ID"
  value       = module.storage.efs_file_system_id
}
