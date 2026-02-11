module "ecfr" {
  source = "../../"

  aws_region          = var.aws_region
  environment         = var.environment
  app_name            = var.app_name
  vpc_cidr            = var.vpc_cidr
  availability_zones  = var.availability_zones
  public_subnet_cidrs = var.public_subnet_cidrs
  eb_instance_type    = var.eb_instance_type
  eb_min_instances    = var.eb_min_instances
  eb_max_instances    = var.eb_max_instances
  fetcher_cpu         = var.fetcher_cpu
  fetcher_memory      = var.fetcher_memory
  fetcher_schedule    = var.fetcher_schedule
  tags                = var.tags
}

output "vpc_id" {
  value = module.ecfr.vpc_id
}

output "public_subnet_ids" {
  value = module.ecfr.public_subnet_ids
}

output "efs_file_system_id" {
  value = module.ecfr.efs_file_system_id
}

output "efs_access_point_id" {
  value = module.ecfr.efs_access_point_id
}

output "ecr_repository_url" {
  value = module.ecfr.ecr_repository_url
}

output "ecs_cluster_name" {
  value = module.ecfr.ecs_cluster_name
}

output "fetcher_task_definition_arn" {
  value = module.ecfr.fetcher_task_definition_arn
}

output "fetcher_security_group_id" {
  value = module.ecfr.fetcher_security_group_id
}

output "eb_environment_url" {
  value = module.ecfr.eb_environment_url
}

output "eb_environment_name" {
  value = module.ecfr.eb_environment_name
}
