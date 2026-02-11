provider "aws" {
  region = var.aws_region

  default_tags {
    tags = merge(var.tags, {
      Application = var.app_name
      Environment = var.environment
      ManagedBy   = "terraform"
    })
  }
}

module "network" {
  source = "./modules/network"

  app_name            = var.app_name
  environment         = var.environment
  vpc_cidr            = var.vpc_cidr
  availability_zones  = var.availability_zones
  public_subnet_cidrs = var.public_subnet_cidrs
}

module "storage" {
  source = "./modules/storage"

  app_name           = var.app_name
  environment        = var.environment
  vpc_id             = module.network.vpc_id
  subnet_ids         = module.network.public_subnet_ids
  efs_security_group = module.network.efs_security_group_id
}

module "fetcher" {
  source = "./modules/fetcher"

  app_name              = var.app_name
  environment           = var.environment
  aws_region            = var.aws_region
  vpc_id                = module.network.vpc_id
  subnet_ids            = module.network.public_subnet_ids
  fetcher_security_group = module.network.fetcher_security_group_id
  efs_file_system_id    = module.storage.efs_file_system_id
  efs_access_point_id   = module.storage.efs_access_point_id
  fetcher_cpu           = var.fetcher_cpu
  fetcher_memory        = var.fetcher_memory
  fetcher_schedule      = var.fetcher_schedule
}

module "elastic_beanstalk" {
  source = "./modules/elastic_beanstalk"

  app_name              = var.app_name
  environment           = var.environment
  vpc_id                = module.network.vpc_id
  subnet_ids            = module.network.public_subnet_ids
  alb_security_group    = module.network.alb_security_group_id
  eb_security_group     = module.network.eb_security_group_id
  efs_file_system_id    = module.storage.efs_file_system_id
  efs_access_point_id   = module.storage.efs_access_point_id
  instance_type         = var.eb_instance_type
  min_instances         = var.eb_min_instances
  max_instances         = var.eb_max_instances
}
