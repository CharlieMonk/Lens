variable "app_name" {
  description = "Application name"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for Fargate tasks"
  type        = list(string)
}

variable "fetcher_security_group" {
  description = "Security group ID for fetcher"
  type        = string
}

variable "efs_file_system_id" {
  description = "EFS filesystem ID"
  type        = string
}

variable "efs_access_point_id" {
  description = "EFS access point ID"
  type        = string
}

variable "fetcher_cpu" {
  description = "CPU units for fetcher task"
  type        = number
  default     = 2048
}

variable "fetcher_memory" {
  description = "Memory for fetcher task in MB"
  type        = number
  default     = 4096
}

variable "fetcher_schedule" {
  description = "EventBridge schedule expression"
  type        = string
  default     = "cron(0 6 ? * SUN *)"
}
