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
  description = "Subnet IDs for ECS tasks"
  type        = list(string)
}

variable "webapp_security_group" {
  description = "Security group ID for webapp"
  type        = string
}

variable "vpc_link_security_group" {
  description = "Security group ID for VPC Link"
  type        = string
}

variable "ecs_cluster_arn" {
  description = "ECS cluster ARN"
  type        = string
}

variable "ecs_cluster_name" {
  description = "ECS cluster name"
  type        = string
}

variable "efs_file_system_id" {
  description = "EFS filesystem ID"
  type        = string
}

variable "efs_file_system_arn" {
  description = "EFS filesystem ARN"
  type        = string
}

variable "efs_access_point_id" {
  description = "EFS access point ID"
  type        = string
}

variable "cpu" {
  description = "CPU units (512 = 0.5 vCPU)"
  type        = number
  default     = 512
}

variable "memory" {
  description = "Memory in MB"
  type        = number
  default     = 1024
}

variable "desired_count" {
  description = "Desired number of tasks"
  type        = number
  default     = 1
}

variable "min_count" {
  description = "Minimum number of tasks"
  type        = number
  default     = 1
}

variable "max_count" {
  description = "Maximum number of tasks"
  type        = number
  default     = 4
}
