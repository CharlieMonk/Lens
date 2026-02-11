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

variable "s3_bucket_name" {
  description = "S3 bucket name for reading fetched data"
  type        = string
}

variable "s3_bucket_arn" {
  description = "S3 bucket ARN"
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
  description = "CPU units (1024 = 1 vCPU)"
  type        = number
  default     = 2048
}

variable "memory" {
  description = "Memory in MB"
  type        = number
  default     = 8192
}
