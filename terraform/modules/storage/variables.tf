variable "app_name" {
  description = "Application name"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "aws_account_id" {
  description = "AWS account ID for unique S3 bucket naming"
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for EFS mount targets"
  type        = list(string)
}

variable "efs_security_group" {
  description = "Security group ID for EFS"
  type        = string
}
