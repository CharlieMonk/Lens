variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (e.g., prod, staging)"
  type        = string
  default     = "prod"
}

variable "app_name" {
  description = "Application name"
  type        = string
  default     = "ecfr"
}

variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  description = "Availability zones to use"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b"]
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "eb_instance_type" {
  description = "EC2 instance type for Elastic Beanstalk"
  type        = string
  default     = "t3.medium"
}

variable "eb_min_instances" {
  description = "Minimum number of EB instances"
  type        = number
  default     = 1
}

variable "eb_max_instances" {
  description = "Maximum number of EB instances"
  type        = number
  default     = 4
}

variable "fetcher_cpu" {
  description = "CPU units for fetcher task (1024 = 1 vCPU)"
  type        = number
  default     = 2048
}

variable "fetcher_memory" {
  description = "Memory for fetcher task in MB"
  type        = number
  default     = 4096
}

variable "fetcher_schedule" {
  description = "EventBridge schedule expression for fetcher"
  type        = string
  default     = "cron(0 6 ? * SUN *)"  # Weekly on Sunday at 6 AM UTC
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}
