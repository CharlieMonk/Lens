variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
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
  description = "Availability zones"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b"]
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "fetcher_lambda_zip_path" {
  description = "Path to fetcher Lambda deployment package"
  type        = string
  default     = "../../../lambda/fetcher.zip"
}

variable "consolidator_cpu" {
  description = "CPU units for consolidator (1024 = 1 vCPU)"
  type        = number
  default     = 2048
}

variable "consolidator_memory" {
  description = "Memory for consolidator in MB"
  type        = number
  default     = 8192
}

variable "webapp_cpu" {
  description = "CPU units for webapp (512 = 0.5 vCPU)"
  type        = number
  default     = 512
}

variable "webapp_memory" {
  description = "Memory for webapp in MB"
  type        = number
  default     = 1024
}

variable "webapp_desired_count" {
  description = "Desired number of webapp tasks"
  type        = number
  default     = 1
}

variable "webapp_min_count" {
  description = "Minimum number of webapp tasks"
  type        = number
  default     = 1
}

variable "webapp_max_count" {
  description = "Maximum number of webapp tasks"
  type        = number
  default     = 4
}

variable "schedule_expression" {
  description = "EventBridge schedule for weekly fetch"
  type        = string
  default     = "cron(0 6 ? * SUN *)"
}

variable "fetcher_max_concurrency" {
  description = "Max concurrent Lambda invocations"
  type        = number
  default     = 25
}

variable "historical_years" {
  description = "Historical years to fetch"
  type        = list(number)
  default     = [2025, 2020, 2015, 2010, 2005, 2000]
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default = {
    Project = "eCFR"
  }
}
