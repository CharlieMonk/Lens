variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "app_name" {
  description = "Application name"
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
}

variable "availability_zones" {
  description = "Availability zones to use"
  type        = list(string)
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets"
  type        = list(string)
}

variable "eb_instance_type" {
  description = "EC2 instance type for Elastic Beanstalk"
  type        = string
}

variable "eb_min_instances" {
  description = "Minimum number of EB instances"
  type        = number
}

variable "eb_max_instances" {
  description = "Maximum number of EB instances"
  type        = number
}

variable "fetcher_cpu" {
  description = "CPU units for fetcher task"
  type        = number
}

variable "fetcher_memory" {
  description = "Memory for fetcher task in MB"
  type        = number
}

variable "fetcher_schedule" {
  description = "EventBridge schedule expression for fetcher"
  type        = string
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}
