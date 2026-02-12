variable "app_name" {
  description = "Application name"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "fetcher_lambda_arn" {
  description = "ARN of the fetcher Lambda function"
  type        = string
}

variable "ecs_cluster_arn" {
  description = "ARN of the ECS cluster for consolidator"
  type        = string
}

variable "consolidator_task_arn" {
  description = "ARN of the consolidator task definition"
  type        = string
}

variable "consolidator_security_group" {
  description = "Security group ID for consolidator"
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for consolidator task"
  type        = list(string)
}

variable "schedule_expression" {
  description = "EventBridge schedule expression"
  type        = string
  default     = "cron(0 6 ? * SUN *)"
}

variable "max_concurrency" {
  description = "Max concurrent Lambda invocations in Map state (should match Lambda concurrency limit)"
  type        = number
  default     = 10  # Matches typical Lambda concurrent execution limit
}

variable "historical_years" {
  description = "List of historical years to fetch"
  type        = list(number)
  default     = [2025, 2020, 2015, 2010, 2005, 2000]
}
