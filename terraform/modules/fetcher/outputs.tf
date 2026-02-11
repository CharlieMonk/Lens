output "lambda_function_arn" {
  description = "Lambda function ARN"
  value       = aws_lambda_function.fetcher.arn
}

output "lambda_function_name" {
  description = "Lambda function name"
  value       = aws_lambda_function.fetcher.function_name
}

output "lambda_invoke_arn" {
  description = "Lambda invoke ARN for Step Functions"
  value       = aws_lambda_function.fetcher.invoke_arn
}

output "log_group_name" {
  description = "CloudWatch log group name"
  value       = aws_cloudwatch_log_group.fetcher.name
}
