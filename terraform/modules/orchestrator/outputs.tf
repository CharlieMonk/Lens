output "state_machine_arn" {
  description = "Step Functions state machine ARN"
  value       = aws_sfn_state_machine.main.arn
}

output "state_machine_name" {
  description = "Step Functions state machine name"
  value       = aws_sfn_state_machine.main.name
}

output "schedule_rule_name" {
  description = "EventBridge schedule rule name"
  value       = aws_cloudwatch_event_rule.weekly.name
}
