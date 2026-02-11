output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "public_subnet_ids" {
  description = "Public subnet IDs"
  value       = aws_subnet.public[*].id
}

output "alb_security_group_id" {
  description = "ALB security group ID"
  value       = aws_security_group.alb.id
}

output "eb_security_group_id" {
  description = "Elastic Beanstalk security group ID"
  value       = aws_security_group.eb.id
}

output "efs_security_group_id" {
  description = "EFS security group ID"
  value       = aws_security_group.efs.id
}

output "fetcher_security_group_id" {
  description = "Fetcher security group ID"
  value       = aws_security_group.fetcher.id
}
