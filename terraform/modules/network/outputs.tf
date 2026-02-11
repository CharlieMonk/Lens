output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "public_subnet_ids" {
  description = "Public subnet IDs"
  value       = aws_subnet.public[*].id
}

output "vpc_link_security_group_id" {
  description = "VPC Link security group ID"
  value       = aws_security_group.vpc_link.id
}

output "webapp_security_group_id" {
  description = "Web app security group ID"
  value       = aws_security_group.webapp.id
}

output "consolidator_security_group_id" {
  description = "Consolidator security group ID"
  value       = aws_security_group.consolidator.id
}

output "efs_security_group_id" {
  description = "EFS security group ID"
  value       = aws_security_group.efs.id
}
