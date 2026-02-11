output "efs_file_system_id" {
  description = "EFS filesystem ID"
  value       = aws_efs_file_system.main.id
}

output "efs_access_point_id" {
  description = "EFS access point ID"
  value       = aws_efs_access_point.main.id
}

output "efs_dns_name" {
  description = "EFS DNS name"
  value       = aws_efs_file_system.main.dns_name
}
