output "s3_bucket_name" {
  description = "S3 bucket name for intermediate data"
  value       = aws_s3_bucket.data.id
}

output "s3_bucket_arn" {
  description = "S3 bucket ARN"
  value       = aws_s3_bucket.data.arn
}

output "efs_file_system_id" {
  description = "EFS filesystem ID"
  value       = aws_efs_file_system.main.id
}

output "efs_file_system_arn" {
  description = "EFS filesystem ARN"
  value       = aws_efs_file_system.main.arn
}

output "efs_access_point_id" {
  description = "EFS access point ID"
  value       = aws_efs_access_point.main.id
}

output "efs_access_point_arn" {
  description = "EFS access point ARN"
  value       = aws_efs_access_point.main.arn
}
