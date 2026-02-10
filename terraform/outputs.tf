output "ec2_public_ip" {
  description = "Public IP of the EC2 pipeline instance"
  value       = aws_instance.pipeline.public_ip
}

output "rds_endpoint" {
  description = "RDS PostgreSQL endpoint"
  value       = aws_db_instance.pipeline.endpoint
}

output "s3_bucket_name" {
  description = "S3 bucket name"
  value       = aws_s3_bucket.data.bucket
}
