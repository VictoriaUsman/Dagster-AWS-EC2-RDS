resource "aws_s3_bucket" "data" {
  bucket = var.s3_bucket_name

  tags = {
    Name = var.s3_bucket_name
  }
}
