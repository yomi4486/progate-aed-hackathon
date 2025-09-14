variable "name_prefix" { type = string }
variable "use_localstack" {
  type    = bool
  default = true
}

locals {
  # Use timestamp suffix to ensure unique bucket names
  timestamp_suffix = formatdate("YYYYMMDD-hhmm", timestamp())
  suffix = var.use_localstack ? "local" : "useast1-${local.timestamp_suffix}"
}

resource "aws_s3_bucket" "raw" {
  bucket = "${var.name_prefix}-raw-${local.suffix}"
  
  provider = aws
}

resource "aws_s3_bucket" "parsed" {
  bucket = "${var.name_prefix}-parsed-${local.suffix}"
  
  provider = aws
}

resource "aws_s3_bucket" "index_ready" {
  bucket = "${var.name_prefix}-index-ready-${local.suffix}"
  
  provider = aws
}

resource "aws_s3_bucket_versioning" "this" {
  for_each = {
    raw         = aws_s3_bucket.raw.id
    parsed      = aws_s3_bucket.parsed.id
    index_ready = aws_s3_bucket.index_ready.id
  }

  bucket = each.value
  versioning_configuration { status = "Enabled" }
}

output "raw_bucket" { value = aws_s3_bucket.raw.bucket }
output "parsed_bucket" { value = aws_s3_bucket.parsed.bucket }
output "index_ready_bucket" { value = aws_s3_bucket.index_ready.bucket }

output "raw_bucket_arn" { value = aws_s3_bucket.raw.arn }
output "parsed_bucket_arn" { value = aws_s3_bucket.parsed.arn }
output "index_ready_bucket_arn" { value = aws_s3_bucket.index_ready.arn }
