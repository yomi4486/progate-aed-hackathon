# Variables moved to main.tf to avoid duplication

provider "aws" {
  region                      = "us-east-1"  # Explicitly set region for debugging
  access_key                  = var.use_localstack ? "test" : null
  secret_key                  = var.use_localstack ? "test" : null
  skip_credentials_validation = var.use_localstack
  skip_requesting_account_id  = var.use_localstack
  s3_use_path_style           = var.use_localstack

  dynamic "endpoints" {
    for_each = var.use_localstack ? [1] : []
    content {
      s3         = var.localstack_endpoint
      sqs        = var.localstack_endpoint
      dynamodb   = var.localstack_endpoint
      iam        = var.localstack_endpoint
      sts        = var.localstack_endpoint
      kinesis    = var.localstack_endpoint
      es         = var.localstack_endpoint # legacy for opensearch 
      opensearch = var.localstack_endpoint
    }
  }
}
