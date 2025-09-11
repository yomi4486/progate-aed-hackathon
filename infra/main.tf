locals {
  project = "aedhack"
  env     = var.use_localstack ? "devlocal" : (var.env != null ? var.env : "dev")
}

variable "env" {
  description = "Environment name (dev/stg/prod)"
  type        = string
  default     = null
}

variable "use_localstack" {
  description = "Whether to use LocalStack for local development"
  type        = bool
  default     = true
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "ap-northeast-1"
}

variable "localstack_endpoint" {
  description = "LocalStack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

module "network" {
  source         = "./modules/network"
  name           = "${local.project}-${local.env}"
  use_localstack = var.use_localstack
}

module "storage" {
  source         = "./modules/storage"
  name_prefix    = "${local.project}-${local.env}"
  use_localstack = var.use_localstack
}

module "queue" {
  source      = "./modules/queue"
  name_prefix = "${local.project}-${local.env}"
}

module "ddb" {
  source     = "./modules/ddb"
  table_name = "${local.project}-${local.env}-url-states"
}

module "opensearch" {
  source         = "./modules/opensearch"
  domain_name    = "${local.project}-${local.env}"
  use_localstack = var.use_localstack
}

# EKS cluster for crawler workloads (only in non-LocalStack environments)
module "eks" {
  count        = var.use_localstack ? 0 : 1
  source       = "./modules/eks"
  cluster_name = "${local.project}-${local.env}-cluster"
  environment  = local.env
  project      = local.project

  # Network configuration
  vpc_id             = module.network.vpc_id
  subnet_ids         = module.network.all_subnet_ids
  private_subnet_ids = module.network.private_subnet_ids
  allowed_cidrs      = ["0.0.0.0/0"] # Restrict this in production

  # Node configuration
  node_instance_types   = ["t3.medium", "t3.large"]
  node_desired_capacity = 2
  node_min_capacity     = 1
  node_max_capacity     = 10
  capacity_type         = "ON_DEMAND"

  # AWS resource ARNs for IRSA
  s3_raw_bucket_arn         = module.storage.raw_bucket_arn
  s3_parsed_bucket_arn      = module.storage.parsed_bucket_arn
  s3_index_ready_bucket_arn = module.storage.index_ready_bucket_arn
  sqs_crawl_queue_arn       = module.queue.url_queue_arn
  sqs_discovery_queue_arn   = module.queue.discovery_queue_arn
  sqs_index_queue_arn       = module.queue.index_queue_arn
  dynamodb_table_arn        = module.ddb.table_arn

  # Feature flags
  enable_cluster_autoscaler = true
  enable_dedicated_nodes    = false
  enable_ssh_access         = true
}

output "vpc_id" { value = module.network.vpc_id }
output "s3_raw_bucket" { value = module.storage.raw_bucket }
output "s3_parsed_bucket" { value = module.storage.parsed_bucket }
output "s3_index_ready_bucket" { value = module.storage.index_ready_bucket }
output "sqs_url_queue" { value = module.queue.url_queue_url }
output "sqs_discovery_queue" { value = module.queue.discovery_queue_url }
output "sqs_index_queue" { value = module.queue.index_queue_url }
output "ddb_table" { value = module.ddb.table_name }
output "opensearch_endpoint" { value = module.opensearch.endpoint }

# EKS outputs (only when EKS is deployed)
output "eks_cluster_id" {
  value = var.use_localstack ? null : module.eks[0].cluster_id
}

output "eks_cluster_endpoint" {
  value = var.use_localstack ? null : module.eks[0].cluster_endpoint
}

output "eks_cluster_name" {
  value = var.use_localstack ? null : module.eks[0].cluster_name
}

output "eks_crawler_service_account_role_arn" {
  value = var.use_localstack ? null : module.eks[0].crawler_service_account_role_arn
}

output "eks_cluster_certificate_authority_data" {
  value     = var.use_localstack ? null : module.eks[0].cluster_certificate_authority_data
  sensitive = true
}

# Convenience outputs for LocalStack-only: normalize URLs to plain localhost:4566
output "sqs_url_queue_local" {
  value       = replace(module.queue.url_queue_url, "http://sqs.${var.aws_region}.localhost.localstack.cloud:4566", "http://localhost:4566")
  description = "SQS URL (localhost form)"
}

output "sqs_index_queue_local" {
  value       = replace(module.queue.index_queue_url, "http://sqs.${var.aws_region}.localhost.localstack.cloud:4566", "http://localhost:4566")
  description = "SQS Index URL (localhost form)"
}

output "localstack_edge_endpoint" {
  value       = var.localstack_endpoint
  description = "LocalStack edge endpoint (http://localhost:4566)"
}
