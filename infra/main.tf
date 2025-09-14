locals {
  project           = "aedhack"
  env              = var.use_localstack ? "devlocal" : (var.env != null ? var.env : "dev")
  timestamp_suffix = formatdate("YYYY-MM-DD-hhmm", timestamp())
}

variable "env" {
  description = "Environment name (dev/stg/prod)"
  type        = string
  default     = null
}

variable "use_localstack" {
  description = "Whether to use LocalStack for local development"
  type        = bool
  default     = false
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "localstack_endpoint" {
  description = "LocalStack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

module "network" {
  source         = "./modules/network"
  name           = "${local.project}-${local.env}-${local.timestamp_suffix}"
  use_localstack = var.use_localstack
}

module "storage" {
  source         = "./modules/storage"
  name_prefix    = "${local.project}-${local.env}-${local.timestamp_suffix}"
  use_localstack = var.use_localstack
}

module "queue" {
  source      = "./modules/queue"
  name_prefix = "${local.project}-${local.env}-${local.timestamp_suffix}"
}

module "ddb" {
  source     = "./modules/ddb"
  table_name = "${local.project}-${local.env}-url-states-${local.timestamp_suffix}"
}

module "opensearch" {
  count         = var.use_localstack ? 0 : 1
  source        = "./modules/opensearch"
  domain_name   = "${local.project}-${local.env}-search-${local.timestamp_suffix}"
  environment   = local.env
  vpc_id        = module.network.vpc_id
  subnet_ids    = [module.network.private_subnet_ids[0]]  # Single subnet for single-node deployment
  
  # Allow access from VPC and specific CIDR blocks
  allowed_cidr_blocks = []
  
  use_localstack = var.use_localstack
}

# EKS cluster for crawler workloads (simplified without KMS encryption)
module "eks" {
  count        = var.use_localstack ? 0 : 1
  source       = "./modules/eks"
  cluster_name = "${local.project}-${local.env}-cluster-${local.timestamp_suffix}"
  environment  = local.env
  project      = local.project

  # Network configuration
  vpc_id             = module.network.vpc_id
  subnet_ids         = module.network.all_subnet_ids
  private_subnet_ids = module.network.private_subnet_ids
  allowed_cidrs      = ["0.0.0.0/0"]

  # Node configuration - using smaller instances for testing
  node_instance_types   = ["t3.small", "t3.medium"]
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
  enable_ssh_access         = false
  use_self_managed_cni      = true  # Using self-managed CNI due to managed addon issues
}

# Essential outputs
output "vpc_id" { value = module.network.vpc_id }
output "s3_raw_bucket" { value = module.storage.raw_bucket }
output "s3_parsed_bucket" { value = module.storage.parsed_bucket }
output "s3_index_ready_bucket" { value = module.storage.index_ready_bucket }
output "sqs_url_queue" { value = module.queue.url_queue_url }
output "sqs_discovery_queue" { value = module.queue.discovery_queue_url }
output "sqs_index_queue" { value = module.queue.index_queue_url }
output "ddb_table" { value = module.ddb.table_name }

# EKS outputs
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

output "eks_indexer_service_account_role_arn" {
  value = var.use_localstack ? null : module.eks[0].indexer_service_account_role_arn
}

# OpenSearch outputs
output "opensearch_endpoint" {
  value = var.use_localstack ? null : module.opensearch[0].endpoint
}

output "opensearch_domain_arn" {
  value = var.use_localstack ? null : module.opensearch[0].domain_arn
}

output "opensearch_kibana_endpoint" {
  value = var.use_localstack ? null : module.opensearch[0].kibana_endpoint
}

output "opensearch_admin_password" {
  value     = var.use_localstack ? null : module.opensearch[0].admin_password
  sensitive = true
}
