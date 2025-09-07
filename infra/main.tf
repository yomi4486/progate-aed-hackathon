locals {
  project = "aedhack"
  env     = var.use_localstack ? "devlocal" : (var.env != null ? var.env : "dev")
}

variable "env" {
  description = "Environment name (dev/stg/prod)"
  type        = string
  default     = null
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

output "vpc_id" { value = module.network.vpc_id }
output "s3_raw_bucket" { value = module.storage.raw_bucket }
output "s3_parsed_bucket" { value = module.storage.parsed_bucket }
output "s3_index_ready_bucket" { value = module.storage.index_ready_bucket }
output "sqs_url_queue" { value = module.queue.url_queue_url }
output "sqs_index_queue" { value = module.queue.index_queue_url }
output "ddb_table" { value = module.ddb.table_name }
output "opensearch_endpoint" { value = module.opensearch.endpoint }

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
