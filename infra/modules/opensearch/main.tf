variable "domain_name" { type = string }
variable "use_localstack" {
  type    = bool
  default = false
}
variable "vpc_id" {
  type        = string
  description = "VPC ID for OpenSearch domain"
}

# Get VPC CIDR block dynamically
data "aws_vpc" "selected" {
  id = var.vpc_id
}
variable "subnet_ids" {
  type        = list(string)
  description = "Subnet IDs for OpenSearch domain (private subnets)"
}
variable "environment" {
  type        = string
  description = "Environment name (dev/staging/prod)"
}
variable "allowed_cidr_blocks" {
  type        = list(string)
  default     = []
  description = "CIDR blocks allowed to access OpenSearch"
}

locals {
  engine_version = "OpenSearch_2.11"
  domain_name    = var.domain_name

  # Production-grade configuration based on environment
  is_production = var.environment == "prod"
  
  instance_config = var.use_localstack ? {
    instance_type  = "t3.micro.search"
    instance_count = 1
    dedicated_master_enabled = false
  } : local.is_production ? {
    instance_type  = "r6g.large.search"  # Better for vector search workloads
    instance_count = 3                   # Multi-AZ for HA
    dedicated_master_enabled = true
    master_instance_type = "r6g.medium.search"
    master_instance_count = 3
  } : {
    instance_type  = "t3.small.search"
    instance_count = 1
    dedicated_master_enabled = false
  }

  ebs_config = var.use_localstack ? {
    volume_size = 10
    volume_type = "gp2"
    iops = null
  } : local.is_production ? {
    volume_size = 100
    volume_type = "gp3"
    iops = 3000
    throughput = 125
  } : {
    volume_size = 20
    volume_type = "gp3"
    iops = null
    throughput = null
  }
}

# Security group for OpenSearch
resource "aws_security_group" "opensearch" {
  count       = var.use_localstack ? 0 : 1
  name        = "${local.domain_name}-opensearch-sg"
  description = "Security group for OpenSearch domain"
  vpc_id      = var.vpc_id

  # HTTPS access from VPC
  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.selected.cidr_block]  # VPC CIDR
  }

  # Additional CIDR blocks if specified
  dynamic "ingress" {
    for_each = var.allowed_cidr_blocks
    content {
      from_port   = 443
      to_port     = 443
      protocol    = "tcp"
      cidr_blocks = [ingress.value]
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${local.domain_name}-opensearch-sg"
    Environment = var.environment
  }
}

# IAM service-linked role for OpenSearch
data "aws_iam_role" "opensearch_service_role" {
  count = var.use_localstack ? 0 : 1
  name  = "AWSServiceRoleForAmazonOpenSearchService"
}

# Create service-linked role if it doesn't exist
resource "aws_iam_service_linked_role" "opensearch" {
  count            = var.use_localstack ? 0 : (try(data.aws_iam_role.opensearch_service_role[0].id, null) == null ? 1 : 0)
  aws_service_name = "opensearchservice.amazonaws.com"
  description      = "Service-linked role for Amazon OpenSearch Service"
}

# Random password for OpenSearch admin user
resource "random_password" "opensearch_admin" {
  count   = var.use_localstack ? 0 : 1
  length  = 16
  special = true
}

resource "aws_opensearch_domain" "this" {
  domain_name    = local.domain_name
  engine_version = local.engine_version

  cluster_config {
    instance_type            = local.instance_config.instance_type
    instance_count           = local.instance_config.instance_count
    dedicated_master_enabled = local.instance_config.dedicated_master_enabled
    dedicated_master_type    = local.instance_config.dedicated_master_enabled ? local.instance_config.master_instance_type : null
    dedicated_master_count   = local.instance_config.dedicated_master_enabled ? local.instance_config.master_instance_count : null

    zone_awareness_enabled = local.instance_config.instance_count > 1
    
    dynamic "zone_awareness_config" {
      for_each = local.instance_config.instance_count > 1 ? [1] : []
      content {
        availability_zone_count = min(local.instance_config.instance_count, 3)
      }
    }
  }

  ebs_options {
    ebs_enabled = true
    volume_size = local.ebs_config.volume_size
    volume_type = local.ebs_config.volume_type
    iops        = local.ebs_config.iops
    throughput  = local.ebs_config.throughput
  }

  # VPC configuration for non-LocalStack deployments
  dynamic "vpc_options" {
    for_each = var.use_localstack ? [] : [1]
    content {
      security_group_ids = [aws_security_group.opensearch[0].id]
      subnet_ids         = var.subnet_ids
    }
  }

  advanced_options = {
    "override_main_response_version"                = "true"
    "rest.action.multi.allow_explicit_index"        = "true"
    "indices.query.bool.max_clause_count"           = "1024"
  }

  advanced_security_options {
    enabled                        = var.use_localstack ? false : true
    internal_user_database_enabled = var.use_localstack ? false : true
    
    dynamic "master_user_options" {
      for_each = var.use_localstack ? [] : [1]
      content {
        master_user_name     = "admin"
        master_user_password = random_password.opensearch_admin[0].result
      }
    }
  }

  node_to_node_encryption {
    enabled = var.use_localstack ? false : true
  }

  encrypt_at_rest {
    enabled = var.use_localstack ? false : true
  }

  domain_endpoint_options {
    enforce_https       = var.use_localstack ? false : true
    tls_security_policy = var.use_localstack ? "Policy-Min-TLS-1-0-2019-07" : "Policy-Min-TLS-1-2-2019-07"
  }

  log_publishing_options {
    cloudwatch_log_group_arn = var.use_localstack ? null : aws_cloudwatch_log_group.opensearch_logs[0].arn
    log_type                 = "INDEX_SLOW_LOGS"
    enabled                  = var.use_localstack ? false : true
  }

  log_publishing_options {
    cloudwatch_log_group_arn = var.use_localstack ? null : aws_cloudwatch_log_group.opensearch_logs[0].arn
    log_type                 = "SEARCH_SLOW_LOGS" 
    enabled                  = var.use_localstack ? false : true
  }

  log_publishing_options {
    cloudwatch_log_group_arn = var.use_localstack ? null : aws_cloudwatch_log_group.opensearch_logs[0].arn
    log_type                 = "ES_APPLICATION_LOGS"
    enabled                  = var.use_localstack ? false : true
  }

  snapshot_options {
    automated_snapshot_start_hour = 3  # 3 AM UTC
  }

  access_policies = var.use_localstack ? null : data.aws_iam_policy_document.opensearch_access_policy[0].json

  tags = {
    Name        = local.domain_name
    Environment = var.environment
    Project     = "aedhack"
    Component   = "search"
  }

  depends_on = [
    aws_iam_service_linked_role.opensearch
  ]
}

# IAM policy document for OpenSearch domain access
data "aws_iam_policy_document" "opensearch_access_policy" {
  count = var.use_localstack ? 0 : 1
  
  statement {
    effect = "Allow"
    
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    
    actions   = ["es:*"]
    resources = ["arn:aws:es:*:*:domain/${local.domain_name}/*"]
    
    condition {
      test     = "IpAddress"
      variable = "aws:sourceIp"
      values   = [data.aws_vpc.selected.cidr_block]  # VPC CIDR
    }
  }
}

# CloudWatch log group for OpenSearch logs
resource "aws_cloudwatch_log_group" "opensearch_logs" {
  count             = var.use_localstack ? 0 : 1
  name              = "/aws/opensearch/domains/${local.domain_name}"
  retention_in_days = 30

  tags = {
    Environment = var.environment
    Project     = "aedhack"
  }
}

# IAM role for OpenSearch access
resource "aws_iam_role" "opensearch_access" {
  count = var.use_localstack ? 0 : 1
  name  = "${local.domain_name}-opensearch-access-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "opensearch.amazonaws.com"
      }
    }]
  })

  tags = {
    Environment = var.environment
    Project     = "aedhack"
  }
}

output "endpoint" { 
  value = aws_opensearch_domain.this.endpoint 
}

output "domain_arn" {
  value = aws_opensearch_domain.this.arn
}

output "domain_id" {
  value = aws_opensearch_domain.this.domain_id
}

output "kibana_endpoint" {
  value = aws_opensearch_domain.this.dashboard_endpoint
}

output "security_group_id" {
  value = var.use_localstack ? null : aws_security_group.opensearch[0].id
}

output "admin_password" {
  value     = var.use_localstack ? null : random_password.opensearch_admin[0].result
  sensitive = true
}
