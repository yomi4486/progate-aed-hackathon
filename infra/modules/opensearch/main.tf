variable "domain_name" { type = string }
variable "use_localstack" {
  type    = bool
  default = true
}

locals {
  engine_version = "OpenSearch_2.11"
}

resource "aws_opensearch_domain" "this" {
  domain_name    = var.domain_name
  engine_version = local.engine_version

  cluster_config {
    instance_type  = var.use_localstack ? "t3.small.search" : "t3.small.search"
    instance_count = 1
  }

  ebs_options {
    ebs_enabled = true
    volume_size = 10
    volume_type = "gp3"
  }

  advanced_options = {
    "override_main_response_version"         = "true"
    "rest.action.multi.allow_explicit_index" = "true"
  }

  advanced_security_options {
    enabled                        = false
    internal_user_database_enabled = false
  }

  node_to_node_encryption {
    enabled = false
  }

  encrypt_at_rest {
    enabled = false
  }

  domain_endpoint_options {
    enforce_https       = false
    tls_security_policy = "Policy-Min-TLS-1-0-2019-07"
  }

  tags = { Name = var.domain_name }
}

output "endpoint" { value = aws_opensearch_domain.this.endpoint }
