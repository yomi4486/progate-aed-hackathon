variable "name" { type = string }
variable "cidr_block" {
  type    = string
  default = "10.10.0.0/16"
}
variable "use_localstack" {
  type    = bool
  default = true
}

locals {
  create_real_network = var.use_localstack ? false : true
}

resource "aws_vpc" "this" {
  count                = local.create_real_network ? 1 : 0
  cidr_block           = var.cidr_block
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = "${var.name}-vpc" }
}

output "vpc_id" {
  value       = local.create_real_network ? aws_vpc.this[0].id : "vpc-localstack"
  description = "VPC id or placeholder when localstack"
}
