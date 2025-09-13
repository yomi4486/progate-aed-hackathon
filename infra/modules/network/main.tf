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

# Data source for availability zones
data "aws_availability_zones" "available" {
  count = local.create_real_network ? 1 : 0
  state = "available"
}

# Public subnets for EKS control plane and NAT gateways
resource "aws_subnet" "public" {
  count                   = local.create_real_network ? 2 : 0
  vpc_id                  = aws_vpc.this[0].id
  cidr_block              = cidrsubnet(var.cidr_block, 4, count.index)
  availability_zone       = data.aws_availability_zones.available[0].names[count.index]
  map_public_ip_on_launch = true

  tags = {
    Name                                        = "${var.name}-public-${count.index + 1}"
    "kubernetes.io/cluster/${var.name}-cluster" = "shared"
    "kubernetes.io/role/elb"                    = "1"
  }
}

# Private subnets for EKS worker nodes
resource "aws_subnet" "private" {
  count             = local.create_real_network ? 2 : 0
  vpc_id            = aws_vpc.this[0].id
  cidr_block        = cidrsubnet(var.cidr_block, 4, count.index + 2)
  availability_zone = data.aws_availability_zones.available[0].names[count.index]

  tags = {
    Name                                        = "${var.name}-private-${count.index + 1}"
    "kubernetes.io/cluster/${var.name}-cluster" = "owned"
    "kubernetes.io/role/internal-elb"           = "1"
  }
}

# Internet Gateway
resource "aws_internet_gateway" "this" {
  count  = local.create_real_network ? 1 : 0
  vpc_id = aws_vpc.this[0].id

  tags = {
    Name = "${var.name}-igw"
  }
}

# Elastic IPs for NAT Gateways
resource "aws_eip" "nat" {
  count  = local.create_real_network ? 2 : 0
  domain = "vpc"

  depends_on = [aws_internet_gateway.this]

  tags = {
    Name = "${var.name}-nat-eip-${count.index + 1}"
  }
}

# NAT Gateways
resource "aws_nat_gateway" "this" {
  count         = local.create_real_network ? 2 : 0
  allocation_id = aws_eip.nat[count.index].id
  subnet_id     = aws_subnet.public[count.index].id

  depends_on = [aws_internet_gateway.this]

  tags = {
    Name = "${var.name}-nat-${count.index + 1}"
  }
}

# Route table for public subnets
resource "aws_route_table" "public" {
  count  = local.create_real_network ? 1 : 0
  vpc_id = aws_vpc.this[0].id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this[0].id
  }

  tags = {
    Name = "${var.name}-public-rt"
  }
}

# Route table associations for public subnets
resource "aws_route_table_association" "public" {
  count          = local.create_real_network ? 2 : 0
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public[0].id
}

# Route tables for private subnets (one per AZ)
resource "aws_route_table" "private" {
  count  = local.create_real_network ? 2 : 0
  vpc_id = aws_vpc.this[0].id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.this[count.index].id
  }

  tags = {
    Name = "${var.name}-private-rt-${count.index + 1}"
  }
}

# Route table associations for private subnets
resource "aws_route_table_association" "private" {
  count          = local.create_real_network ? 2 : 0
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[count.index].id
}

output "vpc_id" {
  value       = local.create_real_network ? aws_vpc.this[0].id : "vpc-localstack"
  description = "VPC id or placeholder when localstack"
}

output "public_subnet_ids" {
  value       = local.create_real_network ? aws_subnet.public[*].id : ["subnet-localstack-public-1", "subnet-localstack-public-2"]
  description = "List of public subnet IDs"
}

output "private_subnet_ids" {
  value       = local.create_real_network ? aws_subnet.private[*].id : ["subnet-localstack-private-1", "subnet-localstack-private-2"]
  description = "List of private subnet IDs"
}

output "all_subnet_ids" {
  value       = local.create_real_network ? concat(aws_subnet.public[*].id, aws_subnet.private[*].id) : ["subnet-localstack-public-1", "subnet-localstack-public-2", "subnet-localstack-private-1", "subnet-localstack-private-2"]
  description = "List of all subnet IDs"
}

output "internet_gateway_id" {
  value       = local.create_real_network ? aws_internet_gateway.this[0].id : "igw-localstack"
  description = "Internet Gateway ID"
}

output "nat_gateway_ids" {
  value       = local.create_real_network ? aws_nat_gateway.this[*].id : ["nat-localstack-1", "nat-localstack-2"]
  description = "List of NAT Gateway IDs"
}
