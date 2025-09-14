# Security Groups for EKS cluster and nodes

# Security group for EKS control plane
resource "aws_security_group" "eks_cluster" {
  name_prefix = "${var.cluster_name}-cluster-"
  description = "Security group for EKS cluster control plane"
  vpc_id      = var.vpc_id

  # Egress rules
  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, {
    Name = "${var.cluster_name}-cluster-sg"
  })
}

# Security group rules for cluster
resource "aws_security_group_rule" "cluster_ingress_workstation_https" {
  count             = length(var.allowed_cidrs) > 0 ? 1 : 0
  description       = "Allow workstation to communicate with the cluster API Server"
  type              = "ingress"
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  cidr_blocks       = var.allowed_cidrs
  security_group_id = aws_security_group.eks_cluster.id
}

# Auto Mode manages node-to-cluster communication automatically

# Auto Mode clusters manage node security groups automatically
