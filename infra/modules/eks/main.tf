# EKS cluster for distributed crawler workloads
locals {
  cluster_name = var.cluster_name

  # Standard tags for all resources
  common_tags = {
    Environment = var.environment
    Project     = var.project
    Component   = "eks"
    ManagedBy   = "terraform"
  }
}

# KMS key for EKS cluster encryption (disabled to avoid permission issues)
# resource "aws_kms_key" "eks" {
#   description             = "EKS Secret Encryption Key for ${local.cluster_name}"
#   deletion_window_in_days = 7
#   enable_key_rotation     = true
#
#   tags = merge(local.common_tags, {
#     Name = "${local.cluster_name}-eks-encryption-key"
#   })
# }
#
# resource "aws_kms_alias" "eks" {
#   name          = "alias/${local.cluster_name}-eks"
#   target_key_id = aws_kms_key.eks.key_id
# }

# EKS Cluster
resource "aws_eks_cluster" "main" {
  name     = local.cluster_name
  role_arn = aws_iam_role.eks_cluster.arn
  version  = var.kubernetes_version

  vpc_config {
    subnet_ids              = var.subnet_ids
    endpoint_private_access = true
    endpoint_public_access  = true
    public_access_cidrs     = var.allowed_cidrs
    security_group_ids      = [aws_security_group.eks_cluster.id]
  }

  # encryption_config {
  #   provider {
  #     key_arn = aws_kms_key.eks.arn
  #   }
  #   resources = ["secrets"]
  # }  # Disabled to avoid KMS permission issues

  # Enable logging
  enabled_cluster_log_types = [
    "api",
    "audit",
    "authenticator",
    "controllerManager",
    "scheduler"
  ]

  depends_on = [
    aws_iam_role_policy_attachment.eks_cluster_policy,
    aws_iam_role_policy_attachment.eks_service_policy,
    aws_cloudwatch_log_group.eks
  ]

  tags = merge(local.common_tags, {
    Name = local.cluster_name
  })
}

# CloudWatch log group for EKS cluster logs (without KMS encryption)
resource "aws_cloudwatch_log_group" "eks" {
  name              = "/aws/eks/${local.cluster_name}/cluster"
  retention_in_days = var.log_retention_days
  # kms_key_id        = aws_kms_key.eks.arn  # Disabled to avoid KMS permission issues

  tags = local.common_tags
}

# EKS Node Groups for crawler workloads
resource "aws_eks_node_group" "crawler_nodes" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "${local.cluster_name}-crawler-nodes"
  node_role_arn   = aws_iam_role.eks_node_group.arn
  subnet_ids      = var.private_subnet_ids

  instance_types = var.node_instance_types
  ami_type       = "AL2_x86_64"
  capacity_type  = var.capacity_type
  # disk_size removed - using launch template instead

  # Scaling configuration optimized for crawler workloads
  scaling_config {
    desired_size = var.node_desired_capacity
    max_size     = var.node_max_capacity
    min_size     = var.node_min_capacity
  }

  # Update configuration
  update_config {
    max_unavailable_percentage = 25
  }

  # Launch template for additional configuration
  launch_template {
    id      = aws_launch_template.eks_nodes.id
    version = aws_launch_template.eks_nodes.latest_version
  }

  # Labels for node selection
  labels = {
    role        = "crawler"
    environment = var.environment
  }

  # Taints for dedicated crawler nodes (optional)
  dynamic "taint" {
    for_each = var.enable_dedicated_nodes ? [1] : []
    content {
      key    = "crawler-dedicated"
      value  = "true"
      effect = "NO_SCHEDULE"
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.eks_worker_node_policy,
    aws_iam_role_policy_attachment.eks_cni_policy,
    aws_iam_role_policy_attachment.eks_container_registry_policy
  ]

  tags = merge(local.common_tags, {
    Name = "${local.cluster_name}-crawler-nodes"
    # Required for cluster autoscaler
    "k8s.io/cluster-autoscaler/enabled"               = "true"
    "k8s.io/cluster-autoscaler/${local.cluster_name}" = "owned"
  })
}

# Launch template for EKS nodes
resource "aws_launch_template" "eks_nodes" {
  name_prefix = "${local.cluster_name}-node-"

  vpc_security_group_ids = [aws_security_group.eks_nodes.id]

  # No user data - use default EKS AMI bootstrap
  # user_data = base64encode(templatefile("${path.module}/user-data-simple.sh", {
  #   cluster_name = local.cluster_name
  # }))

  # Instance metadata service v2 only
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 3  # Required for VPC CNI IPAM daemon to access metadata from pods
  }

  # Block device mapping for EBS volume
  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size           = 20
      volume_type          = "gp3"
      delete_on_termination = true
      encrypted            = true
    }
  }

  # Monitoring
  monitoring {
    enabled = true
  }

  tag_specifications {
    resource_type = "instance"
    tags = merge(local.common_tags, {
      Name = "${local.cluster_name}-crawler-node"
    })
  }

  tags = local.common_tags
}

# OIDC provider for IRSA (IAM Roles for Service Accounts)
data "tls_certificate" "eks" {
  url = aws_eks_cluster.main.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "eks" {
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.eks.certificates[0].sha1_fingerprint]
  url             = aws_eks_cluster.main.identity[0].oidc[0].issuer

  tags = merge(local.common_tags, {
    Name = "${local.cluster_name}-oidc-provider"
  })
}

# EKS cluster add-ons
resource "aws_eks_addon" "vpc_cni" {
  count                       = var.use_self_managed_cni ? 0 : 1
  cluster_name                = aws_eks_cluster.main.name
  addon_name                  = "vpc-cni"
  addon_version               = var.vpc_cni_version
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
  service_account_role_arn    = aws_iam_role.vpc_cni.arn

  depends_on = [aws_eks_node_group.crawler_nodes]

  tags = local.common_tags
}

resource "aws_eks_addon" "coredns" {
  cluster_name                = aws_eks_cluster.main.name
  addon_name                  = "coredns"
  addon_version               = var.coredns_version
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"

  depends_on = [aws_eks_node_group.crawler_nodes]

  tags = local.common_tags
}

resource "aws_eks_addon" "kube_proxy" {
  cluster_name                = aws_eks_cluster.main.name
  addon_name                  = "kube-proxy"
  addon_version               = var.kube_proxy_version
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"

  depends_on = [aws_eks_node_group.crawler_nodes]

  tags = local.common_tags
}

resource "aws_eks_addon" "ebs_csi" {
  cluster_name                = aws_eks_cluster.main.name
  addon_name                  = "aws-ebs-csi-driver"
  addon_version               = var.ebs_csi_version
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
  service_account_role_arn    = aws_iam_role.ebs_csi.arn

  depends_on = [aws_eks_node_group.crawler_nodes]

  tags = local.common_tags
}
