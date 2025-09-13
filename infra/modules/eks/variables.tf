variable "cluster_name" {
  description = "Name of the EKS cluster"
  type        = string
}

variable "environment" {
  description = "Environment name (dev/staging/prod)"
  type        = string
}

variable "project" {
  description = "Project name"
  type        = string
  default     = "aedhack"
}

variable "kubernetes_version" {
  description = "Kubernetes version"
  type        = string
  default     = "1.28"
}

# Network variables
variable "vpc_id" {
  description = "VPC ID where EKS cluster will be created"
  type        = string
}

variable "subnet_ids" {
  description = "List of subnet IDs for EKS cluster (both public and private)"
  type        = list(string)
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs for EKS node groups"
  type        = list(string)
}

variable "allowed_cidrs" {
  description = "List of CIDR blocks allowed to access the cluster"
  type        = list(string)
  default     = []
}

# Node group variables
variable "node_instance_types" {
  description = "EC2 instance types for EKS node group"
  type        = list(string)
  default     = ["t3.medium"]
}

variable "node_desired_capacity" {
  description = "Desired number of nodes"
  type        = number
  default     = 2
}

variable "node_min_capacity" {
  description = "Minimum number of nodes"
  type        = number
  default     = 1
}

variable "node_max_capacity" {
  description = "Maximum number of nodes"
  type        = number
  default     = 10
}

variable "node_disk_size" {
  description = "Disk size for worker nodes (GB)"
  type        = number
  default     = 20
}

variable "capacity_type" {
  description = "Type of capacity associated with the EKS Node Group. Valid values: ON_DEMAND, SPOT"
  type        = string
  default     = "ON_DEMAND"
}

# Add-on versions
variable "vpc_cni_version" {
  description = "Version of the VPC CNI add-on"
  type        = string
  default     = "v1.15.4-eksbuild.1"
}

variable "coredns_version" {
  description = "Version of the CoreDNS add-on"
  type        = string
  default     = "v1.10.1-eksbuild.5"
}

variable "kube_proxy_version" {
  description = "Version of the kube-proxy add-on"
  type        = string
  default     = "v1.28.2-eksbuild.2"
}

variable "ebs_csi_version" {
  description = "Version of the EBS CSI driver add-on"
  type        = string
  default     = "v1.24.1-eksbuild.1"
}

# Feature flags
variable "enable_dedicated_nodes" {
  description = "Enable taints for dedicated crawler nodes"
  type        = bool
  default     = false
}

variable "enable_ssh_access" {
  description = "Enable SSH access to worker nodes"
  type        = bool
  default     = false
}

variable "enable_cluster_autoscaler" {
  description = "Enable cluster autoscaler IAM role and policy"
  type        = bool
  default     = true
}

variable "enable_keda" {
  description = "Enable KEDA operator IAM role and policy for SQS scaling"
  type        = bool
  default     = true
}

# Logging
variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 7
}

# AWS resource ARNs for IRSA policies
variable "s3_raw_bucket_arn" {
  description = "ARN of the S3 raw bucket"
  type        = string
}

variable "s3_parsed_bucket_arn" {
  description = "ARN of the S3 parsed bucket"
  type        = string
}

variable "s3_index_ready_bucket_arn" {
  description = "ARN of the S3 index-ready bucket"
  type        = string
}

variable "sqs_crawl_queue_arn" {
  description = "ARN of the SQS crawl queue"
  type        = string
}

variable "sqs_discovery_queue_arn" {
  description = "ARN of the SQS discovery queue"
  type        = string
}

variable "sqs_index_queue_arn" {
  description = "ARN of the SQS index queue"
  type        = string
}

variable "dynamodb_table_arn" {
  description = "ARN of the DynamoDB table"
  type        = string
}
