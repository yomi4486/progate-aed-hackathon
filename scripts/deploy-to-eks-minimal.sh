#!/bin/bash
set -euo pipefail

# Minimal EKS deployment script - skips OpenSearch and problematic components
CLUSTER_NAME=${1:-"aedhack-prod-cluster"}
ENVIRONMENT=${2:-"prod"}
AWS_REGION=${3:-"us-east-1"}
ECR_REPOSITORY=${4:-""}
IMAGE_TAG=${5:-"latest"}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    local required_tools=("terraform" "kubectl" "aws" "helm" "docker" "jq")
    for tool in "${required_tools[@]}"; do
        command -v $tool &> /dev/null || log_error "Required tool '$tool' not found"
    done
    aws sts get-caller-identity &> /dev/null || log_error "AWS credentials not configured"
    log_success "Prerequisites check passed"
}

# Clean Terraform state
clean_terraform_state() {
    log_info "Cleaning Terraform state..."
    cd infra
    
    # Backup existing state files
    if [ -f terraform.tfstate ]; then
        log_info "Backing up existing Terraform state..."
        cp terraform.tfstate terraform.tfstate.backup-$(date +%Y%m%d-%H%M%S)
    fi
    
    # Remove state files
    rm -f terraform.tfstate terraform.tfstate.backup
    rm -rf .terraform/terraform.tfstate
    
    log_success "Terraform state cleaned"
    cd ..
}

# Deploy minimal infrastructure (without OpenSearch)
deploy_minimal_infrastructure() {
    log_info "Deploying minimal infrastructure (Network, Storage, Queue, DynamoDB, EKS)..."
    cd infra
    
    # Create minimal Terraform configuration
    cat > main-minimal.tf << 'EOF'
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

# EKS cluster for crawler workloads (simplified without KMS encryption)
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
  allowed_cidrs      = ["0.0.0.0/0"]

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
  enable_ssh_access         = false
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
EOF

    # Backup original main.tf
    mv main.tf main.tf.full-backup
    mv main-minimal.tf main.tf
    
    # Initialize and deploy
    terraform init
    terraform workspace new prod 2>/dev/null || terraform workspace select prod
    
    log_info "Creating minimal deployment plan..."
    terraform plan -var="use_localstack=false" -var="env=$ENVIRONMENT" -out=tfplan
    
    log_info "Applying minimal infrastructure (Network, Storage, Queue, DynamoDB, EKS only)..."
    terraform apply tfplan
    
    rm -f tfplan
    log_success "Minimal infrastructure deployed"
    cd ..
}

# Configure kubectl
configure_kubectl() {
    log_info "Configuring kubectl..."
    aws eks update-kubeconfig --region $AWS_REGION --name $CLUSTER_NAME
    kubectl cluster-info
    kubectl get nodes
    log_success "kubectl configured"
}

# Deploy KEDA
deploy_keda() {
    log_info "Deploying KEDA..."
    if ! kubectl get namespace keda &> /dev/null; then
        scripts/install-keda.sh "$CLUSTER_NAME" "$AWS_REGION"
        
        log_info "Waiting for KEDA to be ready..."
        kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=keda-operator -n keda --timeout=300s
        kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=keda-metrics-apiserver -n keda --timeout=300s
    else
        log_warning "KEDA already installed"
    fi
    log_success "KEDA ready"
}

# Update manifests
update_manifests() {
    log_info "Updating Kubernetes manifests..."
    scripts/update-k8s-config.sh infra k8s "$ENVIRONMENT"
    log_success "Manifests updated"
}

# Deploy crawler
deploy_crawler() {
    log_info "Deploying crawler (without search functionality)..."
    
    # Update image if specified
    if [[ -n "$ECR_REPOSITORY" ]]; then
        log_info "Updating image to: $ECR_REPOSITORY:$IMAGE_TAG"
        cp k8s/crawler-deployment.yaml k8s/crawler-deployment.yaml.bak
        sed -i.tmp "s|your-ecr-repo/crawler:latest|$ECR_REPOSITORY:$IMAGE_TAG|g" k8s/crawler-deployment.yaml
        rm -f k8s/crawler-deployment.yaml.tmp
    fi
    
    # Apply configurations
    kubectl apply -f k8s/crawler-configmap.yaml
    kubectl apply -f k8s/crawler-secret.yaml
    kubectl apply -f k8s/crawler-deployment.yaml
    kubectl apply -f k8s/crawler-service.yaml
    kubectl apply -f k8s/keda-setup.yaml
    
    log_info "Waiting for crawler deployment..."
    kubectl wait --for=condition=available --timeout=600s deployment/crawler-worker
    
    # Restore backup
    if [[ -n "$ECR_REPOSITORY" ]] && [ -f k8s/crawler-deployment.yaml.bak ]; then
        mv k8s/crawler-deployment.yaml.bak k8s/crawler-deployment.yaml
    fi
    
    log_success "Crawler deployed"
}

# Show results
show_results() {
    log_success "🎉 Minimal EKS deployment completed!"
    
    echo ""
    log_info "Deployed Components:"
    echo "  ✅ VPC and Networking"
    echo "  ✅ S3 Buckets (raw, parsed, index-ready)"  
    echo "  ✅ SQS Queues (crawl, discovery, index)"
    echo "  ✅ DynamoDB Table (URL states)"
    echo "  ✅ EKS Cluster with Auto Scaling"
    echo "  ✅ KEDA for Pod Auto Scaling"
    echo "  ✅ Distributed Crawler Workers"
    echo "  ❌ OpenSearch (skipped due to permissions)"
    
    echo ""
    log_info "Next Steps:"
    echo "  1. Test crawler deployment: kubectl get pods -l app=crawler-worker"
    echo "  2. Push URLs to SQS: aws sqs send-message --queue-url <URL> --message-body '{\"url\":\"https://example.com\"}'"
    echo "  3. Monitor scaling: kubectl get hpa"
    echo "  4. Add OpenSearch later when permissions are resolved"
    
    echo ""
    log_info "Current Status:"
    kubectl get pods -l app=crawler-worker
}

# Main pipeline
main() {
    log_info "🚀 Starting minimal EKS deployment (without OpenSearch)"
    log_warning "This deployment skips OpenSearch due to AWS permission issues"
    
    check_prerequisites
    clean_terraform_state
    deploy_minimal_infrastructure
    configure_kubectl
    deploy_keda
    update_manifests
    deploy_crawler
    show_results
}

main "$@"