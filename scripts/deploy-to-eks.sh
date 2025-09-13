#!/bin/bash
set -euo pipefail

# Complete EKS deployment script for crawler workloads
CLUSTER_NAME=${1:-"aedhack-dev-cluster"}
ENVIRONMENT=${2:-"dev"}
AWS_REGION=${3:-"ap-northeast-1"}
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
    docker info &> /dev/null || log_error "Docker not running"
    log_success "Prerequisites check passed"
}

# Deploy infrastructure
deploy_infrastructure() {
    log_info "Deploying infrastructure..."
    cd infra
    terraform init
    terraform plan -var="use_localstack=false" -var="env=$ENVIRONMENT" -out=tfplan
    terraform apply tfplan
    rm -f tfplan
    cd ..
    log_success "Infrastructure deployed"
}

# Configure kubectl
configure_kubectl() {
    log_info "Configuring kubectl..."
    aws eks update-kubeconfig --region $AWS_REGION --name $CLUSTER_NAME
    kubectl cluster-info
    log_success "kubectl configured"
}

# Deploy KEDA
deploy_keda() {
    log_info "Deploying KEDA..."
    if ! kubectl get namespace keda &> /dev/null; then
        scripts/install-keda.sh "$CLUSTER_NAME" "$AWS_REGION"
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
    log_info "Deploying crawler..."
    if [[ -n "$ECR_REPOSITORY" ]]; then
        sed -i.bak "s|your-ecr-repo/crawler:latest|$ECR_REPOSITORY:$IMAGE_TAG|g" k8s/crawler-deployment.yaml
    fi
    kubectl apply -f k8s/crawler-configmap.yaml
    kubectl apply -f k8s/crawler-secret.yaml
    kubectl apply -f k8s/crawler-deployment.yaml
    kubectl apply -f k8s/crawler-service.yaml
    kubectl apply -f k8s/keda-setup.yaml
    kubectl wait --for=condition=available --timeout=300s deployment/crawler-worker
    log_success "Crawler deployed"
}

# Main pipeline
main() {
    log_info "Starting EKS deployment for $CLUSTER_NAME ($ENVIRONMENT)"
    check_prerequisites
    deploy_infrastructure
    configure_kubectl  
    deploy_keda
    update_manifests
    deploy_crawler
    
    log_success "🎉 Deployment completed!"
    echo "Monitor: kubectl get pods -l app=crawler-worker -w"
}

main
