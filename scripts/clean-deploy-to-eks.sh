#!/bin/bash
set -euo pipefail

# Clean EKS deployment script - removes LocalStack state conflicts
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
    docker info &> /dev/null || log_error "Docker not running"
    log_success "Prerequisites check passed"
}

# Clean Terraform state
clean_terraform_state() {
    log_info "Cleaning Terraform state (LocalStack → AWS Production)..."
    cd infra
    
    # Backup existing state files
    if [ -f terraform.tfstate ]; then
        log_info "Backing up existing Terraform state..."
        cp terraform.tfstate terraform.tfstate.localstack-backup-$(date +%Y%m%d-%H%M%S)
    fi
    
    if [ -f terraform.tfstate.backup ]; then
        cp terraform.tfstate.backup terraform.tfstate.backup.localstack-backup-$(date +%Y%m%d-%H%M%S)
    fi
    
    # Remove state files and terraform directory
    rm -f terraform.tfstate terraform.tfstate.backup
    rm -rf .terraform/terraform.tfstate
    
    # Clean workspace if exists
    if [ -f .terraform/environment ]; then
        rm -f .terraform/environment
    fi
    
    log_success "Terraform state cleaned"
    cd ..
}

# Initialize and deploy infrastructure
deploy_infrastructure() {
    log_info "Initializing and deploying infrastructure..."
    cd infra
    
    # Initialize Terraform
    terraform init
    
    # Create and use production workspace
    terraform workspace new prod 2>/dev/null || terraform workspace select prod
    
    # Plan deployment
    log_info "Creating deployment plan..."
    terraform plan -var="use_localstack=false" -var="env=$ENVIRONMENT" -out=tfplan
    
    # Apply deployment
    log_info "Applying infrastructure deployment (this may take 15-20 minutes)..."
    terraform apply tfplan
    
    # Clean up plan file
    rm -f tfplan
    
    log_success "Infrastructure deployed successfully"
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
        
        # Wait for KEDA to be ready
        log_info "Waiting for KEDA to be ready..."
        kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=keda-operator -n keda --timeout=300s
        kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=keda-metrics-apiserver -n keda --timeout=300s
    else
        log_warning "KEDA already installed"
    fi
    log_success "KEDA ready"
}

# Update manifests with Terraform outputs
update_manifests() {
    log_info "Updating Kubernetes manifests with Terraform outputs..."
    scripts/update-k8s-config.sh infra k8s "$ENVIRONMENT"
    log_success "Manifests updated"
}

# Deploy crawler application
deploy_crawler() {
    log_info "Deploying crawler application..."
    
    # Update image if ECR repository is specified
    if [[ -n "$ECR_REPOSITORY" ]]; then
        log_info "Updating image to: $ECR_REPOSITORY:$IMAGE_TAG"
        # Create backup of original deployment file
        cp k8s/crawler-deployment.yaml k8s/crawler-deployment.yaml.bak
        # Update image
        sed -i.tmp "s|your-ecr-repo/crawler:latest|$ECR_REPOSITORY:$IMAGE_TAG|g" k8s/crawler-deployment.yaml
        rm -f k8s/crawler-deployment.yaml.tmp
    fi
    
    # Apply configurations
    kubectl apply -f k8s/crawler-configmap.yaml
    kubectl apply -f k8s/crawler-secret.yaml
    
    # Apply application
    kubectl apply -f k8s/crawler-deployment.yaml
    kubectl apply -f k8s/crawler-service.yaml
    
    # Apply KEDA scaler
    kubectl apply -f k8s/keda-setup.yaml
    
    # Wait for deployment to be ready
    log_info "Waiting for crawler deployment to be ready..."
    kubectl wait --for=condition=available --timeout=600s deployment/crawler-worker
    
    # Restore original deployment file if it was modified
    if [[ -n "$ECR_REPOSITORY" ]] && [ -f k8s/crawler-deployment.yaml.bak ]; then
        mv k8s/crawler-deployment.yaml.bak k8s/crawler-deployment.yaml
    fi
    
    log_success "Crawler deployed successfully"
}

# Display deployment information
show_deployment_info() {
    log_success "🎉 Clean deployment completed successfully!"
    
    echo ""
    log_info "Deployment Information:"
    echo "  Cluster: $CLUSTER_NAME"
    echo "  Environment: $ENVIRONMENT" 
    echo "  Region: $AWS_REGION"
    
    if [[ -n "$ECR_REPOSITORY" ]]; then
        echo "  Image: $ECR_REPOSITORY:$IMAGE_TAG"
    fi
    
    echo ""
    log_info "Monitoring Commands:"
    echo "  kubectl get pods -l app=crawler-worker -w"
    echo "  kubectl logs -l app=crawler-worker -f"
    echo "  kubectl describe scaledobject crawler-worker-scaler"
    
    echo ""
    log_info "Current Status:"
    kubectl get pods -l app=crawler-worker
}

# Main pipeline
main() {
    log_info "🚀 Starting clean EKS deployment for $CLUSTER_NAME ($ENVIRONMENT)"
    
    # Confirm before cleaning state
    if [ -f infra/terraform.tfstate ]; then
        log_warning "Existing Terraform state detected. This will be backed up and cleared."
        read -p "Continue with clean deployment? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            log_error "Deployment cancelled by user"
        fi
    fi
    
    check_prerequisites
    clean_terraform_state
    deploy_infrastructure
    configure_kubectl
    deploy_keda
    update_manifests
    deploy_crawler
    show_deployment_info
}

main "$@"