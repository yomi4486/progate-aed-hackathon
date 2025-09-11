#!/bin/bash
set -euo pipefail

# KEDA (Kubernetes Event-driven Autoscaling) installation script for EKS
# This script installs KEDA with proper IRSA configuration for SQS scaling

CLUSTER_NAME=${1:-"aedhack-dev-cluster"}
AWS_REGION=${2:-"ap-northeast-1"}
NAMESPACE="keda"

echo "Installing KEDA on EKS cluster: ${CLUSTER_NAME}"

# Check if kubectl is configured for the correct cluster
current_context=$(kubectl config current-context)
if [[ $current_context != *"${CLUSTER_NAME}"* ]]; then
    echo "Warning: Current kubectl context (${current_context}) doesn't match cluster name (${CLUSTER_NAME})"
    echo "Please ensure you're connected to the correct cluster"
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Create KEDA namespace
echo "Creating KEDA namespace..."
kubectl create namespace ${NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -

# Add KEDA Helm repository
echo "Adding KEDA Helm repository..."
helm repo add kedacore https://kedacore.github.io/charts
helm repo update

# Install KEDA with Helm
echo "Installing KEDA..."
helm upgrade --install keda kedacore/keda \
    --namespace ${NAMESPACE} \
    --create-namespace \
    --set image.keda.tag=2.12.0 \
    --set image.metricsApiServer.tag=2.12.0 \
    --set image.webhooks.tag=2.12.0 \
    --set replicaCount=2 \
    --set resources.operator.requests.cpu=100m \
    --set resources.operator.requests.memory=128Mi \
    --set resources.operator.limits.cpu=1000m \
    --set resources.operator.limits.memory=1000Mi \
    --set resources.metricServer.requests.cpu=100m \
    --set resources.metricServer.requests.memory=128Mi \
    --set resources.webhooks.requests.cpu=100m \
    --set resources.webhooks.requests.memory=128Mi \
    --set podSecurityContext.runAsNonRoot=true \
    --set podSecurityContext.runAsUser=1001 \
    --set securityContext.allowPrivilegeEscalation=false \
    --set securityContext.readOnlyRootFilesystem=true \
    --set securityContext.capabilities.drop[0]=ALL \
    --wait

# Verify KEDA installation
echo "Verifying KEDA installation..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=keda-operator -n ${NAMESPACE} --timeout=300s
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=keda-metrics-apiserver -n ${NAMESPACE} --timeout=300s

echo "KEDA installation completed successfully!"

# Check KEDA version
echo "KEDA version:"
kubectl get deployment keda-operator -n ${NAMESPACE} -o jsonpath='{.spec.template.spec.containers[0].image}'
echo ""

# Display helpful information
echo ""
echo "Next steps:"
echo "1. Create IAM role for KEDA with SQS permissions"
echo "2. Update TriggerAuthentication in k8s/keda-setup.yaml with correct role ARN"
echo "3. Update ScaledObject with your actual SQS queue URLs"
echo "4. Apply the KEDA configuration:"
echo "   kubectl apply -f k8s/keda-setup.yaml"
echo ""
echo "To monitor KEDA scaling events:"
echo "   kubectl get scaledobjects -n default"
echo "   kubectl describe scaledobject crawler-worker-scaler -n default"
echo "   kubectl logs -l app.kubernetes.io/name=keda-operator -n ${NAMESPACE}"
