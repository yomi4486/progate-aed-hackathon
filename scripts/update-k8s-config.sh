#!/bin/bash
set -euo pipefail

# Script to update Kubernetes manifests with Terraform output values
# This script replaces placeholders in K8s manifests with actual AWS resource values

TERRAFORM_DIR=${1:-"infra"}
K8S_DIR=${2:-"k8s"}
ENVIRONMENT=${3:-"prod"}

echo "Updating Kubernetes manifests with Terraform outputs..."

# Check if terraform directory exists
if [[ ! -d "$TERRAFORM_DIR" ]]; then
    echo "Error: Terraform directory '$TERRAFORM_DIR' not found"
    exit 1
fi

# Check if k8s directory exists
if [[ ! -d "$K8S_DIR" ]]; then
    echo "Error: Kubernetes directory '$K8S_DIR' not found"
    exit 1
fi

# Navigate to terraform directory
cd "$TERRAFORM_DIR"

# Get terraform outputs
echo "Fetching Terraform outputs..."
terraform_outputs=$(terraform output -json)

# Extract values using jq
extract_value() {
    local key="$1"
    echo "$terraform_outputs" | jq -r ".${key}.value // empty"
}

# Get all required values
DDB_TABLE=$(extract_value "ddb_table")
SQS_CRAWL_QUEUE_URL=$(extract_value "sqs_url_queue")
SQS_DISCOVERY_QUEUE_URL=$(extract_value "sqs_discovery_queue")  
SQS_INDEX_QUEUE_URL=$(extract_value "sqs_index_queue")
S3_RAW_BUCKET=$(extract_value "s3_raw_bucket")
S3_PARSED_BUCKET=$(extract_value "s3_parsed_bucket")
S3_INDEX_READY_BUCKET=$(extract_value "s3_index_ready_bucket")
CRAWLER_ROLE_ARN=$(extract_value "eks_crawler_service_account_role_arn")
KEDA_ROLE_ARN=$(extract_value "eks.keda_operator_role_arn")

# Extract queue name from URL for CloudWatch metrics
SQS_CRAWL_QUEUE_NAME=$(echo "$SQS_CRAWL_QUEUE_URL" | sed 's/.*\///')

# Validate that we have the required values
if [[ -z "$DDB_TABLE" ]] || [[ -z "$SQS_CRAWL_QUEUE_URL" ]] || [[ -z "$S3_RAW_BUCKET" ]]; then
    echo "Error: Missing required Terraform outputs. Make sure you've run 'terraform apply' successfully."
    echo "Required outputs: ddb_table, sqs_url_queue, s3_raw_bucket"
    exit 1
fi

# Go back to project root
cd ..

echo "Updating crawler secret..."
# Update crawler secret
sed -i.bak \
    -e "s|PLACEHOLDER_DYNAMODB_TABLE|$DDB_TABLE|g" \
    -e "s|PLACEHOLDER_SQS_CRAWL_QUEUE_URL|$SQS_CRAWL_QUEUE_URL|g" \
    -e "s|PLACEHOLDER_SQS_DISCOVERY_QUEUE_URL|$SQS_DISCOVERY_QUEUE_URL|g" \
    -e "s|PLACEHOLDER_SQS_INDEX_QUEUE_URL|$SQS_INDEX_QUEUE_URL|g" \
    -e "s|PLACEHOLDER_S3_RAW_BUCKET|$S3_RAW_BUCKET|g" \
    -e "s|PLACEHOLDER_S3_PARSED_BUCKET|$S3_PARSED_BUCKET|g" \
    -e "s|PLACEHOLDER_S3_INDEX_READY_BUCKET|$S3_INDEX_READY_BUCKET|g" \
    "$K8S_DIR/crawler-secret.yaml"

echo "Updating crawler deployment..."
# Update crawler deployment (service account role)
if [[ -n "$CRAWLER_ROLE_ARN" ]]; then
    sed -i.bak \
        -e "s|PLACEHOLDER_CRAWLER_SERVICE_ACCOUNT_ROLE_ARN|$CRAWLER_ROLE_ARN|g" \
        "$K8S_DIR/crawler-deployment.yaml"
fi

echo "Updating KEDA configuration..."
# Update KEDA configuration
if [[ -n "$KEDA_ROLE_ARN" ]]; then
    sed -i.bak \
        -e "s|PLACEHOLDER_SQS_CRAWL_QUEUE_URL|$SQS_CRAWL_QUEUE_URL|g" \
        -e "s|PLACEHOLDER_SQS_DISCOVERY_QUEUE_URL|$SQS_DISCOVERY_QUEUE_URL|g" \
        -e "s|PLACEHOLDER_SQS_CRAWL_QUEUE_NAME|$SQS_CRAWL_QUEUE_NAME|g" \
        -e "s|PLACEHOLDER_KEDA_OPERATOR_ROLE_ARN|$KEDA_ROLE_ARN|g" \
        "$K8S_DIR/keda-setup.yaml"
fi

# Clean up backup files
rm -f "$K8S_DIR"/*.yaml.bak

echo "✅ Kubernetes manifests updated successfully!"
echo ""
echo "Updated files:"
echo "  - $K8S_DIR/crawler-secret.yaml"
echo "  - $K8S_DIR/crawler-deployment.yaml"
echo "  - $K8S_DIR/keda-setup.yaml"
echo ""
echo "Summary of applied values:"
echo "  DynamoDB Table: $DDB_TABLE"
echo "  SQS Crawl Queue: $SQS_CRAWL_QUEUE_URL"
echo "  SQS Discovery Queue: $SQS_DISCOVERY_QUEUE_URL"
echo "  S3 Raw Bucket: $S3_RAW_BUCKET"
echo "  S3 Parsed Bucket: $S3_PARSED_BUCKET"
if [[ -n "$CRAWLER_ROLE_ARN" ]]; then
    echo "  Crawler IAM Role: $CRAWLER_ROLE_ARN"
fi
if [[ -n "$KEDA_ROLE_ARN" ]]; then
    echo "  KEDA IAM Role: $KEDA_ROLE_ARN"
fi
echo ""
echo "Next steps:"
echo "1. Apply the updated manifests: kubectl apply -f $K8S_DIR/"
echo "2. Verify deployment: kubectl get pods -l app=crawler-worker"
echo "3. Check logs: kubectl logs -l app=crawler-worker --tail=100"
