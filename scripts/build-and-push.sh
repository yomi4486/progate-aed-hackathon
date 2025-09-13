#!/bin/bash
set -e

# Configuration
ECR_REGION=${AWS_REGION:-us-east-1}
ECR_REPO_NAME=${ECR_REPO_NAME:-crawler-worker}
IMAGE_TAG=${IMAGE_TAG:-latest}
AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID}

# Check required variables
if [ -z "$AWS_ACCOUNT_ID" ]; then
    echo "Error: AWS_ACCOUNT_ID environment variable is required"
    exit 1
fi

ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${ECR_REGION}.amazonaws.com"
FULL_IMAGE_NAME="${ECR_REGISTRY}/${ECR_REPO_NAME}:${IMAGE_TAG}"

echo "Building and pushing crawler image..."
echo "Registry: ${ECR_REGISTRY}"
echo "Repository: ${ECR_REPO_NAME}"
echo "Tag: ${IMAGE_TAG}"
echo "Full image name: ${FULL_IMAGE_NAME}"

# Login to ECR
echo "Logging into ECR..."
aws ecr get-login-password --region ${ECR_REGION} | docker login --username AWS --password-stdin ${ECR_REGISTRY}

# Create ECR repository if it doesn't exist
echo "Ensuring ECR repository exists..."
aws ecr describe-repositories --repository-names ${ECR_REPO_NAME} --region ${ECR_REGION} >/dev/null 2>&1 || {
    echo "Creating ECR repository ${ECR_REPO_NAME}..."
    aws ecr create-repository --repository-name ${ECR_REPO_NAME} --region ${ECR_REGION}
}

# Build the image
echo "Building Docker image..."
docker build -t ${FULL_IMAGE_NAME} --target production .

# Tag with additional tags
docker tag ${FULL_IMAGE_NAME} ${ECR_REGISTRY}/${ECR_REPO_NAME}:$(git rev-parse --short HEAD)
docker tag ${FULL_IMAGE_NAME} ${ECR_REGISTRY}/${ECR_REPO_NAME}:$(date +%Y%m%d-%H%M%S)

# Push the image
echo "Pushing image to ECR..."
docker push ${FULL_IMAGE_NAME}
docker push ${ECR_REGISTRY}/${ECR_REPO_NAME}:$(git rev-parse --short HEAD)
docker push ${ECR_REGISTRY}/${ECR_REPO_NAME}:$(date +%Y%m%d-%H%M%S)

echo "Successfully built and pushed image: ${FULL_IMAGE_NAME}"
echo "Additional tags pushed:"
echo "  - ${ECR_REGISTRY}/${ECR_REPO_NAME}:$(git rev-parse --short HEAD)"
echo "  - ${ECR_REGISTRY}/${ECR_REPO_NAME}:$(date +%Y%m%d-%H%M%S)"
