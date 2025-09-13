#!/bin/bash
set -e

echo "🚀 Initializing LocalStack resources for crawler..."

ENDPOINT_URL="http://localhost:4566"
REGION="ap-northeast-1"

# Wait for LocalStack to be ready
echo "⏳ Waiting for LocalStack to be ready..."
until curl -fs http://localhost:4566/_localstack/health | grep -q '"dynamodb": "available"' > /dev/null 2>&1; do
    echo "LocalStack is not ready yet, waiting..."
    sleep 2
done
echo "✅ LocalStack is ready!"

# Set AWS CLI configuration for LocalStack
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=$REGION

echo "🗄️ Creating DynamoDB table..."
aws --endpoint-url=$ENDPOINT_URL dynamodb create-table \
    --table-name "aedhack-devlocal-url-states" \
    --attribute-definitions \
        AttributeName=url_hash,AttributeType=S \
        AttributeName=domain,AttributeType=S \
        AttributeName=state,AttributeType=S \
        AttributeName=updated_at,AttributeType=S \
    --key-schema \
        AttributeName=url_hash,KeyType=HASH \
    --global-secondary-indexes \
        'IndexName=DomainStateIndex,KeySchema=[{AttributeName=domain,KeyType=HASH},{AttributeName=state,KeyType=RANGE}],Projection={ProjectionType=ALL},ProvisionedThroughput={ReadCapacityUnits=5,WriteCapacityUnits=5}' \
        'IndexName=StateUpdatedIndex,KeySchema=[{AttributeName=state,KeyType=HASH},{AttributeName=updated_at,KeyType=RANGE}],Projection={ProjectionType=ALL},ProvisionedThroughput={ReadCapacityUnits=5,WriteCapacityUnits=5}' \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
    --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES \
    || echo "⚠️ DynamoDB table might already exist"

echo "📨 Creating SQS queues..."

# Create main crawl queue  
aws --endpoint-url=$ENDPOINT_URL sqs create-queue \
    --queue-name "aedhack-devlocal-crawl-queue" \
    || echo "⚠️ Crawl queue might already exist"

# Create discovery queue
aws --endpoint-url=$ENDPOINT_URL sqs create-queue \
    --queue-name "aedhack-devlocal-discovery-queue" \
    || echo "⚠️ Discovery queue might already exist"

# Create indexing queue
aws --endpoint-url=$ENDPOINT_URL sqs create-queue \
    --queue-name "aedhack-devlocal-indexing-queue" \
    || echo "⚠️ Indexing queue might already exist"

# Create processing coordination queue
aws --endpoint-url=$ENDPOINT_URL sqs create-queue \
    --queue-name "aedhack-devlocal-processing-queue" \
    || echo "⚠️ Processing queue might already exist"

# Create dead letter queue
aws --endpoint-url=$ENDPOINT_URL sqs create-queue \
    --queue-name "aedhack-devlocal-dlq" \
    || echo "⚠️ DLQ might already exist"

echo "🪣 Creating S3 bucket..."
aws --endpoint-url=$ENDPOINT_URL s3 mb s3://aedhack-devlocal-raw \
    --region $REGION \
    || echo "⚠️ S3 bucket might already exist"

# Enable S3 versioning
aws --endpoint-url=$ENDPOINT_URL s3api put-bucket-versioning \
    --bucket "aedhack-devlocal-raw" \
    --versioning-configuration Status=Enabled \
    || echo "⚠️ S3 versioning might already be enabled"

echo "📊 Verifying created resources..."

echo "DynamoDB Tables:"
aws --endpoint-url=$ENDPOINT_URL dynamodb list-tables --region $REGION

echo -e "\nSQS Queues:"
aws --endpoint-url=$ENDPOINT_URL sqs list-queues --region $REGION

echo -e "\nS3 Buckets:"
aws --endpoint-url=$ENDPOINT_URL s3 ls

echo "✅ LocalStack resources initialized successfully!"

# Seed some test URLs for crawling
echo "🌱 Seeding test crawl URLs..."

aws --endpoint-url=$ENDPOINT_URL sqs send-message \
    --queue-url "http://localhost:4566/000000000000/aedhack-devlocal-crawl-queue" \
    --message-body '{
        "url": "https://httpbin.org/html",
        "domain": "httpbin.org", 
        "priority": 10,
        "retry_count": 0,
        "source": "manual-seed",
        "discovered_at": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'"
    }' \
    || echo "⚠️ Failed to seed test URL"

aws --endpoint-url=$ENDPOINT_URL sqs send-message \
    --queue-url "http://localhost:4566/000000000000/aedhack-devlocal-crawl-queue" \
    --message-body '{
        "url": "https://httpbin.org/json", 
        "domain": "httpbin.org",
        "priority": 5,
        "retry_count": 0,
        "source": "manual-seed",
        "discovered_at": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'"
    }' \
    || echo "⚠️ Failed to seed test URL"

echo "✅ Test URLs seeded to crawl queue!"

echo "🎉 LocalStack setup complete!"
echo ""
echo "You can now:"
echo "  - Check LocalStack web UI: http://localhost:8080"
echo "  - Access services at: http://localhost:4566"
echo "  - Run crawler with: ENVIRONMENT=devlocal uv run python -m app.crawler.worker"
