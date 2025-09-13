#!/bin/bash
set -e

echo "🚀 Initializing LocalStack resources for crawler..."

ENDPOINT_URL="http://localhost:4566"
REGION="us-east-1"

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

echo "🪣 Creating S3 buckets..."
# Raw data bucket
aws --endpoint-url=$ENDPOINT_URL s3 mb s3://aedhack-devlocal-raw \
    --region $REGION \
    || echo "⚠️ Raw S3 bucket might already exist"

# Parsed data bucket
aws --endpoint-url=$ENDPOINT_URL s3 mb s3://aedhack-devlocal-parsed \
    --region $REGION \
    || echo "⚠️ Parsed S3 bucket might already exist"

# Index ready bucket
aws --endpoint-url=$ENDPOINT_URL s3 mb s3://aedhack-devlocal-index-ready \
    --region $REGION \
    || echo "⚠️ Index ready S3 bucket might already exist"

# Enable S3 versioning for all buckets
for bucket in "aedhack-devlocal-raw" "aedhack-devlocal-parsed" "aedhack-devlocal-index-ready"; do
    aws --endpoint-url=$ENDPOINT_URL s3api put-bucket-versioning \
        --bucket "$bucket" \
        --versioning-configuration Status=Enabled \
        || echo "⚠️ S3 versioning might already be enabled for $bucket"
done

echo "🔍 Setting up OpenSearch domain..."

# Create OpenSearch domain
aws --endpoint-url=$ENDPOINT_URL opensearch create-domain \
    --domain-name "aedhack-devlocal-search" \
    --engine-version "OpenSearch_2.11" \
    --cluster-config '{"InstanceType":"t3.micro.search","InstanceCount":1,"DedicatedMasterEnabled":false}' \
    --ebs-options '{"EBSEnabled":true,"VolumeSize":10,"VolumeType":"gp2"}' \
    --access-policies '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["es:*"],"Resource":"*"}]}' \
    --domain-endpoint-options '{"EnforceHTTPS":false}' \
    --advanced-security-options '{"Enabled":false}' \
    --node-to-node-encryption-options '{"Enabled":false}' \
    --encryption-at-rest-options '{"Enabled":false}' \
    || echo "⚠️ OpenSearch domain might already exist"

echo "⏳ Waiting for OpenSearch domain to be ready..."
sleep 10

# Initialize OpenSearch index
echo "📄 Setting up OpenSearch index..."
curl -X PUT "http://localhost:9200/documents" -H 'Content-Type: application/json' -d'{
  "mappings": {
    "properties": {
      "url": {"type": "keyword"},
      "url_hash": {"type": "keyword"},
      "domain": {"type": "keyword"},
      "title": {
        "type": "text",
        "analyzer": "standard",
        "fields": {"keyword": {"type": "keyword"}}
      },
      "content": {"type": "text", "analyzer": "standard"},
      "content_type": {"type": "keyword"},
      "language": {"type": "keyword"},
      "fetched_at": {"type": "date"},
      "indexed_at": {"type": "date"},
      "content_length": {"type": "integer"},
      "processing_priority": {"type": "integer"},
      "status_code": {"type": "integer"},
      "keywords": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
      "categories": {"type": "keyword"},
      "embedding": {
        "type": "dense_vector",
        "dims": 1536
      },
      "raw_s3_key": {"type": "keyword"},
      "parsed_s3_key": {"type": "keyword"}
    }
  },
  "settings": {
    "index": {
      "number_of_shards": 1,
      "number_of_replicas": 0
    }
  }
}' || echo "⚠️ OpenSearch index might already exist"

echo "📊 Verifying created resources..."

echo "DynamoDB Tables:"
aws --endpoint-url=$ENDPOINT_URL dynamodb list-tables --region $REGION

echo -e "\nSQS Queues:"
aws --endpoint-url=$ENDPOINT_URL sqs list-queues --region $REGION

echo -e "\nS3 Buckets:"
aws --endpoint-url=$ENDPOINT_URL s3 ls

echo -e "\nOpenSearch Domain:"
aws --endpoint-url=$ENDPOINT_URL opensearch list-domain-names || echo "⚠️ Could not list OpenSearch domains"

echo "✅ LocalStack resources initialized successfully!"

# Seed some test URLs for crawling
echo "🌱 Seeding test data..."

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

# Add sample documents to OpenSearch for testing search functionality
echo "📄 Adding sample documents to OpenSearch..."

# Sample Japanese document
curl -X POST "http://localhost:9200/documents/_doc/sample1" -H 'Content-Type: application/json' -d'{
  "url": "https://example.jp/sample1",
  "url_hash": "sample1hash",
  "domain": "example.jp",
  "title": "サンプル記事 - Python プログラミング入門",
  "content": "Pythonは初心者にも優しいプログラミング言語です。データ分析や機械学習の分野で広く使われています。",
  "content_type": "html",
  "language": "ja",
  "fetched_at": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")')",
  "indexed_at": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")')",
  "content_length": 150,
  "processing_priority": 10,
  "status_code": 200,
  "keywords": ["Python", "プログラミング", "初心者"],
  "categories": ["technology", "programming"],
  "raw_s3_key": "raw/sample1.html",
  "parsed_s3_key": "parsed/sample1.json"
}' || echo "⚠️ Failed to add sample document 1"

# Sample English document  
curl -X POST "http://localhost:9200/documents/_doc/sample2" -H 'Content-Type: application/json' -d'{
  "url": "https://example.com/sample2",
  "url_hash": "sample2hash",
  "domain": "example.com",
  "title": "Introduction to Machine Learning with Python",
  "content": "Machine learning is a powerful tool for data analysis. Python provides excellent libraries like scikit-learn and TensorFlow.",
  "content_type": "html",
  "language": "en",
  "fetched_at": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")')",
  "indexed_at": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")')",
  "content_length": 200,
  "processing_priority": 8,
  "status_code": 200,
  "keywords": ["machine learning", "Python", "data analysis"],
  "categories": ["technology", "ai"],
  "raw_s3_key": "raw/sample2.html",
  "parsed_s3_key": "parsed/sample2.json"
}' || echo "⚠️ Failed to add sample document 2"

echo "✅ Sample documents added to OpenSearch!"

echo "🎉 LocalStack setup complete!"
echo ""
echo "You can now:"
echo "  - Check LocalStack web UI: http://localhost:8080"
echo "  - Access services at: http://localhost:4566"
echo "  - Access OpenSearch at: http://localhost:9200"
echo "  - Run crawler with: ENVIRONMENT=devlocal uv run python -m app.crawler.worker"
echo "  - Run indexer with: ENVIRONMENT=devlocal uv run python -m app.indexer.main"
echo "  - Test search with: curl 'http://localhost:9200/documents/_search?q=Python'"
echo "  - Check sample docs: curl 'http://localhost:9200/documents/_search?size=10'"
