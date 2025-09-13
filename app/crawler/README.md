# 分散クローラー - Amazon EKS デプロイメントガイド

## 概要

このプロジェクトは、Amazon EKS上で動作する分散ウェブクローラーです。1つのPodが1つのクローラーワーカーとして動作し、SQSキューからメッセージを受信してウェブページをクロールします。

## アーキテクチャ

- **EKS Cluster**: クローラーワーカーのオーケストレーション
- **DynamoDB**: URL状態管理と分散ロック
- **SQS**: クロール対象URLキューイング
- **S3**: クロール済みコンテンツ保存
- **ElastiCache (Redis)**: レート制御とキャッシュ

## 前提条件

### AWS リソース

1. **EKS Cluster**
   ```bash
   # EKSクラスター作成 (例)
   eksctl create cluster --name aedhack-cluster --region ap-northeast-1 --nodes 3 --node-type t3.medium
   ```

2. **DynamoDB テーブル**
   ```bash
   aws dynamodb create-table \
     --table-name aedhack-prod-url-states \
     --attribute-definitions AttributeName=url_hash,AttributeType=S \
     --key-schema AttributeName=url_hash,KeyType=HASH \
     --billing-mode PAY_PER_REQUEST \
     --region ap-northeast-1
   ```

3. **SQS キュー**
   ```bash
   # クロールキュー
   aws sqs create-queue \
     --queue-name aedhack-prod-crawl-queue \
     --region ap-northeast-1
   
   # 発見キュー
   aws sqs create-queue \
     --queue-name aedhack-prod-discovery-queue \
     --region ap-northeast-1
   ```

4. **S3 バケット**
   ```bash
   aws s3 mb s3://aedhack-prod-raw-content --region ap-northeast-1
   ```

5. **ElastiCache Redis**
   ```bash
   aws elasticache create-replication-group \
     --replication-group-id aedhack-prod-redis \
     --description "AED Hackathon Redis Cluster" \
     --node-type cache.t3.micro \
     --region ap-northeast-1
   ```

### IAM 権限

EKSワーカーノードまたはPod用のIAMロールに以下の権限を付与:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:DeleteItem",
        "dynamodb:Query",
        "dynamodb:Scan",
        "dynamodb:BatchGetItem",
        "dynamodb:BatchWriteItem"
      ],
      "Resource": "arn:aws:dynamodb:ap-northeast-1:*:table/aedhack-prod-url-states"
    },
    {
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:SendMessage",
        "sqs:GetQueueAttributes"
      ],
      "Resource": [
        "arn:aws:sqs:ap-northeast-1:*:aedhack-prod-crawl-queue",
        "arn:aws:sqs:ap-northeast-1:*:aedhack-prod-discovery-queue"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::aedhack-prod-raw-content/*"
    }
  ]
}
```

## デプロイメント手順

### 1. 環境設定

```bash
export AWS_ACCOUNT_ID=123456789012
export AWS_REGION=ap-northeast-1
export EKS_CLUSTER_NAME=aedhack-cluster
export ECR_REPO_NAME=crawler-worker
```

### 2. 設定ファイル更新

`k8s/crawler-secret.yaml` を編集して、実際のAWSリソースのURLを設定:

```yaml
stringData:
  DYNAMODB_TABLE: "aedhack-prod-url-states"
  SQS_CRAWL_QUEUE_URL: "https://sqs.ap-northeast-1.amazonaws.com/YOUR_ACCOUNT/aedhack-prod-crawl-queue"
  SQS_DISCOVERY_QUEUE_URL: "https://sqs.ap-northeast-1.amazonaws.com/YOUR_ACCOUNT/aedhack-prod-discovery-queue"
  S3_RAW_BUCKET: "aedhack-prod-raw-content"
  REDIS_URL: "redis://your-elasticache-cluster.cache.amazonaws.com:6379/0"
```

### 3. Docker イメージをビルド・プッシュ

```bash
# ECR認証とイメージビルド・プッシュ
./scripts/build-and-push.sh
```

### 4. EKSにデプロイ

```bash
# EKSクラスターにデプロイ
./scripts/deploy-to-eks.sh
```

## ローカル開発

### Docker Composeでの開発

```yaml
# docker-compose.dev.yml (例)
version: '3.8'
services:
  crawler:
    build:
      context: .
      target: development
    environment:
      - CRAWLER_ENV=devlocal
      - LOG_LEVEL=DEBUG
    volumes:
      - ./app:/app/app:ro
  
  localstack:
    image: localstack/localstack
    ports:
      - "4566:4566"
    environment:
      - SERVICES=dynamodb,sqs,s3
  
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
```

### 単体テスト実行

```bash
# 依存関係インストール
uv sync

# テスト実行
pytest app/crawler/tests/ -v

# 単体でワーカーテスト
python -m app.crawler.worker health --environment devlocal
```

## 運用

### ログ確認

```bash
# すべてのワーカーPodログ
kubectl logs -f deployment/crawler-worker -n default

# 特定のPodのログ
kubectl logs -f crawler-worker-xxx-yyy -n default
```

### スケーリング

```bash
# 手動スケーリング
kubectl scale deployment crawler-worker --replicas=5 -n default

# HPA状態確認
kubectl get hpa crawler-worker-hpa -n default
```

### ヘルスチェック

```bash
# Podの健全性確認
kubectl get pods -l app=crawler-worker -n default

# ワーカー内部統計
kubectl exec -it deployment/crawler-worker -- python -m app.crawler.worker stats
```

### トラブルシューティング

```bash
# Pod詳細情報
kubectl describe pod crawler-worker-xxx-yyy -n default

# イベント確認
kubectl get events --sort-by=.metadata.creationTimestamp -n default

# リソース使用状況
kubectl top pods -l app=crawler-worker -n default
```

## メトリクス

以下のメトリクスが収集されます:

- `crawler_messages_received_total`: 受信メッセージ数
- `crawler_urls_crawled_total`: クロール済みURL数  
- `crawler_success_rate`: 成功率
- `crawler_lock_acquisition_rate`: ロック取得率
- `crawler_processing_duration_seconds`: 処理時間

## アラート設定例

```yaml
# CloudWatch Alarms例
ErrorRateHigh:
  Type: AWS::CloudWatch::Alarm
  Properties:
    AlarmName: CrawlerHighErrorRate
    MetricName: crawler_error_rate
    Threshold: 0.1
    ComparisonOperator: GreaterThanThreshold

QueueDepthHigh:
  Type: AWS::CloudWatch::Alarm  
  Properties:
    AlarmName: CrawlerQueueBacklog
    MetricName: crawler_queue_depth
    Threshold: 1000
    ComparisonOperator: GreaterThanThreshold
```

## セキュリティ

- コンテナは非rootユーザーで実行
- Read-onlyルートファイルシステム
- 最小権限の原則でIAM権限設定
- SecretsでAWSリソース情報を管理

## パフォーマンスチューニング

### リソース制限
```yaml
resources:
  requests:
    memory: "512Mi"
    cpu: "250m"
  limits:
    memory: "1Gi" 
    cpu: "500m"
```

### 同時実行数調整
```yaml
env:
  MAX_CONCURRENT_REQUESTS: "10"  # Pod当たりの同時リクエスト数
  DEFAULT_QPS_PER_DOMAIN: "1"   # ドメイン当たりのQPS制限
```

## 今後の改善点

- [ ] Prometheus/Grafana統合
- [ ] Jaeger分散トレーシング
- [ ] Istio Service Mesh統合
- [ ] 階層化アーキテクチャ (発見層/クロール層分離)
- [ ] バッチ処理モードの追加
