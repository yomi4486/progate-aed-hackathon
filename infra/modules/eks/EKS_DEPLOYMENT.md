# EKS デプロイメントガイド

## 概要

このドキュメントでは、Amazon EKS での1pod=1クローラー構成によるデプロイメント手順を説明します。

## アーキテクチャ

```
┌─────────────────────────────────────────────────────────────────┐
│                           EKS Cluster                          │
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │ Crawler Pod  │  │ Crawler Pod  │  │ Crawler Pod  │         │
│  │   (Worker)   │  │   (Worker)   │  │   (Worker)   │   ...   │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
│           │                 │                 │                │
│           └─────────────────┼─────────────────┘                │
│                             │                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                   KEDA Scaler                          │   │
│  │           (SQS Queue Depth Based)                      │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
           ┌─────────────────────────────────────────────┐
           │               AWS Services                  │
           │                                             │
           │  ┌─────────┐  ┌─────────┐  ┌─────────────┐  │
           │  │   SQS   │  │   S3    │  │  DynamoDB   │  │
           │  │ Queues  │  │Buckets  │  │URL States   │  │
           │  └─────────┘  └─────────┘  └─────────────┘  │
           └─────────────────────────────────────────────┘
```

## 前提条件

### 必要なツール

```bash
# AWS CLI
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip && sudo ./aws/install

# kubectl
curl -o kubectl https://amazon-eks.s3.us-west-2.amazonaws.com/1.21.2/2021-07-05/bin/linux/amd64/kubectl
chmod +x ./kubectl && sudo mv ./kubectl /usr/local/bin

# Helm
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# Terraform
wget https://releases.hashicorp.com/terraform/1.6.0/terraform_1.6.0_linux_amd64.zip
unzip terraform_1.6.0_linux_amd64.zip && sudo mv terraform /usr/local/bin/

# jq
sudo apt-get install jq  # Ubuntu/Debian
# or
brew install jq  # macOS
```

### AWS認証情報の設定

```bash
aws configure
# または環境変数で設定
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"  
export AWS_DEFAULT_REGION="us-east-1"
```

## デプロイメント手順

### 1. 簡単デプロイ（推奨）

全体のパイプラインを一括実行：

```bash
# 基本デプロイメント
./scripts/deploy-to-eks.sh

# カスタムパラメータでデプロイ
./scripts/deploy-to-eks.sh my-cluster prod us-east-1

# ECRリポジトリを指定してイメージビルド＋デプロイ
./scripts/deploy-to-eks.sh my-cluster prod us-east-1 123456789012.dkr.ecr.us-east-1.amazonaws.com/crawler v1.0.0
```

### 2. 段階的デプロイ

個別にステップを実行する場合：

#### Step 1: インフラストラクチャのデプロイ

```bash
cd infra

# Terraform初期化
terraform init

# 実行プランの確認
terraform plan -var="use_localstack=false" -var="env=prod"

# インフラのデプロイ
terraform apply -var="use_localstack=false" -var="env=prod"
```

#### Step 2: EKSクラスターへの接続

```bash
# kubeconfigの更新
aws eks update-kubeconfig --region us-east-1 --name aedhack-prod-cluster

# 接続確認
kubectl cluster-info
kubectl get nodes
```

#### Step 3: KEDAのインストール

```bash
./scripts/install-keda.sh aedhack-prod-cluster us-east-1
```

#### Step 4: Kubernetesマニフェストの更新

```bash
# Terraformの出力値でマニフェストを更新
./scripts/update-k8s-config.sh infra k8s prod
```

#### Step 5: クローラーアプリケーションのデプロイ

```bash
# 設定の適用
kubectl apply -f k8s/crawler-configmap.yaml
kubectl apply -f k8s/crawler-secret.yaml

# アプリケーションのデプロイ
kubectl apply -f k8s/crawler-deployment.yaml
kubectl apply -f k8s/crawler-service.yaml

# KEDAスケーラーの適用
kubectl apply -f k8s/keda-setup.yaml
```

## 動作確認

### ポッドの状態確認

```bash
# クローラーポッドの状態
kubectl get pods -l app=crawler-worker

# ポッドの詳細情報
kubectl describe pods -l app=crawler-worker

# ログの確認
kubectl logs -l app=crawler-worker --tail=100 -f
```

### スケーリングの確認

```bash
# KEDA ScaledObjectの状態
kubectl get scaledobjects
kubectl describe scaledobject crawler-worker-scaler

# HPA（Horizontal Pod Autoscaler）の状態
kubectl get hpa

# スケーリングイベントの確認
kubectl get events --sort-by=.metadata.creationTimestamp
```

### ヘルスチェック

```bash
# 個別ポッドのヘルスチェック
kubectl exec -l app=crawler-worker -- python -m app.crawler.worker health

# サービス経由でのヘルスチェック
kubectl port-forward svc/crawler-worker-service 8080:8080
curl http://localhost:8080/health
```

## 設定のカスタマイズ

### クローラー設定

`k8s/crawler-configmap.yaml`で設定を変更：

```yaml
data:
  MAX_CONCURRENT_REQUESTS: "20"  # 同時リクエスト数
  DEFAULT_QPS_PER_DOMAIN: "2"    # ドメイン別QPS制限
  MAX_RETRIES: "5"               # 最大リトライ回数
```

### スケーリング設定

`k8s/keda-setup.yaml`でスケーリングパラメータを調整：

```yaml
spec:
  minReplicaCount: 2      # 最小ポッド数
  maxReplicaCount: 50     # 最大ポッド数
  pollingInterval: 30     # ポーリング間隔（秒）
  triggers:
  - type: aws-sqs-queue
    metadata:
      queueLength: "10"   # スケールアウトする閾値
```

### リソース制限

`k8s/crawler-deployment.yaml`でリソース制限を調整：

```yaml
resources:
  requests:
    memory: "1Gi"    # 要求メモリ
    cpu: "500m"      # 要求CPU
  limits:
    memory: "2Gi"    # メモリ制限
    cpu: "1000m"     # CPU制限
```

## トラブルシューティング

### よくある問題

#### 1. ポッドがPending状態

```bash
kubectl describe pods -l app=crawler-worker
```

原因と対処法：
- **リソース不足**: ノードのスケールアップまたはリソース要求の削減
- **IAMロール設定不備**: ServiceAccountのIAMロールARNを確認
- **イメージプル失敗**: ECRの認証情報とイメージタグを確認

#### 2. スケーリングが動作しない

```bash
kubectl logs -n keda -l app.kubernetes.io/name=keda-operator
kubectl describe scaledobject crawler-worker-scaler
```

原因と対処法：
- **KEDA権限不足**: IAMロールにSQS/CloudWatch権限があるか確認
- **SQSキューURL間違い**: Terraform出力値が正しく設定されているか確認
- **メトリクス取得失敗**: KEDAオペレーターのログでエラーを確認

#### 3. AWS API エラー

```bash
kubectl logs -l app=crawler-worker
```

原因と対処法：
- **認証情報不足**: IAM Roles for Service Accounts (IRSA)の設定確認
- **権限不足**: IAMポリシーでS3/SQS/DynamDB権限を確認
- **リージョン設定**: 環境変数 `AWS_REGION` が正しいか確認

### ログ分析

```bash
# 全ポッドのログを時系列で表示
kubectl logs -l app=crawler-worker --timestamps=true --since=1h

# 特定エラーパターンの検索
kubectl logs -l app=crawler-worker | grep -i error

# JSON形式ログの整形表示
kubectl logs -l app=crawler-worker | jq '.'
```

## 運用メンテナンス

### ローリング更新

```bash
# 新しいイメージでの更新
kubectl set image deployment/crawler-worker crawler=新しいイメージ:タグ

# 更新状況の確認
kubectl rollout status deployment/crawler-worker

# 問題がある場合のロールバック
kubectl rollout undo deployment/crawler-worker
```

### 設定変更の反映

```bash
# ConfigMapまたはSecretを更新後
kubectl apply -f k8s/crawler-configmap.yaml
kubectl apply -f k8s/crawler-secret.yaml

# ポッドの再起動（設定変更を反映）
kubectl rollout restart deployment/crawler-worker
```

### スケール調整

```bash
# 手動スケール
kubectl scale deployment crawler-worker --replicas=10

# KEDA設定の更新
kubectl apply -f k8s/keda-setup.yaml
```

## セキュリティ考慮事項

### IAM最小権限の原則

各IAMロールは必要最小限の権限のみ付与：

- **Crawler Role**: 指定されたS3バケット、SQSキュー、DynamoDBテーブルのみアクセス可能
- **KEDA Role**: SQSメトリクス取得とCloudWatchメトリクス読み取りのみ
- **Node Role**: EKS運用に必要な基本権限のみ

### ネットワークセキュリティ

- プライベートサブネットでのワーカーノード配置
- セキュリティグループによる通信制限
- NACLによる追加ネットワーク制御

### 機密情報管理

- AWS Secrets Managerまたは外部秘密管理システムの利用推奨
- 環境変数での機密情報露出の回避
- 定期的な認証情報ローテーション

## 監視・アラート

### CloudWatch連携

```bash
# カスタムメトリクスの送信例
aws cloudwatch put-metric-data \
    --namespace "AEDHack/Crawler" \
    --metric-data MetricName=ProcessedURLs,Value=100,Unit=Count
```

### Prometheus/Grafana（オプション）

詳細な監視が必要な場合は、Prometheus Operatorをデプロイ：

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install monitoring prometheus-community/kube-prometheus-stack
```

---

## サポート

問題や質問がある場合は、以下を確認してください：

1. [トラブルシューティング](#トラブルシューティング)セクション
2. `kubectl describe` と `kubectl logs` でのデバッグ情報収集
3. AWS CloudTrailでのAPI呼び出し履歴確認
