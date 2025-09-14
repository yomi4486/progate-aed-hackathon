# 分散クローラー実装タスクリスト

## Phase 1: 基盤コンポーネント実装

### 1.1 プロジェクト構造セットアップ

- [ ] 1.1.1 `app/crawler/` ディレクトリ構造作成

  ```
  app/crawler/
  ├── __init__.py
  ├── core/           # コアロジック
  ├── config/         # 設定管理
  ├── state/          # 状態管理
  ├── rate_limiter/   # レート制御
  ├── http_client/    # HTTP クライアント
  ├── discovery/      # URL発見
  ├── worker/         # ワーカー実装
  ├── coordinator/    # コーディネーター
  ├── monitoring/     # 監視・メトリクス
  ├── utils/          # ユーティリティ
  └── tests/          # テストコード
  ```

- [ ] 1.1.2 基本設定ファイル作成

  - `pyproject.toml` にクローラー用依存関係追加
  - `app/crawler/__init__.py` 作成
  - ログ設定とエラーハンドリング基盤

- [ ] 1.1.3 基本型定義実装
  - `app/crawler/types.py` 作成
  - URLState, CrawlResult, CrawlerConfig 等の型定義
  - 既存 `app/schema/` との統合確認

**受け入れ基準:**

- ディレクトリ構造が作成されている
- 基本的な import が動作する
- ログが出力される

### 1.2 設定管理システム

- [ ] 1.2.1 `config/settings.py` 実装

  ```python
  class CrawlerConfig(BaseSettings):
      # AWS関連設定
      aws_region: str
      dynamodb_table: str
      sqs_crawl_queue_url: str
      sqs_discovery_queue_url: str
      s3_raw_bucket: str
      redis_url: str

      # クローラー設定
      crawler_id: str = Field(default_factory=lambda: str(uuid4()))
      max_concurrent_requests: int = 10
      request_timeout: int = 30
      user_agent: str = "AEDHack-Crawler/1.0"

      # レート制御設定
      default_qps_per_domain: int = 1
      domain_qps_overrides: Dict[str, int] = {}

      # リトライ設定
      max_retries: int = 3
      base_backoff_seconds: int = 60
      max_backoff_seconds: int = 3600

      # タイムアウト設定
      acquisition_ttl_seconds: int = 3600
      heartbeat_interval_seconds: int = 30
  ```

- [ ] 1.2.2 環境別設定ファイル

  - `config/dev.yaml`
  - `config/staging.yaml`
  - `config/prod.yaml`
  - 環境変数オーバーライド機能

- [ ] 1.2.3 設定バリデーション
  - Pydantic バリデーター実装
  - 必須設定項目チェック
  - AWS リソース接続確認

**受け入れ基準:**

- 環境変数から設定が読み込める
- 不正な設定でエラーになる
- AWS 接続設定が検証される

### 1.3 基本ユーティリティ

- [ ] 1.3.1 `utils/url.py` - URL 正規化・ハッシュ生成

  ```python
  def normalize_url(url: str) -> str:
      """URL正規化（fragment除去、クエリパラメータソート等）"""

  def generate_url_hash(url: str) -> str:
      """一意なURLハッシュ生成"""

  def extract_domain(url: str) -> str:
      """URLからドメイン抽出"""

  def is_valid_url(url: str) -> bool:
      """URL妥当性チェック"""
  ```

- [ ] 1.3.2 `utils/retry.py` - リトライ機構

  ```python
  async def exponential_backoff_retry(
      func: Callable,
      max_retries: int,
      base_delay: float,
      max_delay: float,
      exceptions: Tuple[Exception, ...] = (Exception,)
  ):
      """指数バックオフリトライ"""
  ```

- [ ] 1.3.3 `utils/logging.py` - 構造化ログ
  ```python
  def setup_logger(name: str, level: str = "INFO") -> Logger:
      """構造化ログ設定"""

  def log_crawl_event(event_type: str, url: str, **kwargs):
      """クローリングイベントログ"""
  ```

**受け入れ基準:**

- URL 正規化が一貫している
- リトライロジックが動作する
- JSON 形式でログが出力される

## Phase 2: DynamoDB 状態管理実装

### 2.1 DynamoDB 操作基盤

- [ ] 2.1.1 `state/models.py` - DynamoDB モデル定義

  ```python
  class URLStateModel(Model):
      class Meta:
          table_name = "url-states"
          region = "us-east-1"

      url_hash = UnicodeAttribute(hash_key=True)
      url = UnicodeAttribute()
      domain = UnicodeAttribute()
      state = UnicodeAttribute()  # pending/in_progress/done/failed
      crawler_id = UnicodeAttribute(null=True)
      acquired_at = UTCDateTimeAttribute(null=True)
      ttl = UTCDateTimeAttribute()
      last_crawled = UTCDateTimeAttribute(null=True)
      retries = NumberAttribute(default=0)
      error_message = UnicodeAttribute(null=True)

      # GSI for domain queries
      domain_index = DomainIndex()
  ```

- [ ] 2.1.2 DynamoDB クライアント初期化
  - 接続プール設定
  - エラーハンドリング（ThrottlingError 等）
  - ローカル開発用 LocalStack 対応

**受け入れ基準:**

- DynamoDB テーブルに読み書きできる
- 条件付き書き込みが動作する
- LocalStack でテストできる

### 2.2 分散ロック機能

- [ ] 2.2.1 `state/lock_manager.py` 実装

  ```python
  class DistributedLockManager:
      async def try_acquire_url(
          self,
          url_hash: str,
          domain: str,
          crawler_id: str
      ) -> bool:
          """DynamoDB条件付き書き込みによる分散ロック取得"""

      async def release_url(self, url_hash: str, crawler_id: str):
          """ロック解放"""

      async def extend_lock(self, url_hash: str, crawler_id: str) -> bool:
          """ロック期限延長（ハートビート）"""

      async def cleanup_expired_locks(self):
          """期限切れロック清理"""
  ```

- [ ] 2.2.2 ハートビート機構

  - バックグラウンドタスクでロック期限延長
  - 30 秒間隔での heartbeat 送信
  - プロセス異常終了時の自動ロック解放

- [ ] 2.2.3 タイムアウト回復機構
  - TTL 経過した in_progress ステートの検出
  - pending ステートへの自動復旧
  - 統計情報収集

**受け入れ基準:**

- 同じ URL を複数のクローラーが取得できない
- プロセス死亡時にロックが自動解放される
- 期限切れロックが適切に回復される

### 2.3 状態遷移管理

- [ ] 2.3.1 `state/state_manager.py` 実装

  ```python
  class URLStateManager:
      async def update_state(
          self,
          url_hash: str,
          new_state: URLStateEnum,
          crawler_id: str,
          result: Optional[CrawlResult] = None,
          error: Optional[str] = None
      ):
          """状態更新"""

      async def schedule_retry(
          self,
          url_hash: str,
          delay_seconds: int
      ):
          """リトライスケジュール"""

      async def get_pending_urls_for_domain(
          self,
          domain: str,
          limit: int = 100
      ) -> List[str]:
          """ドメイン別pending URL取得"""
  ```

- [ ] 2.3.2 バッチ操作最適化
  - バッチ読み込み・書き込み
  - トランザクション操作
  - DynamoDB キャパシティ効率化

**受け入れ基準:**

- 状態遷移が適切に記録される
- リトライロジックが動作する
- バッチ操作でスループットが向上する

## Phase 3: Redis レート制御実装

### 3.1 Redis 接続・基盤

- [ ] 3.1.1 `rate_limiter/redis_client.py` 実装

  - Redis 接続プール設定
  - フェイルオーバー対応
  - ElastiCache 対応

- [ ] 3.1.2 分散レート制御アルゴリズム
  ```python
  class SlidingWindowRateLimiter:
      async def check_domain_limit(
          self,
          domain: str,
          qps_limit: int
      ) -> bool:
          """Sliding Window Counter によるレート制御"""

      async def record_request(self, domain: str):
          """リクエスト記録"""

      async def get_next_allowed_time(self, domain: str) -> float:
          """次回許可時刻計算"""
  ```

**受け入れ基準:**

- ドメイン別 QPS 制御が機能する
- 複数クローラー間で制御が共有される
- Redis 障害時のフォールバック動作

### 3.2 robots.txt キャッシュ

- [ ] 3.2.1 `rate_limiter/robots_cache.py` 実装
  ```python
  class RobotsCacheManager:
      async def get_robots_parser(self, domain: str) -> Optional[RobotsParser]:
          """robots.txtキャッシュ取得"""

      async def cache_robots_parser(
          self,
          domain: str,
          robots_content: str,
          ttl: int = 3600
      ):
          """robots.txt キャッシュ保存"""
  ```

**受け入れ基準:**

- robots.txt が適切にキャッシュされる
- TTL による自動期限切れ
- パース済み RobotsParser の保存

## Phase 4: HTTP クライアント実装

### 4.1 HTTP 基盤

- [ ] 4.1.1 `http_client/client.py` 実装

  ```python
  class CrawlerHTTPClient:
      def __init__(self, config: CrawlerConfig):
          self.session = aiohttp.ClientSession(
              connector=aiohttp.TCPConnector(
                  limit=config.max_concurrent_requests,
                  limit_per_host=10,
                  ttl_dns_cache=300,
                  use_dns_cache=True
              ),
              timeout=aiohttp.ClientTimeout(total=config.request_timeout)
          )

      async def fetch_url(self, url: str) -> CrawlResult:
          """HTTP取得実行"""

      async def fetch_robots_txt(self, domain: str) -> Optional[str]:
          """robots.txt取得"""
  ```

- [ ] 4.1.2 User-Agent・ヘッダー管理
  - 適切な User-Agent 設定
  - Accept-Language ヘッダー
  - カスタムヘッダー設定機能

**受け入れ基準:**

- HTTP/HTTPS 取得が動作する
- タイムアウト・エラーが適切に処理される
- 接続プールが効率化されている

### 4.2 コンテンツ処理

- [ ] 4.2.1 `http_client/parser.py` - HTML パース

  ```python
  class ContentParser:
      async def extract_text_content(
          self,
          html: str,
          url: str
      ) -> ParsedContent:
          """HTMLからテキスト抽出"""

      async def detect_language(self, text: str) -> str:
          """言語判定"""

      async def extract_metadata(
          self,
          soup: BeautifulSoup,
          url: str
      ) -> Dict[str, Any]:
          """メタデータ抽出（title, description, etc）"""
  ```

- [ ] 4.2.2 言語判定機能
  - langdetect ライブラリ統合
  - 信頼度スコア評価
  - デフォルト言語設定

**受け入れ基準:**

- HTML からクリーンなテキストが抽出される
- 日本語・英語が適切に判定される
- メタデータが構造化される

### 4.3 robots.txt 処理

- [ ] 4.3.1 `http_client/robots.py` 実装
  ```python
  class RobotsChecker:
      async def check_url_allowed(
          self,
          url: str,
          user_agent: str = "*"
      ) -> bool:
          """robots.txt チェック"""

      async def get_crawl_delay(self, domain: str) -> Optional[int]:
          """crawl-delay 取得"""

      async def parse_robots_txt(self, content: str) -> RobotsParser:
          """robots.txt パース"""
  ```

**受け入れ基準:**

- robots.txt 規則が正しく適用される
- crawl-delay が考慮される
- 不正な robots.txt でもエラーにならない

## Phase 5: URL 発見・コーディネーター実装

### 5.1 sitemap.xml 処理

- [ ] 5.1.1 `discovery/sitemap_parser.py` 実装
  ```python
  class SitemapParser:
      async def discover_sitemaps(self, domain: str) -> List[str]:
          """sitemap発見（robots.txt + 既定パス）"""

      async def parse_sitemap_xml(self, sitemap_url: str) -> List[URLInfo]:
          """XMLサイトマップパース"""

      async def parse_sitemap_index(self, sitemap_url: str) -> List[str]:
          """サイトマップインデックスパース"""

      async def extract_urls_recursive(
          self,
          sitemap_urls: List[str],
          max_depth: int = 3
      ) -> List[URLInfo]:
          """再帰的URL抽出"""
  ```

**受け入れ基準:**

- XML sitemap が正しくパースされる
- sitemap index の再帰処理が動作する
- 不正な XML でもエラーにならない

### 5.2 URL 発見コーディネーター

- [ ] 5.2.1 `coordinator/discovery_coordinator.py` 実装

  ```python
  class URLDiscoveryCoordinator:
      async def run(self):
          """メインループ実行"""

      async def process_discovery_queue(self):
          """ドメイン発見キュー処理"""

      async def discover_domain_urls(self, domain: str):
          """ドメイン内URL発見"""
          # 1. robots.txt取得・解析
          # 2. sitemap.xml発見・解析
          # 3. 個別URLをCrawl Queueに投入

      async def enqueue_urls_batch(self, urls: List[str]):
          """URL一括キュー投入"""
  ```

- [ ] 5.2.2 重複 URL 排除機構
  - URL 正規化処理
  - DynamoDB 重複チェック
  - Bloom Filter 活用検討

**受け入れ基準:**

- 新しいドメインの URL 発見が自動化される
- 重複 URL が排除される
- 大量 URL の効率的な処理

### 5.3 SQS キュー管理

- [ ] 5.3.1 `discovery/queue_manager.py` 実装
  ```python
  class SQSQueueManager:
      async def receive_discovery_message(self) -> Optional[DiscoveryMessage]:
          """Discovery Queue メッセージ受信"""

      async def send_crawl_messages(
          self,
          urls: List[str]
      ):
          """Crawl Queue バッチメッセージ送信"""

      async def delete_message(self, receipt_handle: str):
          """処理完了メッセージ削除"""
  ```

**受け入れ基準:**

- SQS メッセージが適切に処理される
- バッチ送信でスループット向上
- エラー時の DLQ 送信

## Phase 6: クローラーワーカー実装

### 6.1 メインワーカーループ

- [ ] 6.1.1 `worker/crawler_worker.py` 実装
  ```python
  class CrawlerWorker:
      async def run(self):
          """ワーカーメインループ"""

      async def process_crawl_queue(self):
          """クロールキュー処理"""
          # 1. SQSメッセージ取得
          # 2. 分散ロック取得試行
          # 3. レート制御チェック
          # 4. robots.txtチェック
          # 5. HTTP取得実行
          # 6. 結果保存・状態更新

      async def crawl_single_url(self, url: str) -> CrawlResult:
          """単一URL クローリング"""
  ```

**受け入れ基準:**

- SQS キューから URL を取得して処理する
- 分散ロック機構が動作する
- エラー時の適切な状態更新

### 6.2 並行処理制御

- [ ] 6.2.1 並行リクエスト制御
  ```python
  class ConcurrentCrawlManager:
      def __init__(self, max_concurrent: int = 10):
          self.semaphore = asyncio.Semaphore(max_concurrent)
          self.active_domains: Dict[str, int] = defaultdict(int)

      async def crawl_with_semaphore(self, url: str) -> CrawlResult:
          """セマフォによる並行制御"""
  ```

**受け入れ基準:**

- 最大同時接続数が制御される
- ドメイン別の並行制御
- リソース効率的な処理

### 6.3 エラーハンドリング・リトライ

- [ ] 6.3.1 `worker/error_handler.py` 実装
  ```python
  class CrawlErrorHandler:
      async def handle_crawl_error(
          self,
          error: Exception,
          url: str,
          retry_count: int
      ):
          """エラー種別による分岐処理"""

      async def should_retry(
          self,
          error: Exception,
          retry_count: int
      ) -> bool:
          """リトライ判定"""

      async def calculate_backoff_delay(
          self,
          retry_count: int,
          base_delay: int = 60
      ) -> int:
          """バックオフ遅延計算"""
  ```

**受け入れ基準:**

- HTTP エラーが適切に分類される
- リトライ可能/不可能が判定される
- 指数バックオフが動作する

## Phase 7: ストレージ連携実装

### 7.1 S3 ストレージ操作

- [ ] 7.1.1 `storage/s3_client.py` 実装
  ```python
  class S3StorageClient:
      async def save_raw_content(
          self,
          url: str,
          content: bytes,
          content_type: str
      ) -> str:
          """生HTMLをS3保存"""

      async def save_parsed_content(
          self,
          url: str,
          parsed_content: ParsedContent
      ) -> str:
          """パース済みコンテンツ保存"""

      async def generate_s3_key(
          self,
          url: str,
          content_type: str = "html"
      ) -> str:
          """S3キー生成（年月日パーティション）"""
  ```

**受け入れ基準:**

- HTML が S3 に保存される
- 効率的なキー構造
- エラー時の適切な処理

### 7.2 データパイプライン統合

- [ ] 7.2.1 インデックスキュー連携
  - S3 保存完了後の SQS メッセージ送信
  - インデクサー向けメッセージ形式
  - エラー時の DLQ 処理

**受け入れ基準:**

- S3 保存後にインデックスキューにメッセージが送信される
- メッセージ形式が正しい

## Phase 8: 監視・ヘルスチェック実装

### 8.1 メトリクス収集

- [ ] 8.1.1 `monitoring/metrics.py` 実装
  ```python
  class CrawlerMetrics:
      async def record_crawl_success(
          self,
          domain: str,
          response_time: float
      ):
          """成功メトリクス記録"""

      async def record_crawl_failure(
          self,
          domain: str,
          error_type: str
      ):
          """失敗メトリクス記録"""

      async def record_queue_depth(
          self,
          queue_name: str,
          depth: int
      ):
          """キュー深度記録"""
  ```

**受け入れ基準:**

- CloudWatch メトリクスが送信される
- カスタムメトリクスが作成される

### 8.2 ヘルスチェック機能

- [ ] 8.2.1 `monitoring/health_checker.py` 実装

  ```python
  class HealthChecker:
      async def check_health(self) -> HealthStatus:
          """総合ヘルスチェック"""

      async def check_dependencies(self) -> Dict[str, bool]:
          """依存サービス確認"""
          # DynamoDB, Redis, SQS, S3 接続確認
  ```

- [ ] 8.2.2 HTTP ヘルスエンドポイント
  ```python
  @app.get("/health")
  async def health_endpoint():
      """K8s用ヘルスチェックエンドポイント"""

  @app.get("/ready")
  async def readiness_endpoint():
      """K8s用レディネスチェック"""
  ```

**受け入れ基準:**

- K8s Liveness/Readiness Probe が動作する
- 依存サービス障害時に適切に Unhealthy になる

### 8.3 構造化ログ出力

- [ ] 8.3.1 ログイベント標準化

  ```python
  # ログイベント例
  logger.info("crawl_started", extra={
      "url": url,
      "domain": domain,
      "crawler_id": crawler_id
  })

  logger.info("crawl_completed", extra={
      "url": url,
      "status_code": response.status,
      "response_time_ms": response_time * 1000,
      "content_length": len(content)
  })
  ```

**受け入れ基準:**

- JSON 形式の構造化ログ
- 検索・分析しやすい形式
- エラー情報の詳細記録

## Phase 9: Kubernetes 統合

### 9.1 Docker コンテナ化

- [ ] 9.1.1 `Dockerfile` 作成

  ```dockerfile
  FROM python:3.11-slim

  WORKDIR /app
  COPY pyproject.toml uv.lock ./
  RUN pip install uv && uv sync --frozen

  COPY app/ ./app/

  # ヘルスチェック用
  HEALTHCHECK --interval=30s --timeout=3s \
    CMD curl -f http://localhost:8080/health || exit 1

  CMD ["python", "-m", "app.crawler.worker"]
  ```

- [ ] 9.1.2 マルチステージビルド最適化
  - ビルド用とランタイム用イメージ分離
  - 依存関係の効率的なキャッシュ
  - セキュリティ強化

**受け入れ基準:**

- Docker イメージがビルドされる
- コンテナが正常に起動する
- ヘルスチェックが動作する

### 9.2 Kubernetes マニフェスト

- [ ] 9.2.1 `k8s/crawler-worker.yaml` 作成

  ```yaml
  apiVersion: apps/v1
  kind: Deployment
  metadata:
    name: crawler-worker
  spec:
    replicas: 3
    selector:
      matchLabels:
        app: crawler-worker
    template:
      spec:
        containers:
          - name: crawler
            image: crawler:latest
            env:
              - name: CRAWLER_ID
                valueFrom:
                  fieldRef:
                    fieldPath: metadata.name
            resources:
              requests:
                memory: "512Mi"
                cpu: "250m"
              limits:
                memory: "1Gi"
                cpu: "500m"
            livenessProbe:
              httpGet:
                path: /health
                port: 8080
            readinessProbe:
              httpGet:
                path: /ready
                port: 8080
  ```

- [ ] 9.2.2 `k8s/crawler-coordinator.yaml` 作成
- [ ] 9.2.3 `k8s/hpa.yaml` - 自動スケーリング設定
- [ ] 9.2.4 `k8s/configmap.yaml` - 設定管理

**受け入れ基準:**

- K8s クラスターにデプロイできる
- HPA による自動スケーリング
- 設定が ConfigMap で管理される

### 9.3 運用機能

- [ ] 9.3.1 Graceful Shutdown 実装

  ```python
  class CrawlerService:
      async def shutdown(self, signal_received=None):
          """グレースフルシャットダウン"""
          # 1. 新しいタスク受付停止
          # 2. 実行中タスクの完了待ち
          # 3. リソース解放
  ```

- [ ] 9.3.2 シグナルハンドリング
  - SIGTERM 受信時の適切な終了処理
  - SIGINT 時のデバッグ情報出力

**受け入れ基準:**

- ポッドが適切に終了する
- 処理中のタスクが中断されない

## Phase 10: テスト実装

### 10.1 単体テスト

- [ ] 10.1.1 コアロジックテスト

  - URL 正規化・ハッシュ生成
  - レート制御ロジック
  - 分散ロック機構
  - エラーハンドリング

- [ ] 10.1.2 HTTP クライアントテスト
  - モックサーバーによる HTTP テスト
  - robots.txt パース テスト
  - コンテンツ抽出テスト

**受け入れ基準:**

- テストカバレッジ 80%以上
- 全単体テストがパスする

### 10.2 統合テスト

- [ ] 10.2.1 LocalStack 統合テスト

  ```python
  @pytest.mark.asyncio
  async def test_full_crawl_pipeline():
      # 1. URL をSQSに投入
      # 2. クローラーが処理
      # 3. DynamoDBに状態記録
      # 4. S3にコンテンツ保存
      # 5. インデックスキューにメッセージ送信
  ```

- [ ] 10.2.2 分散動作テスト
  - 複数クローラーインスタンス起動
  - 同時 URL 処理の競合テスト
  - ロック機構の検証

**受け入れ基準:**

- エンドツーエンドパイプラインが動作する
- 分散環境での競合が正しく処理される

### 10.3 負荷テスト

- [ ] 10.3.1 スループットテスト
  - 1000URL/分の処理能力確認
  - リソース使用量測定
  - ボトルネック特定

**受け入れ基準:**

- 目標スループットを達成する
- メモリリークが発生しない

## Phase 11: 運用・監視統合

### 11.1 CloudWatch 統合

- [ ] 11.1.1 カスタムメトリクス定義

  - `crawler.urls_processed_per_minute`
  - `crawler.success_rate`
  - `crawler.queue_depth`
  - `crawler.lock_contention_rate`

- [ ] 11.1.2 アラート設定
  - エラー率閾値超過
  - キュー深度異常
  - レスポンス時間劣化

**受け入れ基準:**

- メトリクスが CloudWatch に送信される
- アラートが適切に発火する

### 11.2 ダッシュボード作成

- [ ] 11.2.1 運用ダッシュボード設計
  - リアルタイム処理状況
  - エラー率・成功率
  - ドメイン別統計
  - システムリソース使用率

**受け入れ基準:**

- ダッシュボードで運用状況が確認できる
- 問題の早期発見が可能

## 完了条件

### 機能要件

- [ ] 複数のクローラーインスタンスが同時動作する
- [ ] 同一 URL の重複処理が発生しない
- [ ] robots.txt が適切に尊重される
- [ ] ドメイン別レート制御が機能する
- [ ] 障害時の自動復旧が動作する

### 非機能要件

- [ ] 1000URL/分の処理能力
- [ ] 99.9%の可用性
- [ ] 平均レスポンス時間 < 5 秒
- [ ] メモリ使用量 < 1GB per pod

### 運用要件

- [ ] ログによる完全な追跡可能性
- [ ] メトリクスによる監視
- [ ] アラートによる問題通知
- [ ] K8s による自動スケーリング

## 実装優先順位

**高優先度（即座に実装）:**

1. Phase 1: 基盤コンポーネント
2. Phase 2: DynamoDB 状態管理
3. Phase 6: クローラーワーカー（基本版）

**中優先度（次のスプリント）:** 4. Phase 3: Redis レート制御 5. Phase 4: HTTP クライアント 6. Phase 8: 監視・ヘルスチェック

**低優先度（最適化フェーズ）:** 7. Phase 5: URL 発見コーディネーター 8. Phase 9: Kubernetes 統合 9. Phase 10-11: テスト・運用機能

この順序により、早期に MVP（最小機能クローラー）を動作させつつ、段階的に本格運用機能を追加していけます。
