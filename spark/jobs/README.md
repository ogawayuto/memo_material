# Spark Jobs - Kafka Avro Streaming

PySparkを使ったKafka Avroストリーミング処理のジョブ集。

## 📁 ファイル構成

```
spark/jobs/
├── kafka_to_deltalake.py           # メインストリーミングジョブ
├── avro_deserializer.py            # 汎用Avroデシリアライザーモジュール
├── example_usage.py                # Avroデシリアライザーの使用例
├── requirements.txt                # Python依存関係
├── KAFKA_AVRO_STREAMING_GUIDE.md   # 詳細実装ガイド ⭐
├── AVRO_DESERIALIZER_README.md     # Avroデシリアライザードキュメント
└── README.md                       # このファイル
```

## 🚀 クイックスタート

### 1. ストリーミングジョブの起動

```bash
# ジョブを開始
./scripts/manage-streaming-job.sh start

# ジョブを停止
./scripts/manage-streaming-job.sh stop

# ジョブのステータス確認
./scripts/manage-streaming-job.sh status
```

### 2. ログの確認

```bash
# 処理サマリーを表示（成功率など）
./scripts/check-spark-logs.sh summary

# デシリアライズログを表示
./scripts/check-spark-logs.sh deserialize

# エラーログのみ表示
./scripts/check-spark-logs.sh errors

# リアルタイム監視
./scripts/check-spark-logs.sh live

# ヘルプ表示
./scripts/check-spark-logs.sh help
```

**実行例**：
```bash
$ ./scripts/check-spark-logs.sh summary
=== 処理サマリー ===

バッチ処理統計:
[DESERIALIZE BATCH] Processed 1 records: 1 success, 0 errors
[DESERIALIZE BATCH] Processed 1 records: 1 success, 0 errors
...

成功/エラー集計:
総処理レコード数: 5
成功: 5
エラー: 0
成功率: 100.00%
```

### 3. テストデータの投入

```bash
# PostgreSQLに新規レコードを追加
docker exec postgres psql -U postgres -d sourcedb -c \
  "INSERT INTO customers (name, email) VALUES ('Test User', 'test@example.com');"

# 5-10秒後にDelta Lakeに書き込まれる
```

## 📚 主要ドキュメント

### [KAFKA_AVRO_STREAMING_GUIDE.md](KAFKA_AVRO_STREAMING_GUIDE.md) ⭐ **必読**

詳細な実装ガイド。以下の内容を含みます：

- **処理フロー**: データの流れと各ステップの詳細
- **重要な実装ポイント**: AvroDeserializerの正しい使い方
- **注意点**: よくある間違いとその対処法
- **ログの確認方法**: デバッグとトラブルシューティング
- **パフォーマンス最適化**: チューニング方法

### [AVRO_DESERIALIZER_README.md](AVRO_DESERIALIZER_README.md)

汎用Avroデシリアライザーモジュールのドキュメント：

- 使用パターン（柔軟なMapType、明示的なスキーマ、自動推論）
- エラーハンドリング
- 既存コードとの比較

## 🔑 重要ポイント

### AvroDeserializerの正しい初期化

**✅ 正解**：
```python
deserializer = AvroDeserializer(
    schema_registry_client=client,
    schema_str=None  # ← これが重要！自動でSchema IDから取得
)
```

**❌ 間違い**：
```python
deserializer = AvroDeserializer(
    schema_registry_client=client,
    schema_str=schema.schema_str  # ← これだとエラー
)
```

### Confluent Wire Format

```
Byte 0:     Magic Byte (0x00)
Bytes 1-4:  Schema ID (big-endian)
Bytes 5+:   Avro payload
```

## 🛠️ トラブルシューティング

### NULLレコードが書き込まれる

1. **ログを確認**：
   ```bash
   ./scripts/check-spark-logs.sh errors
   ```

2. **デシリアライズログを確認**：
   ```bash
   ./scripts/check-spark-logs.sh deserialize -n 50
   ```

3. **処理統計を確認**：
   ```bash
   ./scripts/check-spark-logs.sh summary
   ```

### PyArrowエラー

```bash
# requirements.txtにpyarrow>=14.0.0が含まれているか確認
cat spark/jobs/requirements.txt | grep pyarrow

# Dockerイメージを再ビルド
docker-compose build spark-master spark-worker
docker-compose up -d --force-recreate spark-master spark-worker
```

### Schema Registry接続エラー

```bash
# Schema Registryが起動しているか確認
docker ps | grep schema-registry

# Schema Registryに接続できるか確認
curl http://localhost:8085/subjects

# Spark内部から接続確認
docker exec spark-master curl http://schema-registry:8081/subjects
```

## 📊 処理確認

### Delta Lakeテーブルの内容確認

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

df = spark.read.format("delta").load("s3a://delta-lake/tables/customers")
df.orderBy("kafka_timestamp", ascending=False).show(10, truncate=False)
```

## 🔍 デバッグモード

より詳細なログが必要な場合は、`kafka_to_deltalake.py`の`parse_confluent_avro`関数内にログを追加：

```python
import sys

def parse_confluent_avro(binary_data, topic="cdc.public.customers"):
    # 詳細ログ
    sys.stderr.write(f"[DEBUG] Input: {len(binary_data)} bytes\n")
    sys.stderr.write(f"[DEBUG] First 20 bytes: {binary_data[:20].hex()}\n")
    sys.stderr.flush()

    # ... 処理続行
```

## 📈 パフォーマンス

### 現在の設定

- **バッチ間隔**: 5秒
- **チェックポイント**: S3 (MinIO)
- **Executor**: 2 cores, 2GB RAM

### チューニング例

```python
# より大きなバッチでスループット向上
spark.conf.set("spark.sql.streaming.kafka.maxOffsetsPerTrigger", 1000)

# メモリ増量
spark.conf.set("spark.executor.memory", "4g")
```

## 🔗 関連リンク

- [Confluent Kafka Python Docs](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Schema Registry API](https://docs.confluent.io/platform/current/schema-registry/develop/api.html)
- [PySpark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Delta Lake Documentation](https://delta.io/)

## ⚡ Tips

### ログファイルの場所

```bash
# Executor logs
/opt/spark/work/app-*/0/stderr
/opt/spark/work/app-*/0/stdout

# 最新のアプリケーションIDを確認
docker logs spark-master 2>&1 | grep "Registered app" | tail -1
```

### よく使うコマンド

```bash
# Kafkaトピック確認
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Schema Registry subjects確認
curl http://localhost:8085/subjects | jq

# MinIO (Delta Lake) 確認
# ブラウザで http://localhost:9001 にアクセス
# ユーザー名: minioadmin / パスワード: minioadmin
```

## 📝 ライセンス

MIT License
