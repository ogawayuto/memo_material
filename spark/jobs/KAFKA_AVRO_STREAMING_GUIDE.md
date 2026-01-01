# Kafka Avro Streaming with Schema Registry - 実装ガイド

PySparkでConfluent Schema Registryを使ったKafka Avroストリーミング処理の実装ガイド。

## 目次

1. [処理フロー](#処理フロー)
2. [重要な実装ポイント](#重要な実装ポイント)
3. [注意点とトラブルシューティング](#注意点とトラブルシューティング)
4. [ログの確認方法](#ログの確認方法)
5. [パフォーマンス最適化](#パフォーマンス最適化)

---

## 処理フロー

### 1. 全体アーキテクチャ

```
PostgreSQL (Debezium)
    ↓ CDC Events
Kafka (Confluent Wire Format: 0x00 + Schema ID + Avro Payload)
    ↓
PySpark Streaming (pandas_udf)
    ↓ AvroDeserializer (schema_str=None)
Schema Registry (自動スキーマ取得)
    ↓
Delta Lake (MinIO/S3)
```

### 2. データフォーマット

#### Confluent Wire Format

```
Byte 0:     Magic Byte (0x00)
Bytes 1-4:  Schema ID (big-endian 4-byte unsigned int)
Bytes 5+:   Avro-encoded payload
```

**例**：
```
00 00 00 00 02 00 02 0c 14 44 65 62 75 67 20 54 65 73 74...
│  └────┬────┘ └──────────────┬──────────────────────────┘
│   Schema ID=2         Avro Payload
Magic Byte
```

### 3. 処理ステップ

```python
# Step 1: Kafkaからバイナリデータ読み取り
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "cdc.public.customers") \
    .load()

# Step 2: pandas_udfでAvroデシリアライズ
parsed_df = kafka_df.select(
    deserialize_avro_udf(col("value")).alias("envelope")
)

# Step 3: Debeziumエンベロープから実データ抽出
transformed_df = parsed_df.select(
    col("envelope.after.id").alias("id"),
    col("envelope.after.name").alias("name"),
    col("envelope.op").alias("operation")
)

# Step 4: Delta Lakeに書き込み
query = transformed_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .start(DELTA_TABLE_PATH)
```

---

## 重要な実装ポイント

### 1. AvroDeserializerの正しい初期化

**❌ 間違い**：
```python
# schema_strを指定すると、Confluent Wire Formatを正しく処理できない
deserializer = AvroDeserializer(
    schema_registry_client=client,
    schema_str=schema.schema_str  # ← これが原因でエラー
)
```

**✅ 正解**：
```python
# schema_str=None で自動的にSchema IDからスキーマを取得
deserializer = AvroDeserializer(
    schema_registry_client=client,
    schema_str=None  # ← 重要！
)
```

### 2. SerializationContextの使用

```python
from confluent_kafka.serialization import SerializationContext, MessageField

# Kafkaトピック名とフィールド種別を指定
ctx = SerializationContext("cdc.public.customers", MessageField.VALUE)

# デシリアライズ実行
deserialized = deserializer(binary_data, ctx)
```

### 3. Executorごとのキャッシング

```python
# グローバル変数でExecutorプロセスごとにキャッシュ
_sr_client = None
_deserializer = None

def get_avro_deserializer():
    """
    Executorプロセス内で一度だけ初期化
    複数タスクで再利用される
    """
    global _deserializer
    if _deserializer is None:
        client = get_schema_registry_client()
        _deserializer = AvroDeserializer(
            schema_registry_client=client,
            schema_str=None
        )
    return _deserializer
```

**利点**：
- Schema Registryへの不要なリクエストを削減
- スキーマキャッシングによる高速化
- 複数バッチ間でデシリアライザーを再利用

### 4. pandas_udfでのエラーハンドリング

```python
@pandas_udf(avro_output_schema)
def deserialize_avro_udf(binary_series: pd.Series) -> pd.DataFrame:
    results = []
    errors = []

    for idx, binary_data in enumerate(binary_series):
        try:
            # デシリアライズ処理
            payload = parse_confluent_avro(binary_data)
            results.append({
                "before": payload.get("before"),
                "after": payload.get("after"),
                "op": payload.get("op"),
                "ts_ms": payload.get("ts_ms"),
                "source": payload.get("source")
            })
        except Exception as e:
            # エラー時はNULLレコードを返す
            errors.append(str(e))
            results.append({
                "before": None,
                "after": None,
                "op": None,
                "ts_ms": None,
                "source": None
            })

    # エラーログ出力
    if errors:
        import sys
        sys.stderr.write(f"Errors: {len(errors)}/{len(binary_series)}\n")

    return pd.DataFrame(results)
```

---

## 注意点とトラブルシューティング

### 注意点1: PyArrowのインストール必須

**症状**：
```
PyArrow >= 11.0.0 must be installed
```

**解決策**：
```bash
# requirements.txtに追加
pyarrow>=14.0.0

# Dockerイメージを再ビルド
docker-compose build spark-master spark-worker
docker-compose up -d --force-recreate spark-master spark-worker
```

### 注意点2: Schema Registry URLの設定

**重要**：Docker内部ネットワークのURLを使用

```python
# ❌ 間違い（ホストからのURL）
SCHEMA_REGISTRY_URL = "http://localhost:8085"

# ✅ 正解（Docker内部ネットワーク）
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
```

### 注意点3: バイナリデータ型の扱い

pandas_udf内では、Sparkの`BinaryType`がPandas Series内で以下のいずれかになる：
- `bytes` オブジェクト
- `bytearray` オブジェクト

**対処法**：
```python
# 両方に対応
first_bytes_hex = (
    binary_data[:20].hex()
    if hasattr(binary_data, 'hex')
    else bytes(binary_data[:20]).hex()
)
```

### 注意点4: NULLレコードの原因

NULLレコードがDelta Lakeに書き込まれる場合、以下を確認：

1. **デシリアライズエラー** → ログ確認
2. **スキーマ不一致** → Schema Registry確認
3. **ネットワークエラー** → Schema Registryへの接続確認

---

## ログの確認方法

### 1. リアルタイムログ監視

#### 方法A: ストリーミングジョブのログ

```bash
# ジョブ起動時のログを表示（自動的に表示される）
./scripts/manage-streaming-job.sh start

# または手動でMasterログを監視
docker logs -f spark-master
```

#### 方法B: Executor のログをリアルタイム監視

```bash
# Workerコンテナ内の最新Executorログを監視
docker exec spark-worker bash -c '
  while true; do
    latest_stderr=$(find /opt/spark/work -name "stderr" -type f -mmin -1 2>/dev/null | sort | tail -1)
    if [ -n "$latest_stderr" ]; then
      tail -f "$latest_stderr"
    fi
    sleep 5
  done
'
```

### 2. デシリアライズログの確認

#### 成功時のログパターン

```
[DESERIALIZE START] len=200, first_20_bytes=000000000200020c14446562756720546573741c
[DESERIALIZE SUCCESS] payload keys: ['before', 'after', 'source', 'transaction', 'op', 'ts_ms', 'ts_us', 'ts_ns']
[DESERIALIZE SUCCESS] op=c, after={'id': 6, 'name': 'Debug Test', 'email': 'debug@test.com', ...}
[DESERIALIZE BATCH] Processed 1 records: 1 success, 0 errors
```

#### エラー時のログパターン

```
[DESERIALIZE ERROR] Row 0: ValueError: Invalid magic byte: 0x?? | First 20 bytes: ...
[DESERIALIZE ERROR] Traceback:
  File "...", line ..., in parse_confluent_avro
    raise ValueError(...)
[DESERIALIZE BATCH] Processed 1 records: 0 success, 1 errors
```

### 3. ログ検索コマンド集

#### 最近のデシリアライズエラーを確認

```bash
docker exec spark-worker bash -c '
  find /opt/spark/work -name "stderr" -type f -mmin -10 2>/dev/null | \
  xargs grep -h "\[DESERIALIZE ERROR\]" 2>/dev/null | \
  tail -20
'
```

#### バッチ処理サマリーを確認

```bash
docker exec spark-worker bash -c '
  find /opt/spark/work -name "stderr" -type f -mmin -10 2>/dev/null | \
  xargs grep -h "\[DESERIALIZE BATCH\]" 2>/dev/null | \
  tail -10
'
```

#### 特定のアプリケーションIDのログを確認

```bash
# アプリケーションIDを確認
docker logs spark-master 2>&1 | grep "Registered app" | tail -1

# 例: app-20260101081204-0002 のログ
docker exec spark-worker tail -100 /opt/spark/work/app-20260101081204-0002/0/stderr
```

### 4. Spark UI でのログ確認

```bash
# Spark Master UI
http://localhost:8080

# Executorのログを確認：
# 1. Master UIでアプリケーションをクリック
# 2. "Executors" タブをクリック
# 3. "stderr" または "stdout" リンクをクリック
```

### 5. デバッグ用の詳細ログ追加

より詳細なログが必要な場合：

```python
# parse_confluent_avro関数に追加
import sys

def parse_confluent_avro(binary_data, topic="cdc.public.customers"):
    # 詳細ログ出力
    sys.stderr.write(f"[DEBUG] Input length: {len(binary_data)}\n")
    sys.stderr.write(f"[DEBUG] First 10 bytes: {binary_data[:10].hex()}\n")

    # Magic byte確認
    magic = binary_data[0]
    sys.stderr.write(f"[DEBUG] Magic byte: 0x{magic:02x}\n")

    # Schema ID確認
    import struct
    schema_id = struct.unpack('>I', binary_data[1:5])[0]
    sys.stderr.write(f"[DEBUG] Schema ID: {schema_id}\n")
    sys.stderr.flush()

    # デシリアライズ実行...
```

---

## パフォーマンス最適化

### 1. バッチサイズの調整

```python
# より大きなバッチでスループット向上
spark.conf.set("spark.sql.streaming.kafka.maxOffsetsPerTrigger", 1000)
```

### 2. Executorリソースの調整

```python
spark = SparkSession.builder \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()
```

### 3. Schema Registryキャッシングの最適化

デシリアライザーは既に以下をキャッシュしています：
- Schema Registry クライアント（Executorごと）
- AvroDeserializerインスタンス（Executorごと）
- スキーマ定義（confluent-kafka内部でキャッシュ）

追加の最適化は通常不要です。

### 4. チェックポイントの定期クリーンアップ

```bash
# 古いチェックポイントを削除（本番環境では注意）
docker exec spark-master rm -rf /tmp/checkpoint-old-*
```

---

## トラブルシューティングチェックリスト

### ✅ デシリアライズが失敗する

- [ ] PyArrowがインストールされているか？
- [ ] Schema Registry URLが正しいか？（Docker内部URL）
- [ ] Schema RegistryにスキーマIDが存在するか？
  ```bash
  curl http://localhost:8085/subjects
  curl http://localhost:8085/subjects/cdc.public.customers-value/versions/latest
  ```
- [ ] KafkaメッセージがConfluent Wire Formatか？
  ```bash
  # 先頭バイトが 0x00 であることを確認
  ```

### ✅ NULLレコードが書き込まれる

- [ ] Executorログでエラーを確認
  ```bash
  docker exec spark-worker bash -c 'find /opt/spark/work -name "stderr" | xargs grep ERROR'
  ```
- [ ] デシリアライズログでエラー件数を確認
  ```bash
  grep "\[DESERIALIZE BATCH\]" stderr
  ```

### ✅ パフォーマンスが遅い

- [ ] バッチサイズは適切か？
- [ ] Executorリソースは十分か？
- [ ] Schema Registry への接続は安定しているか？
- [ ] ネットワーク遅延はないか？

---

## 付録: よく使うコマンド

### Delta Lakeテーブルの確認

```python
# Pythonスクリプト
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
print(f"Total: {df.count()}")
df.orderBy("kafka_timestamp", ascending=False).show(5, truncate=False)
```

### Kafkaメッセージの確認

```bash
# トピック一覧
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# メッセージ数確認
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic cdc.public.customers
```

### Schema Registryの確認

```bash
# 登録済みSubjects
curl http://localhost:8085/subjects | jq

# 特定Subjectの最新スキーマ
curl http://localhost:8085/subjects/cdc.public.customers-value/versions/latest | jq

# Schema IDで取得
curl http://localhost:8085/schemas/ids/2 | jq
```

---

## まとめ

### 成功のポイント

1. ✅ **`AvroDeserializer(schema_str=None)`** - 自動スキーマ取得
2. ✅ **Executorごとのキャッシング** - パフォーマンス最適化
3. ✅ **適切なエラーハンドリング** - 詳細なログ出力
4. ✅ **Docker内部ネットワーク** - 正しいURL設定
5. ✅ **PyArrowのインストール** - pandas_udf必須

### 参考リンク

- [Confluent Kafka Python](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Schema Registry API](https://docs.confluent.io/platform/current/schema-registry/develop/api.html)
- [PySpark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Delta Lake](https://delta.io/)
