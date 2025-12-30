# Spark統合 引継ぎ資料

## 現在の状態（2025年12月30日）

### ✅ 正常動作しているコンポーネント

以下のコンポーネントは正常に動作確認済みです：

| コンポーネント | バージョン | ステータス | ポート | 用途 |
|--------------|----------|----------|-------|------|
| PostgreSQL | 18-alpine | ✅ 動作中 | 5432 | ソースDB（CDC対応） |
| Adminer | latest | ✅ 動作中 | 8081 | PostgreSQL管理UI |
| Apache Kafka | 4.1.1 (KRaft) | ✅ 動作中 | 9092 | メッセージブローカー |
| Kafka UI | latest | ✅ 動作中 | 8082 | Kafka管理UI |
| Debezium (Kafka Connect) | 3.4 | ✅ 動作中 | 8083 | CDC実行環境 |
| MinIO | latest | ✅ 動作中 | 9000, 9001 | S3互換ストレージ |
| JupyterLab | latest | ✅ 動作中 | 8888 | データ分析環境 |

### ❌ 未実装のコンポーネント

- **Apache Spark**: 複数の互換性問題により未統合（詳細は後述）

## アーキテクチャ

### 現在動作中のパイプライン

```
[PostgreSQL] → [Debezium CDC] → [Kafka] → (Sparkは未統合)
     ↓              ↓               ↓
 [Adminer]    [Kafka Connect]  [Kafka UI]

[MinIO] (Delta Lake用ストレージ準備済み)
   ↓
[JupyterLab]
```

### 動作確認済みの機能

1. **PostgreSQL CDC**
   - WALレベル: logical
   - レプリケーションスロット: 設定済み
   - サンプルテーブル: `customers`（id, name, email, created_at, updated_at）

2. **Debezium CDC**
   - コネクタ: `postgres-source-connector`
   - プラグイン: pgoutput
   - 対象テーブル: `public.customers`
   - トピックプレフィックス: `cdc`

3. **Kafka**
   - トピック: `cdc.public.customers`
   - CDCイベント確認済み（Kafka UIで表示可能）

4. **MinIO**
   - バケット: `delta-lake`（作成済み）
   - エンドポイント: http://minio:9000
   - 認証: minioadmin / minioadmin

## Spark統合で発生した問題

### 試行バージョン履歴

#### 試行1: Spark 4.1.0 + Delta Lake 4.0.0
**結果**: ❌ 失敗

**エラー**:
```
java.lang.NoClassDefFoundError: org/apache/spark/sql/catalyst/expressions/TimeAdd
```

**原因**: Delta Lake 4.0.0はSpark 4.0.x向けに設計されており、Spark 4.1.0の内部API変更に対応していない

**参考**:
- [Delta Lake Releases](https://docs.delta.io/latest/releases.html)
- [Spark 4.1.0 Release Notes](https://spark.apache.org/releases/spark-release-4.1.0.html)

---

#### 試行2: Spark 4.0.1 + Delta Lake 4.0.0 + Hadoop 3.4.1
**結果**: ❌ 失敗

**エラー1**:
```
java.lang.NoSuchMethodError: 'java.lang.Enum org.apache.hadoop.util.ConfigurationHelper.resolveEnum(...)'
```

**原因**: Hadoop 3.4.1のAPIとSparkが期待するAPIの不一致

**対応**: hadoop-aws 3.4.1 → 3.3.6にダウングレード

**エラー2**:
```
For input string: "60s"
```

**原因**: Hadoop S3Aの設定で時間値に"60s"のような文字列形式が使われているが、数値（ミリ秒）を期待している

**対応**: S3A設定を明示的に数値で指定
```properties
spark.hadoop.fs.s3a.connection.request.timeout=60000
spark.hadoop.fs.s3a.threads.keepalivetime=60
```

**参考**: [Apache Iceberg Issue #12557](https://github.com/apache/iceberg/issues/12557)

**エラー3**:
```
java.lang.ClassNotFoundException: Class software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider not found
```

**原因**: Hadoop 3.3.6はAWS SDK v1を使用するが、一部の依存関係でAWS SDK v2のクラスが参照されている

**状態**: 未解決

---

### 試行した依存関係の組み合わせ

| Spark | Delta Lake | Hadoop AWS | AWS SDK | 結果 |
|-------|-----------|------------|---------|------|
| 4.1.0 | 4.0.0 | 3.4.2 | 1.12.367 | ❌ TimeAdd エラー |
| 4.0.1 | 4.0.0 | 3.4.1 | 1.12.367 | ❌ Hadoop API エラー |
| 4.0.1 | 4.0.0 | 3.3.6 | 1.12.367 | ❌ AWS SDK v2 エラー |

### ファイル権限の問題

Sparkコンテナで複数の権限エラーが発生しました：

1. **spark-defaults.conf**: `600` → `644`に変更が必要
2. **log4j.properties**: `600` → `644`に変更が必要
3. **kafka_to_deltalake.py**: `600` → `644`に変更が必要
4. **Spark event log**: `/opt/spark/logs/` の権限問題（イベントログを無効化して回避）

## 今後Sparkを統合する際の推奨事項

### 推奨アプローチ1: Spark 3.5系を使用

最も安定した組み合わせ：

```yaml
SPARK_VERSION=3.5.3
DELTA_VERSION=3.2.1
HADOOP_AWS_VERSION=3.3.4
AWS_SDK_BUNDLE_VERSION=1.12.367
```

**理由**:
- Spark 3.5系は成熟しており、Delta Lake 3.x系のサポートが確立されている
- Hadoop 3.3.4 + AWS SDK v1の組み合わせは実績がある
- MinIOとの互換性が高い

**依存関係**:
```bash
--packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3,\
io.delta:delta-spark_2.13:3.2.1,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.367
```

### 推奨アプローチ2: 最新版を待つ

Spark 4.1対応のDelta Lakeがリリースされるまで待つ：

- Delta Lake公式ドキュメントで互換性マトリクスを定期的に確認
- GitHub [delta-io/delta](https://github.com/delta-io/delta/releases) でリリースを監視

### 推奨アプローチ3: AWS SDK v2への移行

Hadoop 3.4系 + AWS SDK v2の組み合わせを試す：

```bash
--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,\
io.delta:delta-spark_2.13:4.0.0,\
org.apache.hadoop:hadoop-aws:3.4.1,\
software.amazon.awssdk:bundle:2.20.160,\
software.amazon.awssdk:url-connection-client:2.20.160
```

**注意点**:
- AWS SDK v2は設定項目が異なる
- MinIOとの互換性を十分に検証する必要がある

## 作成済みファイル

以下のファイルは作成済みで、適切なバージョンに修正すれば使用可能です：

### Spark設定ファイル

1. **`spark/conf/spark-defaults.conf`**
   - Delta Lake拡張設定
   - MinIO S3A設定（数値形式で修正済み）
   - Kafka設定
   - パフォーマンスチューニング設定

2. **`spark/conf/log4j.properties`**
   - ログレベル設定

### Sparkジョブ

**`spark/jobs/kafka_to_deltalake.py`**

主な機能：
- KafkaからDebezium CDCイベントをストリーミング読み込み
- JSON解析（before/after payload）
- Delta Lakeへの書き込み（append mode）
- チェックポイント管理

**スキーマ定義**:
```python
cdc_value_schema = StructType([
    StructField("before", ...),
    StructField("after", StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType())
    ])),
    StructField("op", StringType()),  # c=create, u=update, d=delete
    StructField("ts_ms", LongType()),
    StructField("source", ...)
])
```

### JupyterLabノートブック

**`notebooks/delta_viewer.ipynb`**

主な機能：
- Delta Lakeテーブルの読み込み
- タイムトラベルクエリ
- 履歴表示
- データ可視化

## 起動・停止方法

### 現在のシステム（Sparkなし）

**起動**:
```bash
./scripts/start.sh
```

**停止**:
```bash
./scripts/stop.sh
```

**完全削除**:
```bash
docker-compose down -v
```

### サービスURL

| サービス | URL | 認証情報 |
|---------|-----|---------|
| Adminer | http://localhost:8081 | postgres / postgres / sourcedb |
| Kafka UI | http://localhost:8082 | なし |
| Kafka Connect | http://localhost:8083 | なし |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| JupyterLab | http://localhost:8888 | トークン: delta-lake-token |

## テストシナリオ

### CDCテスト（動作確認済み）

1. Adminer（http://localhost:8081）でPostgreSQLに接続
2. 以下のSQLを実行：
   ```sql
   INSERT INTO customers (name, email)
   VALUES ('Test User', 'test.user@example.com');
   ```
3. Kafka UI（http://localhost:8082）でトピック`cdc.public.customers`を確認
4. メッセージが表示されることを確認 ✅

### Sparkジョブテスト（未完了）

Spark統合後に実行するテスト：

1. Sparkジョブを実行：
   ```bash
   ./scripts/run-spark-job.sh
   ```

2. MinIO Console（http://localhost:9001）で確認：
   - バケット: `delta-lake`
   - パス: `tables/customers/`
   - Delta Lakeファイル（Parquet + _delta_log）の存在確認

3. JupyterLabで確認：
   ```python
   from pyspark.sql import SparkSession
   from delta import *

   spark = SparkSession.builder \
       .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
       .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
       .getOrCreate()

   df = spark.read.format("delta").load("s3a://delta-lake/tables/customers")
   df.show()
   ```

## 技術的な教訓

### 互換性確認の重要性

- **最新版 ≠ 最適**: 最新版は互換性が確立されていない場合がある
- **公式ドキュメントを確認**: 各コンポーネントの互換性マトリクスを事前に確認する
- **一つ前のメジャーバージョン**: 新機能より安定性を優先する場合は、一つ前のメジャーバージョンを検討

### Hadoop S3A設定

- **時間値は数値で**: `"60s"`ではなく`60000`（ミリ秒）
- **AWS SDKバージョン**: Hadoopのバージョンに応じてAWS SDK v1/v2を選択
- **MinIO互換性**: AWS SDK v1の方がMinIOとの互換性が高い傾向

### ファイル権限

- **Docker bind mount**: ホスト側のファイル権限は`644`（読み取り可能）にする
- **実行スクリプト**: `755`（実行可能）にする
- **セキュリティ**: 開発環境のみの設定（本番環境では適切な権限管理が必要）

## 参考リンク

### 公式ドキュメント

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Delta Lake Documentation](https://docs.delta.io/latest/)
- [Hadoop AWS S3A](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)
- [Debezium Documentation](https://debezium.io/documentation/)

### バージョン情報

- [Delta Lake Releases](https://docs.delta.io/latest/releases.html)
- [Spark Releases](https://spark.apache.org/releases/)
- [Hadoop Releases](https://hadoop.apache.org/releases.html)

### トラブルシューティング

- [Apache Iceberg Issue #12557 - "60s" error](https://github.com/apache/iceberg/issues/12557)
- [Spark Migration Guide](https://spark.apache.org/docs/latest/sql-migration-guide.html)
- [Delta Lake GitHub Issues](https://github.com/delta-io/delta/issues)

## 次のステップ

1. **Spark 3.5系での統合を試す**（最も成功確率が高い）
2. Delta Lake公式ドキュメントでSpark 4.1対応版のリリースを待つ
3. 統合成功後、以下を追加実装：
   - マルチテーブルCDC対応
   - データ変換ロジック
   - Delta Lakeのパーティショニング
   - 監視・アラート機能

## 連絡先・質問

このプロジェクトに関する質問や問題が発生した場合は、以下を参照してください：

- プロジェクトディレクトリ: `/home/ogawa/my_work/memo_material`
- 環境変数ファイル: `.env`
- Docker Compose設定: `docker-compose.yml`
- 起動スクリプト: `scripts/start.sh`

---

**作成日**: 2025年12月30日
**最終更新**: 2025年12月30日
**ステータス**: Spark統合は保留中、CDC → Kafkaまでは動作確認済み
