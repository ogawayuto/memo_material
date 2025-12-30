# PostgreSQL → Debezium → Kafka CDC Pipeline

開発/検証用のDockerベースCDCパイプライン環境です。PostgreSQLからDebeziumでCDC（Change Data Capture）を行い、Kafkaにストリーミングします。

PostgreSQLからDebezium→Kafka→Spark→Delta Lakeへの完全なデータパイプラインが構築されています。

## アーキテクチャ

```
[PostgreSQL] → [Debezium CDC] → [Kafka] → [Spark Streaming] → [Delta Lake]
     ↓              ↓               ↓             ↓                   ↓
 [Adminer]    [Kafka Connect]  [Kafka UI]   [Spark UI]            [MinIO]
                                                                      ↓
                                                              [JupyterLab]
```

## コンポーネント（最新バージョン - 2025年12月）

| コンポーネント | バージョン | ポート | 用途 | ステータス |
|--------------|----------|-------|------|----------|
| PostgreSQL | 18.1 | 5432 | ソースDB | ✅ 動作中 |
| Adminer | latest | 8081 | PostgreSQL UI | ✅ 動作中 |
| Apache Kafka | 4.1.1 (KRaft) | 9092 | メッセージブローカー | ✅ 動作中 |
| Kafka UI | latest | 8082 | Kafka管理UI | ✅ 動作中 |
| Debezium | 3.4 (quay.io) | 8083 | CDC実行環境 | ✅ 動作中 |
| MinIO | latest | 9000, 9001 | S3互換ストレージ | ✅ 動作中 |
| Apache Spark | 4.0.1 | 7077, 8080 | ストリーミング処理 | ✅ 動作中 |
| Delta Lake | 4.0.0 | - | ACIDデータレイク | ✅ 動作中 |
| JupyterLab | Spark 4.0.1 | 8888 | データ分析環境 | ✅ 動作中 |

## 必要要件

- Docker Engine 20.10+
- Docker Compose 2.0+
- 4-6GB RAM（Spark統合後は8GB推奨）
- 2コア以上のCPU（Spark統合後は4コア推奨）
- 5GB以上のディスク空き容量

## クイックスタート

### 1. セットアップ

```bash
# プロジェクトディレクトリに移動
cd /path/to/memo_material

# セットアップスクリプトを実行
./scripts/setup.sh
```

### 2. 環境起動

```bash
# 全サービスを起動
./scripts/start.sh
```

起動には約2-3分かかります（初回はイメージダウンロードで更に時間がかかります）。

### 3. 動作確認

```bash
# ヘルスチェック実行
./scripts/health-check.sh
```

### 4. UIアクセス

| サービス | URL | 認証情報 |
|---------|-----|---------|
| Adminer (PostgreSQL UI) | http://localhost:8081 | User: postgres / Pass: postgres / DB: sourcedb |
| Kafka UI | http://localhost:8082 | - |
| Kafka Connect API | http://localhost:8083 | - |
| Spark Master UI | http://localhost:8080 | - |
| Spark Worker UI | http://localhost:8091 | - |
| MinIO Console | http://localhost:9001 | User: minioadmin / Pass: minioadmin |
| JupyterLab | http://localhost:8888 | Token: delta-lake-token |

## 使い方

### PostgreSQLにデータを挿入

Adminer（http://localhost:8081）にアクセスし、以下のSQLを実行：

```sql
INSERT INTO customers (name, email)
VALUES ('New Customer', 'new.customer@example.com');
```

### KafkaでCDCイベントを確認

Kafka UI（http://localhost:8082）で以下を確認：

1. Topics → `cdc.public.customers` を選択
2. Messages タブでCDCイベントを表示

### Sparkストリーミングジョブの管理

**重要**: ストリーミングジョブは同時に1つしか実行できません。

```bash
# ジョブステータス確認
./scripts/manage-streaming-job.sh status

# ジョブ開始
./scripts/manage-streaming-job.sh start

# ジョブ停止
./scripts/manage-streaming-job.sh stop

# ジョブ再起動（停止→チェックポイントクリーンアップ→開始）
./scripts/manage-streaming-job.sh restart

# チェックポイントのみクリーンアップ
./scripts/manage-streaming-job.sh clean
```

Spark Master UI（http://localhost:8080）でジョブの実行状況を確認できます。

### JupyterLabでDelta Lakeクエリ

1. JupyterLab（http://localhost:8888）にアクセス
2. トークン: `delta-lake-token` でログイン
3. `delta_viewer.ipynb` を開く
4. Sparkセッションを起動してDelta Lakeテーブルをクエリ

## データフロー

```
1. PostgreSQL: customersテーブルにINSERT/UPDATE/DELETE
   ↓
2. Debezium: WALからCDCイベントキャプチャ
   ↓
3. Kafka: トピック cdc.public.customers にパブリッシュ ✅
   ↓
4. Spark Streaming: Kafkaトピックからリアルタイム処理 ✅
   ↓
5. Delta Lake: MinIOにACIDトランザクション保存 ✅
   ↓
6. JupyterLab: Delta Lakeテーブルをクエリ・分析 ✅
```

## プロジェクト構造

```
memo_material/
├── docker-compose.yml          # サービス定義
├── .env                        # 環境変数
├── .gitignore                  # Git除外設定
├── README.md                   # このファイル
├── postgres/
│   ├── init.sql               # DB初期化
│   └── postgresql.conf        # PostgreSQL設定
├── debezium/
│   ├── connectors/
│   │   └── postgres-connector.json
│   └── scripts/
│       └── register-connector.sh
├── spark/
│   ├── conf/
│   │   ├── spark-defaults.conf
│   │   └── log4j.properties
│   └── jobs/
│       ├── kafka_to_deltalake.py
│       └── requirements.txt
├── notebooks/
│   ├── delta_viewer.ipynb
│   └── requirements.txt
└── scripts/
    ├── setup.sh
    ├── start.sh
    ├── stop.sh
    ├── health-check.sh
    ├── run-spark-job.sh
    └── manage-streaming-job.sh  # ストリーミングジョブ管理
```

## トラブルシューティング

### サービスが起動しない

```bash
# ログを確認
docker-compose logs <service-name>

# サービスを再起動
docker-compose restart <service-name>
```

### Debeziumコネクタが登録されない

```bash
# コネクタステータス確認
curl http://localhost:8083/connectors/postgres-source-connector/status

# コネクタを再登録
./debezium/scripts/register-connector.sh
```

### Sparkストリーミングジョブの競合エラー

エラー: `Multiple streaming queries are concurrently using s3a://delta-lake/checkpoints/customers/offsets`

**原因**: 複数のストリーミングジョブが同時に実行されています

**解決方法**:
```bash
# 管理スクリプトでジョブを再起動
./scripts/manage-streaming-job.sh restart

# または手動で実行
docker exec spark-master pkill -9 -f "kafka_to_deltalake"
docker run --rm --network el-pipeline-network \
  -e MC_HOST_minio=http://minioadmin:minioadmin@minio:9000 \
  minio/mc rm --recursive --force minio/delta-lake/checkpoints/
./scripts/run-spark-job.sh
```

### Sparkジョブが失敗する

```bash
# Spark Master UIで確認
# http://localhost:8080

# ログ確認
docker logs spark-master
docker logs spark-worker
```

### Delta Lakeが読めない

1. MinIOコンソール（http://localhost:9001）でバケット `delta-lake` が存在するか確認
2. S3A設定が正しいか確認（spark-defaults.conf）
3. Delta Lakeテーブルパスが正しいか確認

## 環境停止

```bash
# 全サービスを停止（データは保持）
./scripts/stop.sh

# 全サービスとデータを削除
docker-compose down -v
```

## セキュリティに関する注意

この環境は**開発/検証専用**です。本番環境では以下を実装してください：

- すべてのデフォルトパスワードを変更
- TLS/SSL暗号化を有効化
- ネットワークセグメンテーション
- シークレット管理ツール（HashiCorp Vault等）
- アクセス制御・認証強化
- 監視・アラート（Prometheus/Grafana）

## 技術仕様

### PostgreSQL CDC設定
- WALレベル: `logical`（CDC必須）
- レプリケーションスロット: 4つ確保
- REPLICA IDENTITY FULL: 全カラム変更追跡

### Kafka設定
- KRaftモード（Zookeeper不要）
- シングルブローカー（開発用）

### Debezium設定
- pgoutputプラグイン（PostgreSQL標準）
- PostgreSQL 18対応
- Kafka 4.1.1ベース

### Spark設定
- スタンドアロンクラスタモード
- Scala 2.13、Java 17/21対応
- Delta Lake 4.0.0統合
- MinIO S3A連携

### Delta Lake設定
- MinIOをS3互換バックエンドとして使用
- ACID保証
- タイムトラベル・バージョニング対応

## バージョン情報

| コンポーネント | バージョン | リリース日 |
|--------------|----------|----------|
| PostgreSQL | 18.1 | 2025-11-13 |
| Apache Kafka | 4.1.1 | 2025-11-12 |
| Debezium | 3.4 (quay.io) | 2025-12-16 |
| Apache Spark | 4.0.1 | 2024-12-19 |
| Delta Lake | 4.0.0 | 2025-06-06 |
| Hadoop AWS | 3.4.1 | 2024-12-19 |

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。

## 参考資料

- [PostgreSQL Documentation](https://www.postgresql.org/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Debezium Documentation](https://debezium.io/documentation/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Delta Lake Documentation](https://docs.delta.io/latest/)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)

## Spark統合について

Spark 4.0.1 + Delta Lake 4.0.0の統合が完了しました。互換性問題の解決履歴とトラブルシューティング情報については以下を参照してください：

📄 **[SPARK_INTEGRATION_HANDOVER.md](SPARK_INTEGRATION_HANDOVER.md)** - Spark統合の実装履歴と技術詳細
