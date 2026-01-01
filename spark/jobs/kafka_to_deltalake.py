"""
Kafka to Delta Lake Streaming Job
Reads CDC events from Kafka and writes to Delta Lake on MinIO
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, to_timestamp, pandas_udf
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, TimestampType, BooleanType
)
import logging
import pandas as pd
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka and MinIO configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "cdc.public.customers"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
DELTA_TABLE_PATH = "s3a://delta-lake/tables/customers"
CHECKPOINT_LOCATION = "s3a://delta-lake/checkpoints/customers"

# Global cache for Schema Registry clients and deserializers (per process)
_sr_client = None
_deserializer = None


def get_schema_registry_client():
    """Get or create Schema Registry client"""
    global _sr_client
    if _sr_client is None:
        _sr_client = SchemaRegistryClient({
            'url': SCHEMA_REGISTRY_URL
        })
    return _sr_client


def get_avro_deserializer():
    """
    Get or create AvroDeserializer

    With schema_str=None, it automatically extracts Schema ID from
    Confluent Wire Format and fetches schema from Schema Registry.
    """
    global _deserializer
    if _deserializer is None:
        client = get_schema_registry_client()
        _deserializer = AvroDeserializer(
            schema_registry_client=client,
            schema_str=None  # Auto-fetch schema from registry based on Schema ID
        )
    return _deserializer


def parse_confluent_avro(binary_data, topic="cdc.public.customers"):
    """
    Parse Confluent Wire Format Avro message using AvroDeserializer

    Args:
        binary_data: Raw Kafka message value (bytes) in Confluent Wire Format
        topic: Kafka topic name (used for SerializationContext)

    Returns:
        dict: Deserialized Avro payload

    Raises:
        ValueError: If data is malformed
    """
    if not binary_data or len(binary_data) < 5:
        raise ValueError(f"Message too short: {len(binary_data) if binary_data else 0} bytes")

    # Get deserializer (cached per executor)
    deserializer = get_avro_deserializer()

    # Create SerializationContext for VALUE field
    ctx = SerializationContext(topic, MessageField.VALUE)

    # Deserialize Confluent Wire Format (returns Python dict)
    # AvroDeserializer automatically:
    # 1. Validates magic byte (0x00)
    # 2. Extracts Schema ID from bytes 1-4
    # 3. Fetches schema from Schema Registry (with caching)
    # 4. Deserializes Avro payload
    deserialized = deserializer(binary_data, ctx)

    return deserialized


# Define output schema for Avro deserialization (Debezium envelope)
avro_output_schema = StructType([
    StructField("before", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("created_at", LongType(), True),
        StructField("updated_at", LongType(), True)
    ]), True),
    StructField("after", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("created_at", LongType(), True),
        StructField("updated_at", LongType(), True)
    ]), True),
    StructField("op", StringType(), True),  # c=create, u=update, d=delete
    StructField("ts_ms", LongType(), True),
    StructField("source", StructType([
        StructField("version", StringType(), True),
        StructField("connector", StringType(), True),
        StructField("name", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("db", StringType(), True),
        StructField("schema", StringType(), True),
        StructField("table", StringType(), True)
    ]), True)
])


@pandas_udf(avro_output_schema)
def deserialize_avro_udf(binary_series: pd.Series) -> pd.DataFrame:
    """
    Vectorized Avro deserialization UDF

    Args:
        binary_series: Pandas Series of binary Kafka values

    Returns:
        DataFrame with deserialized Debezium envelope
    """
    results = []
    errors = []

    for idx, binary_data in enumerate(binary_series):
        try:
            if binary_data is None or len(binary_data) == 0:
                # Handle null/empty messages
                results.append({
                    "before": None,
                    "after": None,
                    "op": None,
                    "ts_ms": None,
                    "source": None
                })
                continue

            # Debug: Log first record in batch
            if idx == 0:
                first_bytes_hex = binary_data[:20].hex() if hasattr(binary_data, 'hex') else bytes(binary_data[:20]).hex()
                import sys
                sys.stdout.write(f"[DESERIALIZE START] len={len(binary_data)}, first_20_bytes={first_bytes_hex}\n")
                sys.stdout.flush()

            # Deserialize Avro
            payload = parse_confluent_avro(binary_data)

            # Debug: Log successful deserialization
            if idx == 0:
                import sys
                sys.stdout.write(f"[DESERIALIZE SUCCESS] payload keys: {list(payload.keys())}\n")
                sys.stdout.write(f"[DESERIALIZE SUCCESS] op={payload.get('op')}, after={payload.get('after')}\n")
                sys.stdout.flush()

            # Extract Debezium envelope fields
            results.append({
                "before": payload.get("before"),
                "after": payload.get("after"),
                "op": payload.get("op"),
                "ts_ms": payload.get("ts_ms"),
                "source": payload.get("source")
            })

        except Exception as e:
            # Log error but continue processing batch
            import traceback
            import sys
            hex_preview = binary_data[:20].hex() if binary_data and len(binary_data) > 0 else "empty"
            error_msg = f"Row {idx}: {type(e).__name__}: {str(e)} | First 20 bytes: {hex_preview}"
            errors.append(error_msg)

            # Write to stderr for immediate visibility
            sys.stderr.write(f"[DESERIALIZE ERROR] {error_msg}\n")
            sys.stderr.write(f"[DESERIALIZE ERROR] Traceback:\n{traceback.format_exc()}\n")
            sys.stderr.flush()

            # Append null record
            results.append({
                "before": None,
                "after": None,
                "op": None,
                "ts_ms": None,
                "source": None
            })

    # Log batch summary
    import sys
    total = len(binary_series)
    success = total - len(errors)
    sys.stderr.write(f"[DESERIALIZE BATCH] Processed {total} records: {success} success, {len(errors)} errors\n")
    sys.stderr.flush()

    if errors:
        sys.stderr.write(f"[DESERIALIZE BATCH] First 3 errors: {errors[:3]}\n")
        sys.stderr.flush()

    # Convert to DataFrame
    return pd.DataFrame(results)


def create_spark_session():
    """Create and configure Spark session for Delta Lake"""
    logger.info("Creating Spark session with Delta Lake...")

    spark = SparkSession.builder \
        .appName("KafkaToDeltaLake") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created successfully with Delta Lake support")
    return spark


def read_from_kafka(spark):
    """Read streaming data from Kafka"""
    logger.info(f"Reading from Kafka topic: {KAFKA_TOPIC}")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    logger.info("Kafka stream initialized")
    return df


def parse_cdc_events(kafka_df):
    """Parse Debezium CDC events from Kafka using Avro deserialization"""
    logger.info("Deserializing Avro CDC events from Schema Registry...")

    # Apply Avro deserialization UDF to binary Kafka value
    parsed_df = kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        deserialize_avro_udf(col("value")).alias("envelope"),
        col("timestamp").alias("kafka_timestamp")
    )

    # Extract relevant fields from payload
    # For inserts (c) and updates (u), use 'after' payload
    # For deletes (d), use 'before' payload to get the ID
    from pyspark.sql.functions import when, coalesce

    transformed_df = parsed_df.select(
        # For DELETE, get id from before; otherwise from after
        coalesce(
            col("envelope.after.id"),
            col("envelope.before.id")
        ).alias("id"),
        col("envelope.after.name").alias("name"),
        col("envelope.after.email").alias("email"),
        (col("envelope.after.created_at") / 1000000).cast("timestamp").alias("created_at"),
        (col("envelope.after.updated_at") / 1000000).cast("timestamp").alias("updated_at"),
        col("envelope.op").alias("operation"),
        (col("envelope.ts_ms") / 1000).cast("timestamp").alias("cdc_timestamp"),
        col("kafka_timestamp")
    )

    logger.info("Avro CDC events deserialized successfully")
    return transformed_df


def process_batch(batch_df, batch_id):
    """Process each micro-batch with logging"""
    count = batch_df.count()
    print(f"[BATCH {batch_id}] Processing {count} records", flush=True)
    logger.info(f"=== Batch {batch_id}: Processing {count} records ===")

    if count > 0:
        # Show sample data
        print(f"[BATCH {batch_id}] Sample data:", flush=True)
        batch_df.show(5, truncate=False)

        # Debug: Check for NULL values
        null_count = batch_df.filter(col("id").isNull()).count()
        print(f"[BATCH {batch_id}] NULL id count: {null_count} out of {count}", flush=True)

        # Debug: Show first record details
        first_record = batch_df.first()
        if first_record:
            print(f"[BATCH {batch_id}] First record: {first_record}", flush=True)

        # Write to Delta Lake
        batch_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(DELTA_TABLE_PATH)

        print(f"[BATCH {batch_id}] Successfully wrote {count} records to Delta Lake", flush=True)
        logger.info(f"=== Batch {batch_id}: Successfully wrote {count} records to Delta Lake ===")
    else:
        print(f"[BATCH {batch_id}] No new data", flush=True)
        logger.info(f"=== Batch {batch_id}: No new data ===")


def write_to_delta_lake(streaming_df):
    """Write streaming data to Delta Lake"""
    logger.info(f"Writing to Delta Lake: {DELTA_TABLE_PATH}")

    query = streaming_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .trigger(processingTime='5 seconds') \
        .start()

    logger.info("Streaming query started successfully")
    logger.info(f"Checkpoint location: {CHECKPOINT_LOCATION}")
    logger.info(f"Delta table location: {DELTA_TABLE_PATH}")
    logger.info("Trigger: Every 5 seconds")

    return query


def main():
    """Main execution function"""
    try:
        logger.info("=" * 80)
        logger.info("Starting Kafka to Delta Lake Streaming Job")
        logger.info("=" * 80)

        # Create Spark session
        spark = create_spark_session()

        # Read from Kafka
        kafka_df = read_from_kafka(spark)

        # Parse CDC events
        transformed_df = parse_cdc_events(kafka_df)

        # Write to Delta Lake
        query = write_to_delta_lake(transformed_df)

        # Wait for termination
        logger.info("Streaming job is running. Press Ctrl+C to stop.")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Error in streaming job: {str(e)}", exc_info=True)
        raise
    finally:
        logger.info("Streaming job terminated")


if __name__ == "__main__":
    main()
