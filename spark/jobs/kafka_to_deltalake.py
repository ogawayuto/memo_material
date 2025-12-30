"""
Kafka to Delta Lake Streaming Job
Reads CDC events from Kafka and writes to Delta Lake on MinIO
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, TimestampType, BooleanType
)
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka and MinIO configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "cdc.public.customers"
DELTA_TABLE_PATH = "s3a://delta-lake/tables/customers"
CHECKPOINT_LOCATION = "s3a://delta-lake/checkpoints/customers"

# Define schema for Debezium CDC payload
cdc_value_schema = StructType([
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
    """Parse Debezium CDC events from Kafka"""
    logger.info("Parsing CDC events...")

    # Parse JSON value from Kafka message
    parsed_df = kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        from_json(col("value").cast("string"), cdc_value_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    )

    # Extract relevant fields
    # For inserts and updates, use 'after' payload
    # For deletes, use 'before' payload
    transformed_df = parsed_df.select(
        col("data.after.id").alias("id"),
        col("data.after.name").alias("name"),
        col("data.after.email").alias("email"),
        (col("data.after.created_at") / 1000).cast("timestamp").alias("created_at"),
        (col("data.after.updated_at") / 1000).cast("timestamp").alias("updated_at"),
        col("data.op").alias("operation"),
        (col("data.ts_ms") / 1000).cast("timestamp").alias("cdc_timestamp"),
        col("kafka_timestamp")
    ).filter(col("data.after").isNotNull())  # Filter out delete events for now

    logger.info("CDC events parsed successfully")
    return transformed_df


def write_to_delta_lake(streaming_df):
    """Write streaming data to Delta Lake"""
    logger.info(f"Writing to Delta Lake: {DELTA_TABLE_PATH}")

    query = streaming_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .option("mergeSchema", "true") \
        .start(DELTA_TABLE_PATH)

    logger.info("Streaming query started successfully")
    logger.info(f"Checkpoint location: {CHECKPOINT_LOCATION}")
    logger.info(f"Delta table location: {DELTA_TABLE_PATH}")

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
