"""
Example: Using Generic Avro Deserializer with Kafka Streaming

This example demonstrates how to use the avro_deserializer module
for various Kafka streaming scenarios.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
from avro_deserializer import create_avro_deserializer, create_flexible_deserializer, infer_spark_schema


def example1_flexible_output():
    """
    Example 1: Flexible output with MapType (recommended for unknown schemas)

    Use this when:
    - You don't know the Avro schema upfront
    - Schema might evolve over time
    - You want maximum flexibility
    """
    spark = SparkSession.builder.appName("FlexibleAvroExample").getOrCreate()

    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "my-topic") \
        .option("startingOffsets", "earliest") \
        .load()

    # Create flexible deserializer
    deserialize = create_flexible_deserializer(
        schema_registry_url="http://schema-registry:8081"
    )

    # Deserialize and expand map fields
    parsed_df = kafka_df.select(
        col("key").cast("string").alias("key"),
        deserialize(col("value")).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select(
        "key",
        "data.*",  # Expand all map fields
        "kafka_timestamp"
    )

    # Write to console for debugging
    query = parsed_df.writeStream \
        .format("console") \
        .option("truncate", False) \
        .start()

    return query


def example2_specific_schema():
    """
    Example 2: Specific output schema for known structure

    Use this when:
    - You know the exact Avro schema
    - You want type safety
    - Schema is stable and won't change frequently
    """
    spark = SparkSession.builder.appName("SpecificSchemaExample").getOrCreate()

    # Define expected schema
    output_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("created_at", LongType(), True)
    ])

    # Create deserializer with specific schema
    deserialize = create_avro_deserializer(
        schema_registry_url="http://schema-registry:8081",
        output_schema=output_schema,
        error_handling='null'
    )

    # Read and deserialize
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "users-topic") \
        .load()

    parsed_df = kafka_df.select(
        deserialize(col("value")).alias("user")
    ).select("user.*")

    return parsed_df


def example3_inferred_schema():
    """
    Example 3: Infer schema from Schema Registry

    Use this when:
    - Schema is registered in Schema Registry
    - You want type safety without manually defining schema
    - You know the schema ID in advance
    """
    spark = SparkSession.builder.appName("InferredSchemaExample").getOrCreate()

    SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
    KNOWN_SCHEMA_ID = 1  # Get this from Schema Registry UI or API

    # Infer schema from Schema Registry
    output_schema = infer_spark_schema(SCHEMA_REGISTRY_URL, KNOWN_SCHEMA_ID)

    # Create deserializer with inferred schema
    deserialize = create_avro_deserializer(
        schema_registry_url=SCHEMA_REGISTRY_URL,
        output_schema=output_schema
    )

    # Read and deserialize
    kafka_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "events-topic") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    parsed_df = kafka_df.select(
        col("key").cast("string").alias("event_key"),
        deserialize(col("value")).alias("event")
    ).select("event_key", "event.*")

    return parsed_df


def example4_debezium_cdc():
    """
    Example 4: Debezium CDC events (like your current kafka_to_deltalake.py)

    Use this for:
    - Change Data Capture (CDC) events
    - Debezium format with before/after payloads
    """
    from pyspark.sql.functions import when, coalesce

    spark = SparkSession.builder.appName("DebeziumCDCExample").getOrCreate()

    # Debezium envelope schema
    debezium_schema = StructType([
        StructField("before", StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True)
        ]), True),
        StructField("after", StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True)
        ]), True),
        StructField("op", StringType(), True),  # c, u, d, r
        StructField("ts_ms", LongType(), True)
    ])

    deserialize = create_avro_deserializer(
        schema_registry_url="http://schema-registry:8081",
        output_schema=debezium_schema
    )

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "cdc.public.customers") \
        .load()

    # Deserialize Debezium envelope
    parsed_df = kafka_df.select(
        deserialize(col("value")).alias("envelope")
    )

    # Extract data (for DELETE, use before; otherwise use after)
    transformed_df = parsed_df.select(
        coalesce(
            col("envelope.after.id"),
            col("envelope.before.id")
        ).alias("id"),
        col("envelope.after.name").alias("name"),
        col("envelope.after.email").alias("email"),
        col("envelope.op").alias("operation"),
        (col("envelope.ts_ms") / 1000).cast("timestamp").alias("event_time")
    )

    return transformed_df


def example5_error_handling():
    """
    Example 5: Different error handling strategies
    """
    spark = SparkSession.builder.appName("ErrorHandlingExample").getOrCreate()

    # Strategy 1: Return null for failed records (default)
    deserialize_null = create_avro_deserializer(
        schema_registry_url="http://schema-registry:8081",
        error_handling='null'
    )

    # Strategy 2: Skip failed records entirely
    deserialize_skip = create_avro_deserializer(
        schema_registry_url="http://schema-registry:8081",
        error_handling='skip'
    )

    # Strategy 3: Raise exception on first error
    deserialize_raise = create_avro_deserializer(
        schema_registry_url="http://schema-registry:8081",
        error_handling='raise'
    )

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "my-topic") \
        .load()

    # Use appropriate strategy based on requirements
    parsed_df = kafka_df.select(
        deserialize_null(col("value")).alias("data")
    ).filter(col("data").isNotNull())  # Filter out failed records

    return parsed_df


def example6_batch_processing():
    """
    Example 6: Batch processing (non-streaming)
    """
    spark = SparkSession.builder.appName("BatchAvroExample").getOrCreate()

    deserialize = create_flexible_deserializer(
        schema_registry_url="http://schema-registry:8081"
    )

    # Read specific offset range
    kafka_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "analytics-events") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    # Deserialize and process
    parsed_df = kafka_df.select(
        col("partition"),
        col("offset"),
        deserialize(col("value")).alias("event")
    ).select("partition", "offset", "event.*")

    # Write to Parquet
    parsed_df.write \
        .mode("overwrite") \
        .parquet("s3a://my-bucket/processed-events")

    return parsed_df


if __name__ == "__main__":
    """
    Run examples
    """
    import sys

    if len(sys.argv) < 2:
        print("Usage: spark-submit example_usage.py <example_number>")
        print("\nAvailable examples:")
        print("  1 - Flexible output (recommended)")
        print("  2 - Specific schema")
        print("  3 - Inferred schema from Schema Registry")
        print("  4 - Debezium CDC events")
        print("  5 - Error handling strategies")
        print("  6 - Batch processing")
        sys.exit(1)

    example_num = sys.argv[1]

    if example_num == "1":
        query = example1_flexible_output()
        query.awaitTermination()
    elif example_num == "2":
        df = example2_specific_schema()
        df.writeStream.format("console").start().awaitTermination()
    elif example_num == "3":
        df = example3_inferred_schema()
        df.show()
    elif example_num == "4":
        df = example4_debezium_cdc()
        df.writeStream.format("console").start().awaitTermination()
    elif example_num == "5":
        df = example5_error_handling()
        df.writeStream.format("console").start().awaitTermination()
    elif example_num == "6":
        df = example6_batch_processing()
        df.show()
    else:
        print(f"Unknown example: {example_num}")
        sys.exit(1)
