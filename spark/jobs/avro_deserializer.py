"""
Generic Avro Deserializer for PySpark with Confluent Schema Registry Support

This module provides a reusable Avro deserializer for PySpark that:
- Handles Confluent Wire Format (Magic Byte 0x00 + 4-byte Schema ID + Avro payload)
- Automatically fetches schemas from Confluent Schema Registry
- Caches Schema Registry clients and deserializers per executor
- Supports schema evolution
- Provides robust error handling
- Works with both streaming and batch DataFrames

Usage Example:
    from avro_deserializer import create_avro_deserializer, infer_spark_schema

    # Create deserializer UDF
    deserialize_udf = create_avro_deserializer(
        schema_registry_url="http://schema-registry:8081"
    )

    # Apply to Kafka DataFrame
    df = kafka_df.select(
        col("key"),
        deserialize_udf(col("value")).alias("data")
    )
"""

import os
import struct
import logging
from typing import Optional, Dict, Any
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, MapType
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import fastavro

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global cache for Schema Registry clients and deserializers
# These are initialized per executor process and reused across tasks
_SR_CLIENT_CACHE: Dict[str, SchemaRegistryClient] = {}
_DESERIALIZER_CACHE: Dict[tuple, AvroDeserializer] = {}


def get_schema_registry_client(url: str) -> SchemaRegistryClient:
    """
    Get or create Schema Registry client with per-executor caching.

    This function maintains a singleton client per Schema Registry URL
    within each executor process, avoiding redundant connections.

    Args:
        url: Schema Registry URL

    Returns:
        SchemaRegistryClient instance
    """
    if url not in _SR_CLIENT_CACHE:
        logger.info(f"Initializing Schema Registry client for {url}")
        _SR_CLIENT_CACHE[url] = SchemaRegistryClient({'url': url})
    return _SR_CLIENT_CACHE[url]


def get_deserializer(sr_url: str, schema_id: int) -> AvroDeserializer:
    """
    Get or create cached deserializer for a specific schema ID.

    Deserializers are cached per (Schema Registry URL, Schema ID) pair
    to optimize performance and support schema evolution.

    Args:
        sr_url: Schema Registry URL
        schema_id: Avro schema ID from Confluent Wire Format

    Returns:
        AvroDeserializer instance
    """
    cache_key = (sr_url, schema_id)

    if cache_key not in _DESERIALIZER_CACHE:
        logger.info(f"Creating deserializer for schema ID {schema_id}")
        client = get_schema_registry_client(sr_url)

        # Fetch schema from registry
        schema = client.get_schema(schema_id)

        # Create deserializer
        _DESERIALIZER_CACHE[cache_key] = AvroDeserializer(
            schema_registry_client=client,
            schema_str=schema.schema_str
        )

        logger.info(f"Deserializer cached for schema ID {schema_id}")

    return _DESERIALIZER_CACHE[cache_key]


def parse_confluent_avro(
    binary_data: bytes,
    sr_url: str,
    return_format: str = 'dict'
) -> Optional[Dict[str, Any]]:
    """
    Parse Confluent Wire Format Avro message.

    Confluent Wire Format:
    - Byte 0: Magic byte (always 0x00)
    - Bytes 1-4: Schema ID (big-endian unsigned int)
    - Bytes 5+: Avro-encoded payload

    Args:
        binary_data: Raw Kafka message value (bytes)
        sr_url: Schema Registry URL
        return_format: Output format ('dict' or 'json')

    Returns:
        Deserialized Avro payload as Python dict, or None on error

    Raises:
        ValueError: If magic byte is incorrect or data is malformed
    """
    if not binary_data or len(binary_data) < 5:
        raise ValueError(
            f"Message too short: {len(binary_data) if binary_data else 0} bytes. "
            "Expected at least 5 bytes (1 magic byte + 4 schema ID bytes)"
        )

    # Validate magic byte
    magic_byte = binary_data[0]
    if magic_byte != 0:
        first_bytes_hex = binary_data[:min(10, len(binary_data))].hex()
        raise ValueError(
            f"Invalid magic byte: 0x{magic_byte:02x} (expected 0x00). "
            f"First 10 bytes: {first_bytes_hex}"
        )

    # Extract schema ID (big-endian 4-byte unsigned integer)
    schema_id = struct.unpack('>I', binary_data[1:5])[0]

    # Extract Avro payload (everything after first 5 bytes)
    avro_payload = binary_data[5:]

    # Get cached deserializer for this schema ID
    deserializer = get_deserializer(sr_url, schema_id)

    # Deserialize payload (returns Python dict)
    deserialized = deserializer(avro_payload, None)

    return deserialized


def create_avro_deserializer(
    schema_registry_url: Optional[str] = None,
    output_schema: Optional[StructType] = None,
    error_handling: str = 'null'
):
    """
    Create a pandas_udf for Avro deserialization with Schema Registry support.

    This factory function creates a vectorized UDF that deserializes Confluent
    Wire Format Avro messages. The UDF maintains cached connections to Schema
    Registry and caches deserializers per schema ID.

    Args:
        schema_registry_url: Schema Registry URL (defaults to env var SCHEMA_REGISTRY_URL)
        output_schema: PySpark StructType for output. If None, uses flexible MapType.
        error_handling: How to handle errors:
            - 'null': Return null for failed records (default)
            - 'skip': Skip failed records entirely
            - 'raise': Raise exception on first error

    Returns:
        pandas_udf function for deserialization

    Example:
        >>> deserialize = create_avro_deserializer(
        ...     schema_registry_url="http://schema-registry:8081"
        ... )
        >>> df = kafka_df.select(deserialize(col("value")).alias("data"))
    """
    # Get Schema Registry URL from parameter or environment
    sr_url = schema_registry_url or os.getenv(
        'SCHEMA_REGISTRY_URL',
        'http://schema-registry:8081'
    )

    # Default output schema: flexible map that can hold any Avro structure
    if output_schema is None:
        output_schema = MapType(StringType(), StringType())

    @pandas_udf(output_schema)
    def deserialize_avro_udf(binary_series: pd.Series) -> pd.Series:
        """
        Vectorized Avro deserialization UDF.

        This UDF processes batches of binary Kafka messages, deserializing
        each one using the Confluent Wire Format.

        Args:
            binary_series: Pandas Series of binary Kafka values

        Returns:
            Pandas Series of deserialized records (dicts or structs)
        """
        results = []
        errors_count = 0

        for idx, binary_data in enumerate(binary_series):
            try:
                # Handle null/empty messages
                if binary_data is None or len(binary_data) == 0:
                    if error_handling == 'skip':
                        continue
                    results.append(None)
                    continue

                # Deserialize Avro message
                payload = parse_confluent_avro(binary_data, sr_url)
                results.append(payload)

            except Exception as e:
                errors_count += 1

                # Log detailed error information
                hex_preview = (
                    binary_data[:20].hex()
                    if binary_data and len(binary_data) > 0
                    else "empty"
                )
                error_msg = (
                    f"Row {idx}: {type(e).__name__}: {str(e)} | "
                    f"First 20 bytes: {hex_preview}"
                )

                # Print to stdout for immediate visibility in Spark logs
                print(f"[AVRO DESERIALIZE ERROR] {error_msg}", flush=True)
                logger.error(error_msg)

                # Handle error based on strategy
                if error_handling == 'raise':
                    raise
                elif error_handling == 'skip':
                    continue
                else:  # 'null'
                    results.append(None)

        # Log batch summary
        total = len(binary_series)
        success = total - errors_count
        if errors_count > 0:
            print(
                f"[AVRO DESERIALIZE BATCH] Processed {total} records: "
                f"{success} succeeded, {errors_count} failed",
                flush=True
            )

        # Convert to Series
        return pd.Series(results)

    return deserialize_avro_udf


def infer_spark_schema(schema_registry_url: str, schema_id: int) -> StructType:
    """
    Infer PySpark StructType from Avro schema in Schema Registry.

    This helper function fetches an Avro schema from the registry and converts
    it to a PySpark StructType, which can be used as the output schema for
    the deserializer UDF.

    Args:
        schema_registry_url: Schema Registry URL
        schema_id: Avro schema ID to fetch

    Returns:
        PySpark StructType corresponding to the Avro schema

    Example:
        >>> schema = infer_spark_schema("http://schema-registry:8081", schema_id=1)
        >>> deserialize = create_avro_deserializer(output_schema=schema)
    """
    import json
    from pyspark.sql.types import (
        IntegerType, LongType, FloatType, DoubleType,
        BooleanType, StringType, ArrayType, StructType, StructField
    )

    # Fetch schema
    client = get_schema_registry_client(schema_registry_url)
    schema = client.get_schema(schema_id)
    avro_schema = json.loads(schema.schema_str)

    def avro_type_to_spark(avro_type, nullable=True):
        """Convert Avro type to Spark type"""
        # Handle union types (nullable fields)
        if isinstance(avro_type, list):
            non_null_types = [t for t in avro_type if t != "null"]
            if len(non_null_types) == 1:
                return avro_type_to_spark(non_null_types[0], nullable=True)
            else:
                # Complex union - use string as fallback
                return StringType()

        # Handle primitive types
        if avro_type == "string":
            return StringType()
        elif avro_type == "int":
            return IntegerType()
        elif avro_type == "long":
            return LongType()
        elif avro_type == "float":
            return FloatType()
        elif avro_type == "double":
            return DoubleType()
        elif avro_type == "boolean":
            return BooleanType()

        # Handle complex types
        elif isinstance(avro_type, dict):
            if avro_type.get("type") == "record":
                fields = []
                for field in avro_type.get("fields", []):
                    field_name = field["name"]
                    field_type = avro_type_to_spark(field["type"])
                    fields.append(StructField(field_name, field_type, True))
                return StructType(fields)

            elif avro_type.get("type") == "array":
                item_type = avro_type_to_spark(avro_type["items"])
                return ArrayType(item_type)

            elif avro_type.get("type") in ["string", "int", "long", "float", "double", "boolean"]:
                return avro_type_to_spark(avro_type["type"])

        # Fallback to string
        return StringType()

    # Convert top-level schema
    return avro_type_to_spark(avro_schema)


# Convenience function for common use case
def create_flexible_deserializer(schema_registry_url: Optional[str] = None):
    """
    Create a deserializer with flexible output (MapType).

    This is the recommended approach when you don't know the exact Avro schema
    upfront or when dealing with schema evolution. The output is a Map that
    can hold any structure.

    Args:
        schema_registry_url: Schema Registry URL

    Returns:
        pandas_udf for flexible Avro deserialization

    Example:
        >>> from pyspark.sql.functions import col
        >>> deserialize = create_flexible_deserializer()
        >>> df = kafka_df.select(
        ...     col("key"),
        ...     deserialize(col("value")).alias("data")
        ... ).select("key", "data.*")  # Expand map fields
    """
    return create_avro_deserializer(
        schema_registry_url=schema_registry_url,
        output_schema=MapType(StringType(), StringType()),
        error_handling='null'
    )


if __name__ == "__main__":
    """
    Example usage and testing
    """
    print("Avro Deserializer Module")
    print("=" * 60)
    print("\nUsage Example:")
    print("""
    from pyspark.sql.functions import col
    from avro_deserializer import create_avro_deserializer

    # Option 1: Flexible output (recommended)
    deserialize = create_avro_deserializer()
    df = kafka_df.select(
        col("key"),
        deserialize(col("value")).alias("data")
    )

    # Option 2: Specific schema
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True)
    ])

    deserialize = create_avro_deserializer(
        schema_registry_url="http://schema-registry:8081",
        output_schema=schema
    )

    df = kafka_df.select(
        deserialize(col("value")).alias("data")
    ).select("data.*")

    # Option 3: Infer schema from Schema Registry
    from avro_deserializer import infer_spark_schema

    schema = infer_spark_schema("http://schema-registry:8081", schema_id=1)
    deserialize = create_avro_deserializer(output_schema=schema)
    """)
