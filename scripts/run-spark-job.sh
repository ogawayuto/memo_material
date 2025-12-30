#!/bin/bash
# Run Spark job to process Kafka CDC events and write to Delta Lake

echo "Starting Spark Streaming Job: Kafka → Delta Lake"
echo "=================================================="
echo ""

docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,io.delta:delta-spark_2.13:4.0.0,org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.681 \
  /opt/spark/jobs/kafka_to_deltalake.py

echo ""
echo "Spark job completed!"
