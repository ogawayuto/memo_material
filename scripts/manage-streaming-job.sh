#!/bin/bash
# Spark Streaming Job Manager for Kafka to Delta Lake

set -e

# Function to check if job is running
is_job_running() {
    docker exec spark-master ps aux | grep -E "(kafka_to_deltalake)" | grep -v grep > /dev/null 2>&1
    return $?
}

# Function to stop the job
stop_job() {
    echo "Stopping Spark streaming job..."
    if is_job_running; then
        docker exec spark-master pkill -9 -f "kafka_to_deltalake" || true
        sleep 3
        if is_job_running; then
            echo "❌ Failed to stop the job"
            return 1
        else
            echo "✓ Job stopped successfully"
        fi
    else
        echo "ℹ No job is currently running"
    fi
}

# Function to clean checkpoint
clean_checkpoint() {
    echo "Cleaning checkpoint directory..."
    docker run --rm --network el-pipeline-network \
        -e MC_HOST_minio=http://minioadmin:minioadmin@minio:9000 \
        minio/mc rm --recursive --force minio/delta-lake/checkpoints/ 2>/dev/null || true
    echo "✓ Checkpoint cleaned"
}

# Function to start the job
start_job() {
    echo "Starting Spark streaming job..."
    if is_job_running; then
        echo "⚠ Job is already running. Stop it first with: $0 stop"
        return 1
    fi

    echo "Submitting job to Spark cluster..."
    docker exec -d -e HOME=/opt/spark spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages io.delta:delta-spark_2.13:4.0.0,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.apache.hadoop:hadoop-aws:3.4.1 \
        --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
        --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
        --conf "spark.jars.ivy=/tmp/.ivy2" \
        /opt/spark/jobs/kafka_to_deltalake.py

    echo ""
    echo "Waiting for job to start..."
    sleep 3

    if is_job_running; then
        echo "✓ Job started successfully"
        echo ""
        echo "============================================"
        echo "  Showing startup logs (Ctrl+C to exit)"
        echo "============================================"
        echo ""

        # Follow logs and show important startup events
        docker logs spark-master -f --tail 50 2>&1 | grep --line-buffered -E "(Starting|Spark|Kafka|Schema Registry|Avro|Batch|Processing|ERROR|WARN|Delta)"
    else
        echo "❌ Failed to start job"
        echo ""
        echo "Recent logs:"
        docker logs spark-master --tail 30 2>&1
        return 1
    fi
}

# Function to restart the job
restart_job() {
    echo "Restarting Spark streaming job..."
    stop_job
    clean_checkpoint
    sleep 2
    start_job
}

# Function to show job status
status_job() {
    echo "============================================"
    echo "  Spark Streaming Job Status"
    echo "============================================"
    echo ""

    # Check if job is running
    if is_job_running; then
        echo "Status: ✓ RUNNING"
        echo ""

        # Show process details
        echo "Process Details:"
        docker exec spark-master ps aux | grep -E "(kafka_to_deltalake)" | grep -v grep | head -5
        echo ""

        # Check recent logs for activity
        echo "Recent Activity (last 10 lines):"
        docker logs spark-master --tail 10 2>&1 | grep -E "(Batch|Processing|records|ERROR|Avro)" || echo "  (No recent activity found)"
        echo ""

        # Count error logs
        ERROR_COUNT=$(docker logs spark-master 2>&1 | grep -c "ERROR" || echo "0")
        if [ "$ERROR_COUNT" -gt 0 ]; then
            echo "⚠ Errors detected: $ERROR_COUNT error messages in logs"
            echo "  View errors: docker logs spark-master 2>&1 | grep ERROR"
        else
            echo "✓ No errors detected in logs"
        fi
        echo ""

        # Check Schema Registry connectivity
        echo "Schema Registry Status:"
        if curl -s http://localhost:8085/subjects > /dev/null 2>&1; then
            SUBJECT_COUNT=$(curl -s http://localhost:8085/subjects | jq '. | length' 2>/dev/null || echo "?")
            echo "  ✓ Connected (Subjects: $SUBJECT_COUNT)"
        else
            echo "  ✗ Not accessible"
        fi
        echo ""

        # Show Spark UI URL
        echo "Monitoring:"
        echo "  Spark Master UI: http://localhost:8080"
        echo "  Spark Driver UI: http://localhost:4040 (when job is running)"
        echo "  View logs: docker logs spark-master -f"

    else
        echo "Status: ○ NOT RUNNING"
        echo ""
        echo "Start the job with: $0 start"

        # Check if there are recent error logs
        if docker logs spark-master --tail 50 2>&1 | grep -q "ERROR"; then
            echo ""
            echo "⚠ Recent errors detected in logs:"
            docker logs spark-master --tail 20 2>&1 | grep "ERROR" | tail -5
        fi
    fi

    echo ""
    echo "============================================"
}

# Function to monitor job in real-time
monitor_job() {
    echo "============================================"
    echo "  Real-time Job Monitor"
    echo "============================================"
    echo "Press Ctrl+C to exit"
    echo ""

    if ! is_job_running; then
        echo "○ Job is NOT running"
        echo "Start the job with: $0 start"
        exit 1
    fi

    # Follow logs with filtering for important events
    docker logs spark-master -f 2>&1 | grep --line-buffered -E "(Batch|Processing|records|ERROR|WARN|Avro|Deserialization|Delta)"
}

# Function to show detailed logs
logs_job() {
    LINES="${2:-50}"
    echo "Showing last $LINES lines of logs..."
    echo "============================================"
    docker logs spark-master --tail "$LINES" 2>&1
}

# Main script logic
case "${1:-}" in
    start)
        start_job
        ;;
    stop)
        stop_job
        ;;
    restart)
        restart_job
        ;;
    clean)
        clean_checkpoint
        ;;
    status)
        status_job
        ;;
    monitor)
        monitor_job
        ;;
    logs)
        logs_job "$@"
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|clean|status|monitor|logs}"
        echo ""
        echo "Commands:"
        echo "  start   - Start the streaming job (fails if already running)"
        echo "  stop    - Stop the running job"
        echo "  restart - Stop job, clean checkpoint, and start again"
        echo "  clean   - Clean checkpoint directory only"
        echo "  status  - Check detailed job status with error detection"
        echo "  monitor - Real-time log monitoring (Ctrl+C to exit)"
        echo "  logs    - Show recent logs (default: 50 lines, e.g., 'logs 100')"
        echo ""
        echo "Examples:"
        echo "  $0 status          # Check if job is running"
        echo "  $0 monitor         # Watch logs in real-time"
        echo "  $0 logs 100        # Show last 100 log lines"
        exit 1
        ;;
esac
