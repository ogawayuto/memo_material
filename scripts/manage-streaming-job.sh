#!/bin/bash
# Spark Streaming Job Manager for Kafka to Delta Lake

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

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

    cd "$PROJECT_DIR"
    ./scripts/run-spark-job.sh
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
    echo "Checking job status..."
    if is_job_running; then
        echo "✓ Job is RUNNING"
        echo ""
        echo "Processes:"
        docker exec spark-master ps aux | grep -E "(kafka_to_deltalake)" | grep -v grep
    else
        echo "○ Job is NOT running"
    fi
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
    *)
        echo "Usage: $0 {start|stop|restart|clean|status}"
        echo ""
        echo "Commands:"
        echo "  start   - Start the streaming job (fails if already running)"
        echo "  stop    - Stop the running job"
        echo "  restart - Stop job, clean checkpoint, and start again"
        echo "  clean   - Clean checkpoint directory only"
        echo "  status  - Check if job is running"
        exit 1
        ;;
esac
