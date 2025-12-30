#!/bin/bash
# Start script for EL Pipeline Environment

set -e

echo "========================================="
echo "Starting EL Pipeline Environment"
echo "========================================="
echo ""

# Check if running from project root
if [ ! -f "docker-compose.yml" ]; then
    echo "Error: This script must be run from the project root directory"
    exit 1
fi

echo "Step 1: Starting core services (PostgreSQL, Kafka, MinIO)..."
docker-compose up -d postgres kafka minio
echo "   ✓ Core services started"
echo ""

echo "Waiting 30 seconds for core services to initialize..."
sleep 30
echo ""

echo "Step 2: Initializing PostgreSQL database..."
./scripts/init-postgres.sh
echo "   ✓ PostgreSQL initialized"
echo ""

echo "Step 3: Initializing MinIO buckets..."
docker-compose up -d minio-init
sleep 5
echo "   ✓ MinIO initialized"
echo ""

echo "Step 4: Starting Kafka Connect and UI services..."
docker-compose up -d kafka-connect kafka-ui adminer
echo "   ✓ UI services started"
echo ""

echo "Waiting 20 seconds for Kafka Connect to be ready..."
sleep 20
echo ""

echo "Step 5: Registering Debezium connector..."
if [ -f "debezium/scripts/register-connector.sh" ]; then
    ./debezium/scripts/register-connector.sh
else
    echo "   Registering connector via curl..."
    curl -X POST http://localhost:8083/connectors \
      -H "Content-Type: application/json" \
      -d @debezium/connectors/postgres-connector.json
    echo ""
fi
echo "   ✓ Debezium connector registered"
echo ""

echo "Step 6: Starting JupyterLab..."
docker-compose up -d jupyter
echo "   ✓ JupyterLab started"
echo ""

echo "========================================="
echo "EL Pipeline Environment Started!"
echo "========================================="
echo ""
echo "Service URLs:"
echo "  - Adminer (PostgreSQL):  http://localhost:8081"
echo "  - Kafka UI:              http://localhost:8082"
echo "  - Kafka Connect API:     http://localhost:8083"
echo "  - MinIO Console:         http://localhost:9001"
echo "  - JupyterLab:            http://localhost:8888 (token: delta-lake-token)"
echo ""
echo "Note: Spark integration is pending. See SPARK_INTEGRATION_HANDOVER.md"
echo ""
echo "Check service status:"
echo "  ./scripts/health-check.sh"
echo ""
echo "Stop all services:"
echo "  ./scripts/stop.sh"
echo ""
