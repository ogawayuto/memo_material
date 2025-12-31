#!/bin/bash
# Start script for EL Pipeline Environment
# This script is idempotent and always initializes to the same state

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

echo "Step 0: Ensuring clean state..."
docker-compose down -v 2>/dev/null || true
echo "   ✓ Previous state cleaned"
echo ""

echo "Step 1: Building Kafka Connect with Avro support..."
docker-compose build kafka-connect
echo "   ✓ Kafka Connect built"
echo ""

echo "Step 2: Starting core services (PostgreSQL, Kafka, MinIO)..."
docker-compose up -d postgres kafka minio
echo "   ✓ Core services started"
echo ""

echo "Waiting 30 seconds for core services to initialize..."
sleep 30
echo ""

echo "Step 3: Starting Schema Registry..."
docker-compose up -d schema-registry
echo "   ✓ Schema Registry started"
echo ""

echo "Waiting 15 seconds for Schema Registry to initialize..."
sleep 15
echo ""

echo "Step 4: Initializing PostgreSQL database..."
# Wait for PostgreSQL to be ready
until docker exec postgres pg_isready -U postgres > /dev/null 2>&1; do
  echo "   Waiting for PostgreSQL to be ready..."
  sleep 2
done

# Initialize database
docker exec -i postgres psql -U postgres << 'EOSQL'
-- Create source database (if not exists)
SELECT 'CREATE DATABASE sourcedb' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'sourcedb')\gexec

-- Connect to the source database
\c sourcedb;

-- Drop and recreate customers table for idempotency
DROP TABLE IF EXISTS customers CASCADE;

CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Set REPLICA IDENTITY FULL for CDC
ALTER TABLE customers REPLICA IDENTITY FULL;

-- Insert sample data
INSERT INTO customers (name, email) VALUES
    ('John Doe', 'john.doe@example.com'),
    ('Jane Smith', 'jane.smith@example.com'),
    ('Alice Johnson', 'alice.johnson@example.com');

-- Create updated_at trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger for updated_at
CREATE TRIGGER update_customers_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

SELECT 'Database initialized successfully!' AS status;
EOSQL

echo "   ✓ PostgreSQL initialized"
echo ""

echo "Step 5: Initializing MinIO buckets..."
docker-compose up -d minio-init
sleep 5
echo "   ✓ MinIO initialized"
echo ""

echo "Step 6: Starting Kafka Connect and UI services..."
docker-compose up -d kafka-connect kafka-ui schema-registry-ui adminer
echo "   ✓ Services started"
echo ""

echo "Waiting for Kafka Connect to be ready..."
MAX_RETRIES=60
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -f http://localhost:8083/ > /dev/null 2>&1; then
        echo "   ✓ Kafka Connect is ready"
        break
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
        echo "   ✗ Kafka Connect failed to start within expected time"
        echo "   Check logs with: docker-compose logs kafka-connect"
        exit 1
    fi
    echo "   Kafka Connect is not ready yet. Waiting... ($RETRY_COUNT/$MAX_RETRIES)"
    sleep 2
done
echo ""

echo "Step 7: Registering Debezium connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/connectors/postgres-connector.json 2>/dev/null || true
echo ""
echo "   ✓ Debezium connector registered"
echo ""

echo "Step 8: Starting Spark cluster..."
docker-compose up -d spark-master spark-worker
echo "   ✓ Spark cluster started"
echo ""

echo "Waiting 15 seconds for Spark cluster to be ready..."
sleep 15
echo ""

echo "Step 9: Starting JupyterLab..."
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
echo "  - Schema Registry:       http://localhost:8085"
echo "  - Schema Registry UI:    http://localhost:8086"
echo "  - Spark Master UI:       http://localhost:8080"
echo "  - Spark Worker UI:       http://localhost:8091"
echo "  - MinIO Console:         http://localhost:9001"
echo "  - JupyterLab:            http://localhost:8888 (token: delta-lake-token)"
echo ""
echo "Spark Streaming Job Management:"
echo "  - Start job:    ./scripts/manage-streaming-job.sh start"
echo "  - Stop job:     ./scripts/manage-streaming-job.sh stop"
echo "  - Restart job:  ./scripts/manage-streaming-job.sh restart"
echo "  - Check status: ./scripts/manage-streaming-job.sh status"
echo ""
echo "Stop all services:"
echo "  ./scripts/stop.sh"
echo ""
