#!/bin/bash
# Health check script for EL Pipeline Environment

echo "========================================="
echo "EL Pipeline Health Check"
echo "========================================="
echo ""

# Check if running from project root
if [ ! -f "docker-compose.yml" ]; then
    echo "Error: This script must be run from the project root directory"
    exit 1
fi

echo "1. Docker Compose Services Status:"
echo "-----------------------------------"
docker-compose ps
echo ""

echo "2. Service Health Checks:"
echo "-------------------------"

# PostgreSQL
echo -n "PostgreSQL:        "
if curl -f http://localhost:5432 > /dev/null 2>&1 || nc -z localhost 5432 2>/dev/null; then
    echo "✓ Running"
else
    echo "✗ Not responding"
fi

# Adminer
echo -n "Adminer:           "
if curl -f http://localhost:8081 > /dev/null 2>&1; then
    echo "✓ Running"
else
    echo "✗ Not responding"
fi

# Kafka
echo -n "Kafka:             "
if nc -z localhost 9092 2>/dev/null; then
    echo "✓ Running"
else
    echo "✗ Not responding"
fi

# Kafka UI
echo -n "Kafka UI:          "
if curl -f http://localhost:8082 > /dev/null 2>&1; then
    echo "✓ Running"
else
    echo "✗ Not responding"
fi

# Kafka Connect
echo -n "Kafka Connect:     "
if curl -f http://localhost:8083 > /dev/null 2>&1; then
    echo "✓ Running"
else
    echo "✗ Not responding"
fi

# MinIO
echo -n "MinIO:             "
if curl -f http://localhost:9000/minio/health/live > /dev/null 2>&1; then
    echo "✓ Running"
else
    echo "✗ Not responding"
fi

# MinIO Console
echo -n "MinIO Console:     "
if curl -f http://localhost:9001 > /dev/null 2>&1; then
    echo "✓ Running"
else
    echo "✗ Not responding"
fi

# Spark Master
echo -n "Spark Master:      "
if curl -f http://localhost:8080 > /dev/null 2>&1; then
    echo "✓ Running"
else
    echo "✗ Not responding"
fi

# Spark Worker 1
echo -n "Spark Worker 1:    "
if curl -f http://localhost:8091 > /dev/null 2>&1; then
    echo "✓ Running"
else
    echo "✗ Not responding"
fi

# Spark Worker 2
echo -n "Spark Worker 2:    "
if curl -f http://localhost:8092 > /dev/null 2>&1; then
    echo "✓ Running"
else
    echo "✗ Not responding"
fi

# JupyterLab
echo -n "JupyterLab:        "
if curl -f http://localhost:8888 > /dev/null 2>&1; then
    echo "✓ Running"
else
    echo "✗ Not responding"
fi

echo ""

echo "3. Debezium Connector Status:"
echo "------------------------------"
if curl -f http://localhost:8083 > /dev/null 2>&1; then
    echo "Registered Connectors:"
    curl -s http://localhost:8083/connectors | jq '.'
    echo ""
    echo "Connector Status:"
    curl -s http://localhost:8083/connectors/postgres-source-connector/status 2>/dev/null | jq '.' || echo "Connector not found or not registered yet"
else
    echo "Kafka Connect is not available"
fi

echo ""
echo "========================================="
echo "Health Check Complete"
echo "========================================="
echo ""
