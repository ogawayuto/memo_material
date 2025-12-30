#!/bin/bash
# Stop script for EL Pipeline Environment

set -e

echo "========================================="
echo "Stopping EL Pipeline Environment"
echo "========================================="
echo ""

# Check if running from project root
if [ ! -f "docker-compose.yml" ]; then
    echo "Error: This script must be run from the project root directory"
    exit 1
fi

echo "Step 1: Stopping Spark streaming jobs..."
if [ -f "scripts/manage-streaming-job.sh" ]; then
    ./scripts/manage-streaming-job.sh stop || true
else
    docker exec spark-master pkill -9 -f "kafka_to_deltalake" 2>/dev/null || true
fi
echo "   ✓ Spark jobs stopped"
echo ""

echo "Step 2: Stopping all services..."
docker-compose down

echo ""
echo "========================================="
echo "EL Pipeline Environment Stopped!"
echo "========================================="
echo ""
echo "To remove all data volumes, run:"
echo "  docker-compose down -v"
echo ""
echo "To restart the environment, run:"
echo "  ./scripts/start.sh"
echo ""
