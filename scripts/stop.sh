#!/bin/bash
# Stop script for EL Pipeline Environment
# This script is idempotent and completely destroys all state

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
./scripts/manage-streaming-job.sh stop 2>/dev/null || true
echo "   ✓ Spark jobs stopped"
echo ""

echo "Step 2: Stopping all services and removing volumes..."
docker-compose down -v
echo "   ✓ All services stopped and volumes removed"
echo ""

echo "Step 3: Removing orphaned containers and networks..."
docker-compose down --remove-orphans 2>/dev/null || true
echo "   ✓ Cleanup complete"
echo ""

echo "========================================="
echo "EL Pipeline Environment Stopped!"
echo "========================================="
echo ""
echo "All data has been destroyed."
echo "To restart the environment, run:"
echo "  ./scripts/start.sh"
echo ""
