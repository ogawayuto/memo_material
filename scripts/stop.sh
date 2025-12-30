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

echo "Stopping all services..."
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
