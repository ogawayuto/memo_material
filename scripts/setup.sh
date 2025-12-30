#!/bin/bash
# Setup script for EL Pipeline Environment

set -e

echo "========================================="
echo "EL Pipeline Environment Setup"
echo "========================================="
echo ""

# Check if running from project root
if [ ! -f "docker-compose.yml" ]; then
    echo "Error: This script must be run from the project root directory"
    exit 1
fi

echo "1. Setting executable permissions for scripts..."
chmod +x scripts/*.sh
chmod +x debezium/scripts/*.sh
echo "   ✓ Permissions set"
echo ""

echo "2. Checking Docker installation..."
if ! command -v docker &> /dev/null; then
    echo "   ✗ Docker is not installed. Please install Docker first."
    exit 1
fi
echo "   ✓ Docker is installed: $(docker --version)"
echo ""

echo "3. Checking Docker Compose installation..."
if ! command -v docker-compose &> /dev/null; then
    echo "   ✗ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi
echo "   ✓ Docker Compose is installed: $(docker-compose --version)"
echo ""

echo "4. Validating docker-compose.yml..."
docker-compose config > /dev/null
echo "   ✓ docker-compose.yml is valid"
echo ""

echo "5. Creating necessary directories..."
mkdir -p postgres debezium/connectors debezium/scripts spark/conf spark/jobs minio notebooks scripts
echo "   ✓ Directories created"
echo ""

echo "========================================="
echo "Setup completed successfully!"
echo "========================================="
echo ""
echo "Next steps:"
echo "  1. Review .env file and adjust settings if needed"
echo "  2. Run './scripts/start.sh' to start the pipeline"
echo "  3. Wait for all services to be ready"
echo "  4. Access the UIs:"
echo "     - Adminer (PostgreSQL):  http://localhost:8081"
echo "     - Kafka UI:              http://localhost:8082"
echo "     - MinIO Console:         http://localhost:9001"
echo "     - Spark Master UI:       http://localhost:8080"
echo "     - JupyterLab:            http://localhost:8888"
echo ""
