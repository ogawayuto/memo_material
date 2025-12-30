#!/bin/bash
# Debezium Connector Registration Script

set -e

KAFKA_CONNECT_URL="http://localhost:8083"
CONNECTOR_CONFIG_FILE="debezium/connectors/postgres-connector.json"

echo "Waiting for Kafka Connect to be ready..."
until curl -f ${KAFKA_CONNECT_URL}/ > /dev/null 2>&1; do
  echo "Kafka Connect is not ready yet. Waiting..."
  sleep 5
done

echo "Kafka Connect is ready!"

echo "Checking if connector already exists..."
CONNECTOR_NAME=$(jq -r '.name' ${CONNECTOR_CONFIG_FILE})
if curl -f ${KAFKA_CONNECT_URL}/connectors/${CONNECTOR_NAME} > /dev/null 2>&1; then
  echo "Connector '${CONNECTOR_NAME}' already exists. Deleting it first..."
  curl -X DELETE ${KAFKA_CONNECT_URL}/connectors/${CONNECTOR_NAME}
  sleep 2
fi

echo "Registering Debezium connector..."
curl -X POST \
  -H "Content-Type: application/json" \
  --data @${CONNECTOR_CONFIG_FILE} \
  ${KAFKA_CONNECT_URL}/connectors

echo ""
echo "Connector registered successfully!"

echo "Checking connector status..."
sleep 3
curl -s ${KAFKA_CONNECT_URL}/connectors/${CONNECTOR_NAME}/status | jq '.'

echo ""
echo "Available connectors:"
curl -s ${KAFKA_CONNECT_URL}/connectors | jq '.'
