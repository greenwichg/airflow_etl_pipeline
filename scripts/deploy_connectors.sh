#!/usr/bin/env bash
# =============================================================================
# Deploy Debezium CDC connectors and Snowflake sink to Kafka Connect
#
# Prerequisites:
#   - docker-compose up -d  (Kafka Connect must be healthy)
#   - Environment variables set for database credentials
#
# Usage:
#   ./scripts/deploy_connectors.sh [--delete-first]
# =============================================================================

set -euo pipefail

CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
CONFIG_DIR="$(cd "$(dirname "$0")/../config/debezium" && pwd)"

# Wait for Kafka Connect to be ready
echo "Waiting for Kafka Connect at ${CONNECT_URL} ..."
for i in $(seq 1 30); do
    if curl -sf "${CONNECT_URL}/connectors" > /dev/null 2>&1; then
        echo "Kafka Connect is ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: Kafka Connect not reachable after 30 attempts." >&2
        exit 1
    fi
    sleep 5
done

# Optional: delete existing connectors first (for re-deployment)
if [ "${1:-}" = "--delete-first" ]; then
    echo "Deleting existing connectors ..."
    for connector in $(curl -sf "${CONNECT_URL}/connectors" | python3 -c "import sys,json; [print(c) for c in json.load(sys.stdin)]" 2>/dev/null); do
        echo "  Deleting ${connector}"
        curl -sf -X DELETE "${CONNECT_URL}/connectors/${connector}" || true
    done
    echo ""
fi

# Create Kafka topics required by CDC pipeline
echo "Creating Kafka topics ..."
docker-compose exec -T kafka kafka-topics --create --if-not-exists \
    --bootstrap-server kafka:29092 \
    --topic cdc.retail.sales.transactions \
    --partitions 4 \
    --replication-factor 1 \
    --config cleanup.policy=compact \
    --config retention.ms=604800000

# Create dead-letter-queue topics
for region in us_east us_west europe asia_pacific; do
    docker-compose exec -T kafka kafka-topics --create --if-not-exists \
        --bootstrap-server kafka:29092 \
        --topic "dlq.cdc.retail.${region}" \
        --partitions 1 \
        --replication-factor 1
done

docker-compose exec -T kafka kafka-topics --create --if-not-exists \
    --bootstrap-server kafka:29092 \
    --topic dlq.snowflake.sink \
    --partitions 1 \
    --replication-factor 1

echo ""

# Deploy Debezium source connectors (one per region)
echo "Deploying Debezium source connectors ..."
for connector_file in "${CONFIG_DIR}"/connector_*.json; do
    connector_name=$(python3 -c "import json; print(json.load(open('${connector_file}'))['name'])")
    echo "  Deploying ${connector_name} from $(basename "${connector_file}")"

    # Substitute environment variables in the config
    config_json=$(envsubst < "${connector_file}")

    response=$(curl -sf -X POST "${CONNECT_URL}/connectors" \
        -H "Content-Type: application/json" \
        -d "${config_json}" 2>&1) || {
        echo "    WARNING: Deploy failed, attempting update ..."
        curl -sf -X PUT "${CONNECT_URL}/connectors/${connector_name}/config" \
            -H "Content-Type: application/json" \
            -d "$(echo "${config_json}" | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin)['config']))")"
    }
    echo "    OK"
done

echo ""

# Deploy Snowflake sink connector
echo "Deploying Snowflake sink connector ..."
sink_config=$(envsubst < "${CONFIG_DIR}/snowflake_sink_connector.json")
sink_name=$(echo "${sink_config}" | python3 -c "import sys,json; print(json.load(sys.stdin)['name'])")

curl -sf -X POST "${CONNECT_URL}/connectors" \
    -H "Content-Type: application/json" \
    -d "${sink_config}" 2>/dev/null || {
    echo "  WARNING: Deploy failed, attempting update ..."
    curl -sf -X PUT "${CONNECT_URL}/connectors/${sink_name}/config" \
        -H "Content-Type: application/json" \
        -d "$(echo "${sink_config}" | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin)['config']))")"
}
echo "  OK"

echo ""

# Verify all connectors
echo "========================================="
echo "Deployed connectors:"
echo "========================================="
curl -sf "${CONNECT_URL}/connectors" | python3 -c "
import sys, json
connectors = json.load(sys.stdin)
for c in connectors:
    print(f'  - {c}')
print(f'\nTotal: {len(connectors)} connectors')
"

echo ""
echo "Check connector status with:"
echo "  curl ${CONNECT_URL}/connectors/<name>/status"
