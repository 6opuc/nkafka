#!/usr/bin/env bash
# Creates SCRAM users after the Kafka cluster is up.
# Connects via the internal PLAINTEXT listener (no auth needed inside Docker network).
set -euo pipefail

BROKER="${1:-kafka-1}"
CONTAINER="${2:-kafka-1}"
BOOTSTRAP="${BROKER}:9092"

# Users to create (username:password)
USERS=(
  "admin:admin-secret"
)

echo "=== Waiting for cluster to be ready ==="
for i in $(seq 1 30); do
  if docker exec "${CONTAINER}" /opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --describe --entity-type users 2>/dev/null; then
    echo "Cluster is ready (attempt $i)"
    break
  fi
  echo "Waiting... (attempt $i)"
  sleep 2
done

echo ""
echo "=== Creating SCRAM users ==="
for USERPASS in "${USERS[@]}"; do
  USERNAME="${USERPASS%%:*}"
  PASSWORD="${USERPASS##*:}"
  echo "Creating user: ${USERNAME}"

  docker exec "${CONTAINER}" /opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --alter --add-config "SCRAM-SHA-512=[iterations=4096,password=${PASSWORD}]" \
    --entity-type users --entity-name "${USERNAME}" 2>/dev/null || {
    echo "Failed to create user ${USERNAME} (may already exist)"
  }
done

echo ""
echo "=== Verifying SCRAM users ==="
docker exec "${CONTAINER}" /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server "${BOOTSTRAP}" \
  --describe --entity-type users 2>/dev/null

echo ""
echo "=== Cluster init complete ==="
echo "Connect external clients to: localhost:9192 (SASL_SSL + SCRAM-SHA-512)"
echo "  Username: admin"
echo "  Password: admin-secret"
