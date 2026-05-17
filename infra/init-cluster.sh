#!/usr/bin/env bash
# Creates SCRAM users after the Kafka cluster is up.
# Connects via the internal PLAINTEXT listener (no auth needed inside container network).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/container-tool.sh"

BROKER="${1:-kafka-1}"
CONTAINER="${2:-kafka-1}"
BOOTSTRAP="${BROKER}:9092"

# Users to create (username:password)
USERS=(
  "admin:admin-secret"
)

echo "=== Waiting for cluster to be ready ==="
for i in $(seq 1 30); do
  if ${CONTAINER_TOOL} exec "${CONTAINER}" /opt/kafka/bin/kafka-configs.sh \
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

  ${CONTAINER_TOOL} exec "${CONTAINER}" /opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --alter --add-config "SCRAM-SHA-512=[iterations=4096,password=${PASSWORD}]" \
    --entity-type users --entity-name "${USERNAME}" 2>/dev/null || {
    echo "Failed to create user ${USERNAME} (may already exist)"
  }
done

echo ""
echo "=== Verifying SCRAM users ==="
${CONTAINER_TOOL} exec "${CONTAINER}" /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server "${BOOTSTRAP}" \
  --describe --entity-type users 2>/dev/null

echo ""
echo "=== Cluster init complete ==="
echo "External listeners:"
echo "  SASL_SSL + SCRAM-SHA-512: localhost:9192,9292,9392"
echo "    Username: admin  Password: admin-secret"
echo "  PLAINTEXT: localhost:9193,9293,9393"
