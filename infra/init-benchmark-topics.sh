#!/usr/bin/env bash
# Creates benchmark test topics and fills them with data.
# Uses the internal PLAINTEXT listener (no auth) inside the container network.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/container-tool.sh"

BOOTSTRAP="kafka-1:9092"
CONTAINER="kafka-1"
PARTITIONS=12
REPLICATION=2

# All scenarios from FetchBenchmarks (including commented-out)
# Format: topic_name:message_count:message_size_bytes
SCENARIOS=(
  "test_p12_m1M_s4B:1000000:4"
  "test_p12_m100K_s4KB:100000:4096"
  "test_p12_m10K_s40KB:10000:40960"
  "test_p12_m4K_s100KB:4000:102400"
  "test_p12_m40K_s10KB:40000:10240"
  "test_p12_m1K_s400KB:1000:409600"
)

echo "=== Waiting for cluster to be ready ==="
for i in $(seq 1 30); do
  if ${CONTAINER_TOOL} exec "${CONTAINER}" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP}" --list 2>/dev/null; then
    echo "Cluster is ready (attempt $i)"
    break
  fi
  echo "Waiting... (attempt $i)"
  sleep 2
done

echo ""
echo "=== Creating benchmark topics ==="
for SCENARIO in "${SCENARIOS[@]}"; do
  TOPIC="${SCENARIO%%:*}"
  REST="${SCENARIO#*:}"
  MSG_COUNT="${REST%%:*}"
  MSG_SIZE="${REST#*:}"

  EXISTING=$(${CONTAINER_TOOL} exec "${CONTAINER}" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP}" --list 2>/dev/null | grep -c "^${TOPIC}$" || true)

  if [ "${EXISTING}" -gt 0 ]; then
    echo "  ${TOPIC} already exists — skipping"
    continue
  fi

  echo "  Creating ${TOPIC} (${PARTITIONS} partitions, RF=${REPLICATION})..."
  ${CONTAINER_TOOL} exec "${CONTAINER}" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --create \
    --topic "${TOPIC}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION}" \
    --config retention.ms=-1 \
    --config retention.bytes=-1 \
    --config min.insync.replicas=2 2>&1

  echo "  Filling ${TOPIC} with ${MSG_COUNT} messages of ${MSG_SIZE} bytes..."
  ${CONTAINER_TOOL} exec "${CONTAINER}" /opt/kafka/bin/kafka-producer-perf-test.sh \
    --topic "${TOPIC}" \
    --num-records "${MSG_COUNT}" \
    --record-size "${MSG_SIZE}" \
    --throughput -1 \
    --producer-props "bootstrap.servers=${BOOTSTRAP}" 2>&1

  echo ""
done

echo "=== Verifying topics ==="
${CONTAINER_TOOL} exec "${CONTAINER}" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP}" --describe 2>/dev/null | grep -E "^Topic:.*test_p"

echo ""
echo "=== Benchmark data setup complete ==="
