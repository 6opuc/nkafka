#!/usr/bin/env bash
# Generates self-signed TLS certificates for the Kafka cluster.
# Uses a container (apache/kafka image) for keytool since JKS format is needed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/container-tool.sh"

SECRETS_DIR="${SCRIPT_DIR}/secrets"
PASSWORD="${PASSWORD:-changeit}"
DAYS="${DAYS:-3650}"
BROKER_DAYS="${BROKER_DAYS:-365}"
KEY_SIZE=4096
BROKERS=("kafka-1" "kafka-2" "kafka-3")
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:4.2.0}"

rm -rf "${SECRETS_DIR}"
mkdir -p "${SECRETS_DIR}"

echo "=== Generating Certificate Authority (OpenSSL) ==="
openssl req -new -x509 -keyout "${SECRETS_DIR}/ca-key.pem" \
  -out "${SECRETS_DIR}/ca-cert.pem" \
  -days "${DAYS}" -newkey rsa:${KEY_SIZE} -nodes \
  -subj "/CN=Kafka-CA/O=nKafka/C=US"

echo "=== Creating Per-Broker Certificates and JKS Keystores (via ${CONTAINER_TOOL}) ==="
${CONTAINER_TOOL} pull "${KAFKA_IMAGE}" --quiet

for BROKER in "${BROKERS[@]}"; do
  echo "--- ${BROKER} ---"

  # Generate private key and CSR with OpenSSL (we need SANs)
  openssl genrsa -out "${SECRETS_DIR}/${BROKER}-key.pem" "${KEY_SIZE}"

  openssl req -new -key "${SECRETS_DIR}/${BROKER}-key.pem" \
    -out "${SECRETS_DIR}/${BROKER}.csr" \
    -subj "/CN=${BROKER}/OU=Kafka/O=nKafka/C=US"

  # Sign with CA
  openssl x509 -req -CA "${SECRETS_DIR}/ca-cert.pem" \
    -CAkey "${SECRETS_DIR}/ca-key.pem" \
    -in "${SECRETS_DIR}/${BROKER}.csr" \
    -out "${SECRETS_DIR}/${BROKER}-cert.pem" \
    -days "${BROKER_DAYS}" -CAcreateserial -sha256 \
    -extfile <(cat <<END
basicConstraints=CA:FALSE
subjectAltName=DNS:${BROKER},DNS:localhost,IP:127.0.0.1
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
END
    )

  rm -f "${SECRETS_DIR}/${BROKER}.csr"

  echo "  ${BROKER} certificate signed"

  # Convert to PKCS12, then to JKS using Docker's keytool
  # First create PKCS12 bundle (cert + key)
  openssl pkcs12 -export \
    -inkey "${SECRETS_DIR}/${BROKER}-key.pem" \
    -in "${SECRETS_DIR}/${BROKER}-cert.pem" \
    -certfile "${SECRETS_DIR}/ca-cert.pem" \
    -name "${BROKER}" \
    -out "${SECRETS_DIR}/${BROKER}.p12" \
    -password "pass:${PASSWORD}"

  # Convert PKCS12 to JKS using keytool in container
  ${CONTAINER_TOOL} run --rm \
    -u "$(id -u):$(id -g)" \
    -v "${SECRETS_DIR}:/secrets:rw" \
    --entrypoint keytool \
    "${KAFKA_IMAGE}" \
    -importkeystore \
    -destkeystore "/secrets/${BROKER}.keystore.jks" \
    -deststorepass "${PASSWORD}" \
    -srckeystore "/secrets/${BROKER}.p12" \
    -srcstoretype PKCS12 \
    -srcstorepass "${PASSWORD}" \
    -alias "${BROKER}" 2>/dev/null

  rm -f "${SECRETS_DIR}/${BROKER}.p12"

  echo "  ${BROKER}.keystore.jks created"
done

echo "=== Creating Truststore (JKS) ==="
${CONTAINER_TOOL} run --rm \
  -u "$(id -u):$(id -g)" \
  -v "${SECRETS_DIR}:/secrets:rw" \
  --entrypoint keytool \
  "${KAFKA_IMAGE}" \
  -keystore "/secrets/server.truststore.jks" \
  -alias CARoot -import -file "/secrets/ca-cert.pem" \
  -storepass "${PASSWORD}" -noprompt 2>/dev/null

echo "=== Creating Password Files ==="
echo -n "${PASSWORD}" > "${SECRETS_DIR}/keystore-creds"
echo -n "${PASSWORD}" > "${SECRETS_DIR}/key-creds"
echo -n "${PASSWORD}" > "${SECRETS_DIR}/truststore-creds"

echo "=== Creating Empty JAAS Config ==="
cat > "${SECRETS_DIR}/empty-jaas.conf" << 'EOF'
KafkaServer {
    org.apache.kafka.common.security.scram.ScramLoginModule required;
};
EOF

echo "=== Verifying ==="
for BROKER in "${BROKERS[@]}"; do
  echo "--- ${BROKER} ---"
  openssl verify -CAfile "${SECRETS_DIR}/ca-cert.pem" \
    "${SECRETS_DIR}/${BROKER}-cert.pem"
done

echo ""
echo "=== Files ==="
ls -la "${SECRETS_DIR}/"
echo ""
echo "CA cert (for clients): ${SECRETS_DIR}/ca-cert.pem"
echo "Truststore (for clients): ${SECRETS_DIR}/server.truststore.jks"
echo "Password: ${PASSWORD}"
