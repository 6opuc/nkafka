# Detects the container tool (docker/podman) and compose command.
# Source this from other scripts: source "$(dirname "$0")/container-tool.sh"

if [ -z "${CONTAINER_TOOL:-}" ]; then
  if command -v podman &>/dev/null; then
    CONTAINER_TOOL="podman"
  else
    CONTAINER_TOOL="docker"
  fi
fi

if [ -z "${COMPOSE_TOOL:-}" ]; then
  if command -v docker-compose &>/dev/null; then
    COMPOSE_TOOL="docker-compose"
  elif ${CONTAINER_TOOL} compose version &>/dev/null; then
    COMPOSE_TOOL="${CONTAINER_TOOL} compose"
  elif command -v podman-compose &>/dev/null; then
    COMPOSE_TOOL="podman-compose"
  else
    COMPOSE_TOOL="${CONTAINER_TOOL} compose"
  fi
fi
