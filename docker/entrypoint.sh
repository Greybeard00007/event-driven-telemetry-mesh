#!/usr/bin/env bash
set -e

# Default host/port configurations
HOST="${API_HOST:-0.0.0.0}"
PORT="${API_PORT:-8000}"

echo "Starting Event-Driven Telemetry Mesh service with command: $@"
exec "$@"
