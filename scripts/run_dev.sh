#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$ROOT_DIR"

if [ -d ".venv" ]; then
    echo "Activating virtual environment..."
    source .venv/bin/activate
fi

echo "Starting Event-Driven Telemetry Mesh API & Dashboard on http://localhost:8000 ..."
exec uvicorn telemetry_mesh.api.app:app --host 0.0.0.0 --port 8000 --reload
