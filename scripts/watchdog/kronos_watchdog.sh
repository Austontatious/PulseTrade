#!/usr/bin/env bash
set -euo pipefail

# Simple health watchdog for Kronos services.
# Usage: ./kronos_watchdog.sh /mnt/data/PulseTrade

PROJECT_DIR=${1:-"/mnt/data/PulseTrade"}
COMPOSE="docker compose --compatibility"

cd "$PROJECT_DIR"

check_container () {
    local name=$1
    local status
    status=$(docker inspect --format='{{.State.Health.Status}}' "$name" 2>/dev/null || echo "missing")
    echo "[$(date -Is)] $name health=$status"
    if [[ "$status" != "healthy" ]]; then
        echo "[$(date -Is)] Restarting $name"
        $COMPOSE restart "$name"
    fi
}

check_container pulsetrade_kronos-nbeats_1
check_container pulsetrade_kronos-graph_1
