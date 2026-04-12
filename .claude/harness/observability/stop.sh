#!/usr/bin/env bash
# Graceful shutdown for observability stack.
# Stop order: Vector first (flush buffers) → VictoriaMetrics → VictoriaLogs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_DIR="$SCRIPT_DIR/pids"

GRACE_PERIOD=5

stop_service() {
    local name="$1"
    local pidfile="$PID_DIR/$name.pid"

    if [ ! -f "$pidfile" ]; then
        echo "$name: not running (no PID file)"
        return 0
    fi

    local pid
    pid=$(cat "$pidfile")

    if ! kill -0 "$pid" 2>/dev/null; then
        echo "$name: not running (stale PID $pid)"
        rm -f "$pidfile"
        return 0
    fi

    # Verify the PID belongs to the expected binary before killing
    local cmdname
    cmdname=$(ps -p "$pid" -o comm= 2>/dev/null || echo "")
    case "$name" in
        victoria-logs)    echo "$cmdname" | grep -q "victoria-logs" || { echo "$name: PID $pid is not $name — skipping"; rm -f "$pidfile"; return 0; } ;;
        victoria-metrics) echo "$cmdname" | grep -q "victoria-metrics" || { echo "$name: PID $pid is not $name — skipping"; rm -f "$pidfile"; return 0; } ;;
        vector)           echo "$cmdname" | grep -q "vector" || { echo "$name: PID $pid is not $name — skipping"; rm -f "$pidfile"; return 0; } ;;
    esac

    echo -n "$name: stopping (PID $pid)... "
    kill -TERM "$pid" 2>/dev/null || true

    # Wait for graceful shutdown
    local elapsed=0
    while kill -0 "$pid" 2>/dev/null && [ "$elapsed" -lt "$GRACE_PERIOD" ]; do
        sleep 1
        elapsed=$((elapsed + 1))
    done

    if kill -0 "$pid" 2>/dev/null; then
        echo -n "force killing... "
        kill -KILL "$pid" 2>/dev/null || true
        sleep 1
    fi

    rm -f "$pidfile"
    echo "stopped"
}

echo "=== Stopping Observability Stack ==="
stop_service vector
stop_service victoria-metrics
stop_service victoria-logs
echo "=== All services stopped ==="
