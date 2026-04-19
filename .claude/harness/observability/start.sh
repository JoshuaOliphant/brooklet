#!/usr/bin/env bash
# Idempotent startup for observability stack.
# PID-guarded: safe to call repeatedly (e.g. from SessionStart hook).
# Start order: VictoriaLogs → VictoriaMetrics → Vector (Vector needs backends up).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BIN_DIR="$SCRIPT_DIR/bin"
PID_DIR="$SCRIPT_DIR/pids"
LOG_DIR="$SCRIPT_DIR/logs"
DATA_DIR="$SCRIPT_DIR/data"

mkdir -p "$PID_DIR" "$LOG_DIR" "$DATA_DIR/victoria-logs" "$DATA_DIR/victoria-metrics" "$DATA_DIR/vector" "$DATA_DIR/jsonl"

# --- Housekeeping: prune old data on every startup ---
# JSONL files older than 3 days
find "$DATA_DIR/jsonl" -name "*.jsonl" -mtime +3 -delete 2>/dev/null || true
# Truncate service logs over 10MB
find "$LOG_DIR" -name "*.log" -size +10M -exec truncate -s 0 {} \; 2>/dev/null || true

# Check if binaries are installed
if [ ! -f "$BIN_DIR/victoria-logs-prod" ] || [ ! -f "$BIN_DIR/victoria-metrics-prod" ] || [ ! -f "$BIN_DIR/vector" ]; then
    echo "Binaries not found. Running install.sh..."
    bash "$SCRIPT_DIR/install.sh"
fi

is_running() {
    local name="$1"
    local pidfile="$PID_DIR/$name.pid"
    if [ -f "$pidfile" ]; then
        local pid
        pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            # Verify the PID belongs to the expected binary
            local cmdname
            cmdname=$(ps -p "$pid" -o comm= 2>/dev/null || echo "")
            case "$name" in
                victoria-logs)    echo "$cmdname" | grep -q "victoria-logs" && return 0 ;;
                victoria-metrics) echo "$cmdname" | grep -q "victoria-metrics" && return 0 ;;
                vector)           echo "$cmdname" | grep -q "vector" && return 0 ;;
            esac
            echo "WARNING: PID $pid is not $name (found: $cmdname) — cleaning up" >&2
        fi
        rm -f "$pidfile"
    fi
    return 1
}

wait_for_health() {
    local name="$1" url="$2" max_attempts="${3:-15}"
    for i in $(seq 1 "$max_attempts"); do
        if curl -sf "$url" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "WARNING: $name health check failed after ${max_attempts}s ($url)" >&2
    return 1
}

start_service() {
    local name="$1"
    shift
    if is_running "$name"; then
        echo "$name: already running (PID $(cat "$PID_DIR/$name.pid"))"
        return 0
    fi
    echo -n "$name: starting... "
    "$@" >> "$LOG_DIR/$name.log" 2>&1 &
    local pid=$!
    echo "$pid" > "$PID_DIR/$name.pid"
    # Verify process didn't immediately crash
    sleep 0.5
    if ! kill -0 "$pid" 2>/dev/null; then
        echo "FAILED (check $LOG_DIR/$name.log)" >&2
        rm -f "$PID_DIR/$name.pid"
        return 1
    fi
    echo "started (PID $pid)"
}

# --- Start VictoriaLogs ---
start_service victoria-logs \
    "$BIN_DIR/victoria-logs-prod" \
    -storageDataPath="$DATA_DIR/victoria-logs" \
    -httpListenAddr=:9428 \
    -retentionPeriod=7d

# --- Start VictoriaMetrics ---
start_service victoria-metrics \
    "$BIN_DIR/victoria-metrics-prod" \
    -storageDataPath="$DATA_DIR/victoria-metrics" \
    -httpListenAddr=:8428 \
    -retentionPeriod=7d

# --- Wait for backends before starting Vector ---
wait_for_health "VictoriaLogs" "http://127.0.0.1:9428/health"
wait_for_health "VictoriaMetrics" "http://127.0.0.1:8428/health"

# --- Start Vector (run from SCRIPT_DIR so relative paths in vector.toml resolve) ---
cd "$SCRIPT_DIR"
start_service vector \
    "$BIN_DIR/vector" \
    --config "$SCRIPT_DIR/vector.toml"
cd - >/dev/null

# Brief pause for Vector to bind ports
sleep 1

# --- Register OTLP topics in brooklet (idempotent) ---
if command -v brooklet >/dev/null 2>&1; then
    brooklet register otel/logs "$SCRIPT_DIR/data/jsonl/logs/*.jsonl" --mode glob \
        || echo "WARNING: failed to register otel/logs topic" >&2
    brooklet register otel/traces "$SCRIPT_DIR/data/jsonl/traces/*.jsonl" --mode glob \
        || echo "WARNING: failed to register otel/traces topic" >&2
    brooklet register otel/metrics "$SCRIPT_DIR/data/jsonl/metrics/*.jsonl" --mode glob \
        || echo "WARNING: failed to register otel/metrics topic" >&2
fi

echo ""
echo "=== Observability Stack ==="
echo "VictoriaLogs   http://127.0.0.1:9428  (LogsQL)"
echo "VictoriaMetrics http://127.0.0.1:8428  (PromQL)"
echo "Vector OTLP    grpc://127.0.0.1:4317  http://127.0.0.1:4318"
echo "==========================="
