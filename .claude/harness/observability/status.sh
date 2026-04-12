#!/usr/bin/env bash
# Health check for observability stack — reports UP/DOWN per service.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_DIR="$SCRIPT_DIR/pids"

check_service() {
    local name="$1" port="$2" health_url="$3"
    local pidfile="$PID_DIR/$name.pid"
    local status="DOWN" pid="-"

    if [ -f "$pidfile" ]; then
        pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            # Verify the PID belongs to the expected binary
            local cmdname
            cmdname=$(ps -p "$pid" -o comm= 2>/dev/null || echo "")
            local valid_pid=false
            case "$name" in
                victoria-logs)    echo "$cmdname" | grep -q "victoria-logs" && valid_pid=true ;;
                victoria-metrics) echo "$cmdname" | grep -q "victoria-metrics" && valid_pid=true ;;
                vector)           echo "$cmdname" | grep -q "vector" && valid_pid=true ;;
            esac
            if [ "$valid_pid" = true ]; then
                # For services with HTTP health endpoints, use curl; for others just check PID
                if [ -n "$health_url" ] && curl -sf "$health_url" >/dev/null 2>&1; then
                    status="UP"
                elif [ -z "$health_url" ]; then
                    status="UP"
                else
                    status="PID_ONLY"
                fi
            else
                status="STALE"
                echo "WARNING: PID $pid is not $name (found: $cmdname)" >&2
            fi
        else
            pid="stale"
        fi
    fi

    printf "  %-20s %-8s  PID: %-8s  :%s\n" "$name" "$status" "$pid" "$port"
}

echo "=== Observability Stack Status ==="
check_service victoria-logs    9428 "http://127.0.0.1:9428/health"
check_service victoria-metrics 8428 "http://127.0.0.1:8428/health"
check_service vector           4318 ""
echo "==================================="
