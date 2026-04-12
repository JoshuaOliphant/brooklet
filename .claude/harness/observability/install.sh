#!/usr/bin/env bash
# Observability stack installer — downloads VictoriaMetrics, VictoriaLogs, and Vector binaries.
# Idempotent: skips download if correct version already present.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BIN_DIR="$SCRIPT_DIR/bin"

# Pinned versions for reproducibility
VM_VERSION="v1.139.0"
VL_VERSION="v1.24.0-victorialogs"
VECTOR_VERSION="v0.54.0"

mkdir -p "$BIN_DIR"

download_victoria_metrics() {
    local binary="$BIN_DIR/victoria-metrics-prod"
    if [ -f "$binary" ] && "$binary" --version 2>&1 | grep -q "${VM_VERSION#v}"; then
        echo "VictoriaMetrics ${VM_VERSION} already installed"
        return 0
    fi
    echo "Downloading VictoriaMetrics ${VM_VERSION}..."
    local url="https://github.com/VictoriaMetrics/VictoriaMetrics/releases/download/${VM_VERSION}/victoria-metrics-linux-amd64-${VM_VERSION}.tar.gz"
    curl -sfL "$url" | tar xz -C "$BIN_DIR"
    chmod +x "$binary"
    echo "VictoriaMetrics ${VM_VERSION}: $("$binary" --version 2>&1 | head -1)"
}

download_victoria_logs() {
    local binary="$BIN_DIR/victoria-logs-prod"
    if [ -f "$binary" ] && "$binary" --version 2>&1 | grep -q "victoria-logs"; then
        echo "VictoriaLogs ${VL_VERSION} already installed"
        return 0
    fi
    echo "Downloading VictoriaLogs ${VL_VERSION}..."
    local url="https://github.com/VictoriaMetrics/VictoriaMetrics/releases/download/${VL_VERSION}/victoria-logs-linux-amd64-${VL_VERSION}.tar.gz"
    curl -sfL "$url" | tar xz -C "$BIN_DIR"
    chmod +x "$binary"
    echo "VictoriaLogs ${VL_VERSION}: $("$binary" --version 2>&1 | head -1)"
}

download_vector() {
    local binary="$BIN_DIR/vector"
    if [ -f "$binary" ] && "$binary" --version 2>&1 | grep -q "${VECTOR_VERSION#v}"; then
        echo "Vector ${VECTOR_VERSION} already installed"
        return 0
    fi
    echo "Downloading Vector ${VECTOR_VERSION}..."
    local url="https://github.com/vectordotdev/vector/releases/download/${VECTOR_VERSION}/vector-${VECTOR_VERSION#v}-x86_64-unknown-linux-gnu.tar.gz"
    local tmpdir
    tmpdir=$(mktemp -d)
    curl -sfL "$url" | tar xz -C "$tmpdir"
    mv -f "$tmpdir"/vector-x86_64-unknown-linux-gnu/bin/vector "$binary"
    rm -rf "$tmpdir"
    chmod +x "$binary"
    echo "Vector ${VECTOR_VERSION}: $("$binary" --version 2>&1 | head -1)"
}

echo "=== Observability Stack Installer ==="
download_victoria_metrics
download_victoria_logs
download_vector
echo "=== All binaries installed to $BIN_DIR ==="
