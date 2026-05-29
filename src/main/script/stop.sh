#!/bin/bash

# Find directories relative to script location
SCRIPT_DIR=$(cd "$(dirname "$0")"; pwd)
PID_FILE="$SCRIPT_DIR/app.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "PID file not found at $PID_FILE. Is KafkaFlow running?"
    exit 1
fi

PID=$(cat "$PID_FILE")

# Check if process is actually running
if ! ps -p "$PID" > /dev/null 2>&1; then
    echo "Process with PID $PID is not running. Cleaning up stale PID file."
    rm -f "$PID_FILE"
    exit 0
fi

echo "Stopping KafkaFlow (PID: $PID)..."
# Attempt graceful shutdown (SIGTERM)
kill "$PID"

# Loop to wait for termination (15 seconds max)
TIMEOUT=15
count=0
while ps -p "$PID" > /dev/null 2>&1; do
    if [ $count -ge $TIMEOUT ]; then
        echo "Grace period of $TIMEOUT seconds exceeded. Forcing termination (kill -9)..."
        kill -9 "$PID"
        break
    fi
    sleep 1
    ((count++))
done

rm -f "$PID_FILE"
echo "KafkaFlow stopped successfully."
