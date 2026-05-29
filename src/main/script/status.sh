#!/bin/bash

# Find directories relative to script location
SCRIPT_DIR=$(cd "$(dirname "$0")"; pwd)
PID_FILE="$SCRIPT_DIR/app.pid"

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "KafkaFlow status: RUNNING (PID: $PID)"
        exit 0
    else
        echo "KafkaFlow status: STOPPED (Stale PID file found)"
        exit 2
    fi
else
    echo "KafkaFlow status: STOPPED"
    exit 3
fi
