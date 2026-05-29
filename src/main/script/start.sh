#!/bin/bash

# Find directories relative to script location
SCRIPT_DIR=$(cd "$(dirname "$0")"; pwd)
APP_DIR=$(cd "$SCRIPT_DIR/.."; pwd)

# Configuration matching your specific structure: JAR in bin/
JAR_NAME="kafka-flow-1.0.0.jar"
JAR_PATH="$APP_DIR/bin/$JAR_NAME"
CONFIG_DIR="$APP_DIR/config"
LOG_DIR="$APP_DIR/logs"
PID_FILE="$SCRIPT_DIR/app.pid"

# Path to your JDK 17 java executable (falls back to system default if not found)
JAVA_CMD="/usr/local/jdk-17/bin/java"
if [ ! -x "$JAVA_CMD" ]; then
    JAVA_CMD="java"
fi

# JVM options (memory settings and optimization)
JVM_OPTS="-Xms512m -Xmx1024m -XX:+UseG1GC -Dfile.encoding=UTF-8"
# Active Spring Boot profile and config location pointing to config/
SPRING_OPTS="--spring.profiles.active=prod --spring.config.location=classpath:/,file:$CONFIG_DIR/"

# Ensure logging directory exists
mkdir -p "$LOG_DIR"

# Check if application is already running
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "Warning: KafkaFlow is already running (PID: $PID). Aborting startup."
        exit 1
    else
        rm -f "$PID_FILE"
    fi
fi

echo "Starting KafkaFlow (kafka-flow-1.0.0)..."
cd "$APP_DIR" || exit 1

# Start in background using nohup and redirect console output
nohup $JAVA_CMD $JVM_OPTS -jar "$JAR_PATH" $SPRING_OPTS > "$LOG_DIR/console.log" 2>&1 &

PID=$!
echo $PID > "$PID_FILE"

# Wait to verify if process stays active
sleep 3
if ps -p "$PID" > /dev/null 2>&1; then
    echo "KafkaFlow started successfully! PID: $PID"
    echo "Console logs are redirected to: $LOG_DIR/console.log"
else
    echo "Error: KafkaFlow failed to start. Review console logs at: $LOG_DIR/console.log"
    rm -f "$PID_FILE"
    exit 1
fi
