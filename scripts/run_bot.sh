#!/usr/bin/env bash
#
# Auto-restart wrapper for pmbot-rust ORB strategy.
# - Prevents duplicate instances via PID file
# - Rotates log to last 1000 lines on each restart
# - Restarts on crash with 5-second delay
# - Handles SIGTERM gracefully
#
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PID_FILE="$PROJECT_DIR/data/bot.pid"
LOG_FILE="$PROJECT_DIR/data/bot_orb.log"
MAX_LOG_LINES=1000

# Ensure data directory exists
mkdir -p "$PROJECT_DIR/data"

# --- PID file management ---

cleanup() {
    echo "[$(date -u +%FT%T)] Caught signal, shutting down..." >> "$LOG_FILE"
    if [ -n "${BOT_PID:-}" ]; then
        kill "$BOT_PID" 2>/dev/null || true
        wait "$BOT_PID" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
    exit 0
}

trap cleanup SIGTERM SIGINT SIGHUP

# Check for existing instance
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE" 2>/dev/null || true)
    if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
        echo "Bot already running (PID $OLD_PID). Killing stale process..."
        kill "$OLD_PID" 2>/dev/null || true
        sleep 2
        # Force kill if still alive
        if kill -0 "$OLD_PID" 2>/dev/null; then
            kill -9 "$OLD_PID" 2>/dev/null || true
            sleep 1
        fi
    fi
    rm -f "$PID_FILE"
fi

# Write our PID
echo $$ > "$PID_FILE"

# --- Log rotation ---

rotate_log() {
    if [ -f "$LOG_FILE" ]; then
        local line_count
        line_count=$(wc -l < "$LOG_FILE" 2>/dev/null || echo 0)
        if [ "$line_count" -gt "$MAX_LOG_LINES" ]; then
            local tmp="$LOG_FILE.tmp"
            tail -n "$MAX_LOG_LINES" "$LOG_FILE" > "$tmp"
            mv "$tmp" "$LOG_FILE"
            echo "[$(date -u +%FT%T)] Log rotated: trimmed to $MAX_LOG_LINES lines" >> "$LOG_FILE"
        fi
    fi
}

# --- Main loop ---

cd "$PROJECT_DIR"

echo "[$(date -u +%FT%T)] Bot wrapper starting (PID $$)" >> "$LOG_FILE"

while true; do
    rotate_log

    echo "[$(date -u +%FT%T)] Starting bot..." >> "$LOG_FILE"

    # Run the bot, appending stdout+stderr to log
    cargo run --release -- --mode live --strategy orb >> "$LOG_FILE" 2>&1 &
    BOT_PID=$!

    # Wait for the bot process to exit
    wait "$BOT_PID" || true
    EXIT_CODE=$?
    BOT_PID=""

    echo "[$(date -u +%FT%T)] Bot exited with code $EXIT_CODE" >> "$LOG_FILE"

    if [ "$EXIT_CODE" -eq 0 ]; then
        echo "[$(date -u +%FT%T)] Clean exit, not restarting." >> "$LOG_FILE"
        break
    fi

    echo "[$(date -u +%FT%T)] Restarting in 5 seconds..." >> "$LOG_FILE"
    sleep 5
done

rm -f "$PID_FILE"
