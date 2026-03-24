#!/usr/bin/env bash
#
# Auto-start bot when BTC volatility is high enough.
# Checks every 5 minutes. Starts bot when BTC moves >0.1% in 5 min.
# Sends Telegram alert when starting/stopping.
#
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

source .env 2>/dev/null || true

BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
CHAT_ID="${TELEGRAM_CHAT_ID:-}"
PID_FILE="data/bot.pid"
MIN_VOLATILITY=0.1  # minimum 0.1% move in 5 minutes
LAST_PRICE=""
CHECK_INTERVAL=300  # 5 minutes

send_telegram() {
    if [ -n "$BOT_TOKEN" ] && [ -n "$CHAT_ID" ]; then
        curl -s "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
            -d "chat_id=${CHAT_ID}" \
            -d "text=$1" \
            -d "parse_mode=HTML" > /dev/null 2>&1 || true
    fi
}

get_btc_price() {
    curl -s "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT" 2>/dev/null | \
        python3 -c "import json,sys; print(json.load(sys.stdin)['price'])" 2>/dev/null || echo ""
}

is_bot_running() {
    if [ -f "$PID_FILE" ]; then
        local pid=$(cat "$PID_FILE" 2>/dev/null || echo "")
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            return 0
        fi
    fi
    return 1
}

start_bot() {
    echo "[$(date -u +%FT%T)] Starting bot — volatility detected"
    nohup ./scripts/run_bot.sh > /dev/null 2>&1 &
    send_telegram "🟢 <b>Bot auto-started</b> — BTC volatility ${VOL}% detected"
}

stop_bot() {
    echo "[$(date -u +%FT%T)] Stopping bot — market too quiet"
    if [ -f "$PID_FILE" ]; then
        local pid=$(cat "$PID_FILE" 2>/dev/null || echo "")
        if [ -n "$pid" ]; then
            kill "$pid" 2>/dev/null || true
        fi
        rm -f "$PID_FILE"
    fi
    # Also kill any pmbot processes
    pkill -f "pmbot-rust" 2>/dev/null || true
    send_telegram "🔴 <b>Bot auto-stopped</b> — market too quiet"
}

echo "[$(date -u +%FT%T)] Auto-start monitor running (min volatility: ${MIN_VOLATILITY}%)"
send_telegram "👁 <b>Auto-start monitor</b> running — will start bot when BTC moves >${MIN_VOLATILITY}%"

QUIET_CYCLES=0

while true; do
    PRICE=$(get_btc_price)

    if [ -z "$PRICE" ]; then
        sleep "$CHECK_INTERVAL"
        continue
    fi

    if [ -n "$LAST_PRICE" ]; then
        VOL=$(python3 -c "
p1, p2 = float('$LAST_PRICE'), float('$PRICE')
vol = abs(p2 - p1) / p1 * 100
print(f'{vol:.3f}')
" 2>/dev/null || echo "0")

        echo "[$(date -u +%FT%T)] BTC: \$$PRICE | 5min move: ${VOL}%"

        VOL_HIGH=$(python3 -c "print('yes' if float('$VOL') >= $MIN_VOLATILITY else 'no')" 2>/dev/null || echo "no")

        if [ "$VOL_HIGH" = "yes" ]; then
            QUIET_CYCLES=0
            if ! is_bot_running; then
                start_bot
            fi
        else
            QUIET_CYCLES=$((QUIET_CYCLES + 1))
            # Stop bot after 6 quiet cycles (30 min of no volatility)
            if [ "$QUIET_CYCLES" -ge 6 ] && is_bot_running; then
                stop_bot
            fi
        fi
    fi

    LAST_PRICE="$PRICE"
    sleep "$CHECK_INTERVAL"
done
