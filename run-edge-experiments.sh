#!/bin/bash
# Edge threshold experiment: runs bot for 1 hour at each edge value
# and produces a summary report at the end.

set -e

EDGES=("0.08" "0.09" "0.10")
DURATION=3600  # 1 hour per experiment
BINARY="target/release/pmbot-rust"
ENV_FILE=".env"
REPORT_FILE="edge-experiment-report.md"
LOG_DIR="edge-experiment-logs"

mkdir -p "$LOG_DIR"

echo "=== Edge Threshold Experiment ==="
echo "Start time: $(date)"
echo "Running ${#EDGES[@]} experiments, ${DURATION}s each"
echo ""

for edge in "${EDGES[@]}"; do
    LOG_FILE="$LOG_DIR/edge-${edge}.log"
    echo "--- Experiment: MIN_EDGE_THRESHOLD=${edge} ---"
    echo "  Start: $(date)"

    # Update .env with this edge value
    sed -i '' "s/^MIN_EDGE_THRESHOLD=.*/MIN_EDGE_THRESHOLD=${edge}/" "$ENV_FILE"

    # Start bot
    $BINARY --mode paper --strategy unified > "$LOG_FILE" 2>&1 &
    BOT_PID=$!
    echo "  PID: $BOT_PID"

    # Wait 1 hour
    sleep $DURATION

    # Graceful shutdown (SIGTERM triggers shutdown log)
    kill $BOT_PID 2>/dev/null || true
    sleep 5
    # Force kill if still running
    kill -9 $BOT_PID 2>/dev/null || true
    sleep 2

    echo "  End: $(date)"
    echo "  Log: $LOG_FILE ($(wc -l < "$LOG_FILE") lines)"
    echo ""
done

echo "=== All experiments complete ==="
echo "Generating report..."

# Generate markdown report
cat > "$REPORT_FILE" << 'HEADER'
# Edge Threshold Experiment Results

Each edge value was tested for 1 hour in paper trading mode with a $100 portfolio.

| Edge | Signals | Resolved | Wins | Losses | Win Rate | Realized P&L | Portfolio | Return % |
|------|---------|----------|------|--------|----------|-------------|-----------|----------|
HEADER

for edge in "${EDGES[@]}"; do
    LOG_FILE="$LOG_DIR/edge-${edge}.log"

    # Count signals
    SIGNALS=$(grep -c "up/down signal\|executing polymarket order" "$LOG_FILE" 2>/dev/null || echo "0")

    # Count resolutions
    RESOLVED=$(grep -c "updown market resolved" "$LOG_FILE" 2>/dev/null || echo "0")

    # Count wins/losses from resolution lines
    WINS=$(grep "updown market resolved" "$LOG_FILE" 2>/dev/null | grep -c "won=true" || echo "0")
    LOSSES=$(grep "updown market resolved" "$LOG_FILE" 2>/dev/null | grep -c "won=false" || echo "0")

    # Win rate
    if [ "$RESOLVED" -gt 0 ]; then
        WIN_RATE=$(echo "scale=1; $WINS * 100 / $RESOLVED" | bc)
    else
        WIN_RATE="n/a"
    fi

    # Get final shutdown line for P&L
    SHUTDOWN_LINE=$(grep "Execution engine shutting down" "$LOG_FILE" 2>/dev/null || echo "")

    if [ -n "$SHUTDOWN_LINE" ]; then
        REALIZED=$(echo "$SHUTDOWN_LINE" | sed -n 's/.*realized_pnl=\([^ ]*\).*/\1/p' | head -c 8)
        PORTFOLIO=$(echo "$SHUTDOWN_LINE" | sed -n 's/.*portfolio_value=\([^ ]*\).*/\1/p' | head -c 8)
        RETURN_PCT=$(echo "$SHUTDOWN_LINE" | sed -n 's/.*return_pct=\([^ ]*\).*/\1/p')
    else
        # Try the last paper trading summary instead
        SUMMARY_LINE=$(grep "paper trading summary" "$LOG_FILE" 2>/dev/null | tail -1 || echo "")
        if [ -n "$SUMMARY_LINE" ]; then
            REALIZED=$(echo "$SUMMARY_LINE" | sed -n 's/.*realized_pnl=\([^ ]*\).*/\1/p' | head -c 8)
            PORTFOLIO=$(echo "$SUMMARY_LINE" | sed -n 's/.*portfolio_value=\([^ ]*\).*/\1/p' | head -c 8)
            RETURN_PCT=$(echo "$SUMMARY_LINE" | sed -n 's/.*return_pct=\([^ ]*\).*/\1/p')
        else
            REALIZED="n/a"
            PORTFOLIO="n/a"
            RETURN_PCT="n/a"
        fi
    fi

    echo "| ${edge} | ${SIGNALS} | ${RESOLVED} | ${WINS} | ${LOSSES} | ${WIN_RATE}% | \$${REALIZED} | \$${PORTFOLIO} | ${RETURN_PCT} |" >> "$REPORT_FILE"
done

# Add detailed per-edge breakdowns
echo "" >> "$REPORT_FILE"
echo "## Detailed Breakdown" >> "$REPORT_FILE"
echo "" >> "$REPORT_FILE"

for edge in "${EDGES[@]}"; do
    LOG_FILE="$LOG_DIR/edge-${edge}.log"
    echo "### Edge = ${edge}" >> "$REPORT_FILE"
    echo "" >> "$REPORT_FILE"

    # Show all resolution events
    echo "**Resolutions:**" >> "$REPORT_FILE"
    echo '```' >> "$REPORT_FILE"
    grep "updown market resolved" "$LOG_FILE" 2>/dev/null | \
        sed 's/.*updown market resolved //' | \
        sed 's/\x1b\[[0-9;]*m//g' || echo "(none)"
    echo '```' >> "$REPORT_FILE"
    echo "" >> "$REPORT_FILE"
done >> "$REPORT_FILE" 2>/dev/null

echo ""
echo "=== REPORT ==="
cat "$REPORT_FILE"
echo ""
echo "Report saved to: $REPORT_FILE"
echo "Logs saved to: $LOG_DIR/"
echo "Finished: $(date)"
