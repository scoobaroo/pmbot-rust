#!/bin/bash
# Generate detailed edge experiment report with buy/sell breakdown
# Run after experiments complete: bash generate-report.sh

EDGES=("0.08" "0.09" "0.10")
LOG_DIR="edge-experiment-logs"
REPORT_FILE="edge-experiment-report.md"

cat > "$REPORT_FILE" << 'HEADER'
# Edge Threshold Experiment Results

Each edge value was tested for 1 hour in paper trading mode with a $100 portfolio.
Position sizing: MAX_POSITION_USD=5, MAX_TOTAL_EXPOSURE_USD=25, KELLY_FRACTION_CAP=0.25

## Summary

| Edge | Signals | Resolved | Wins | Losses | Win % | P&L | Portfolio | Return |
|------|---------|----------|------|--------|-------|-----|-----------|--------|
HEADER

for edge in "${EDGES[@]}"; do
    LOG_FILE="$LOG_DIR/edge-${edge}.log"
    [ -f "$LOG_FILE" ] || continue

    SIGNALS=$(grep -c "up/down signal" "$LOG_FILE" 2>/dev/null || echo "0")

    # Extract resolutions
    grep "updown market resolved" "$LOG_FILE" 2>/dev/null | sed 's/\x1b\[[0-9;]*m//g' > /tmp/resolved_${edge}.txt

    RESOLVED=$(wc -l < /tmp/resolved_${edge}.txt | tr -d ' ')
    WINS=$(grep -c "won=true" /tmp/resolved_${edge}.txt 2>/dev/null || echo "0")
    LOSSES=$(grep -c "won=false" /tmp/resolved_${edge}.txt 2>/dev/null || echo "0")

    if [ "$RESOLVED" -gt 0 ]; then
        WIN_RATE=$(echo "scale=1; $WINS * 100 / $RESOLVED" | bc)
    else
        WIN_RATE="n/a"
    fi

    # Get final P&L from shutdown line or last summary
    FINAL_LINE=$(grep "Execution engine shutting down" "$LOG_FILE" 2>/dev/null | sed 's/\x1b\[[0-9;]*m//g')
    if [ -z "$FINAL_LINE" ]; then
        FINAL_LINE=$(grep "paper trading summary" "$LOG_FILE" 2>/dev/null | sed 's/\x1b\[[0-9;]*m//g' | tail -1)
    fi

    if [ -n "$FINAL_LINE" ]; then
        REALIZED=$(echo "$FINAL_LINE" | sed -n 's/.*realized_pnl=\([^ ]*\).*/\1/p' | cut -c1-8)
        PORTFOLIO=$(echo "$FINAL_LINE" | sed -n 's/.*portfolio_value=\([^ ]*\).*/\1/p' | cut -c1-8)
        RETURN_PCT=$(echo "$FINAL_LINE" | sed -n 's/.*return_pct=\([^ ]*\).*/\1/p')
    else
        REALIZED="n/a"; PORTFOLIO="n/a"; RETURN_PCT="n/a"
    fi

    echo "| ${edge} | ${SIGNALS} | ${RESOLVED} | ${WINS} | ${LOSSES} | ${WIN_RATE}% | \$${REALIZED} | \$${PORTFOLIO} | ${RETURN_PCT} |" >> "$REPORT_FILE"
done

# Buy/Sell breakdown table
cat >> "$REPORT_FILE" << 'HEADER2'

## Buy (Up) vs Sell (Down) Breakdown

| Edge | Up Wins | Up Losses | Up Win % | Down Wins | Down Losses | Down Win % |
|------|---------|-----------|----------|-----------|-------------|------------|
HEADER2

for edge in "${EDGES[@]}"; do
    [ -f "/tmp/resolved_${edge}.txt" ] || continue

    BUY_WINS=$(grep -c "side=BUY.*won=true" /tmp/resolved_${edge}.txt 2>/dev/null || echo "0")
    BUY_LOSSES=$(grep -c "side=BUY.*won=false" /tmp/resolved_${edge}.txt 2>/dev/null || echo "0")
    BUY_TOTAL=$(($BUY_WINS + $BUY_LOSSES))

    SELL_WINS=$(grep -c "side=SELL.*won=true" /tmp/resolved_${edge}.txt 2>/dev/null || echo "0")
    SELL_LOSSES=$(grep -c "side=SELL.*won=false" /tmp/resolved_${edge}.txt 2>/dev/null || echo "0")
    SELL_TOTAL=$(($SELL_WINS + $SELL_LOSSES))

    if [ "$BUY_TOTAL" -gt 0 ]; then
        BUY_WIN_PCT=$(echo "scale=1; $BUY_WINS * 100 / $BUY_TOTAL" | bc)
    else
        BUY_WIN_PCT="n/a"
    fi

    if [ "$SELL_TOTAL" -gt 0 ]; then
        SELL_WIN_PCT=$(echo "scale=1; $SELL_WINS * 100 / $SELL_TOTAL" | bc)
    else
        SELL_WIN_PCT="n/a"
    fi

    echo "| ${edge} | ${BUY_WINS} | ${BUY_LOSSES} | ${BUY_WIN_PCT}% | ${SELL_WINS} | ${SELL_LOSSES} | ${SELL_WIN_PCT}% |" >> "$REPORT_FILE"
done

# Per-trade details
echo "" >> "$REPORT_FILE"
echo "## Individual Trades" >> "$REPORT_FILE"

for edge in "${EDGES[@]}"; do
    [ -f "/tmp/resolved_${edge}.txt" ] || continue
    echo "" >> "$REPORT_FILE"
    echo "### Edge = ${edge}" >> "$REPORT_FILE"
    echo "" >> "$REPORT_FILE"
    echo "| # | Side | Entry | Won | Spot | Start | P&L | Portfolio |" >> "$REPORT_FILE"
    echo "|---|------|-------|-----|------|-------|-----|-----------|" >> "$REPORT_FILE"

    n=1
    while IFS= read -r line; do
        SIDE=$(echo "$line" | sed -n 's/.*side=\([^ ]*\).*/\1/p')
        ENTRY=$(echo "$line" | sed -n 's/.*entry_price=\([^ ]*\).*/\1/p')
        WON=$(echo "$line" | sed -n 's/.*won=\([^ ]*\).*/\1/p')
        SPOT=$(echo "$line" | sed -n 's/.*spot=\([^ ]*\).*/\1/p' | cut -c1-10)
        START=$(echo "$line" | sed -n 's/.*start_price=\([^ ]*\).*/\1/p' | cut -c1-10)
        PNL=$(echo "$line" | sed -n 's/.*pnl=\([^ ]*\).*/\1/p' | cut -c1-8)
        PORT=$(echo "$line" | sed -n 's/.*portfolio=\([^ ]*\).*/\1/p' | cut -c1-8)

        SIDE_LABEL="$SIDE"
        if [ "$SIDE" = "BUY" ]; then SIDE_LABEL="Up"; fi
        if [ "$SIDE" = "SELL" ]; then SIDE_LABEL="Down"; fi

        RESULT="Loss"
        if [ "$WON" = "true" ]; then RESULT="Win"; fi

        echo "| $n | $SIDE_LABEL | $ENTRY | $RESULT | $SPOT | $START | \$$PNL | \$$PORT |" >> "$REPORT_FILE"
        n=$((n + 1))
    done < /tmp/resolved_${edge}.txt
done

echo "" >> "$REPORT_FILE"
echo "---" >> "$REPORT_FILE"
echo "*Generated: $(date)*" >> "$REPORT_FILE"

echo "Report saved to: $REPORT_FILE"
cat "$REPORT_FILE"
