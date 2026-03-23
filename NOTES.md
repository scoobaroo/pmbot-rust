# PMBot Development Notes

## Known Gotchas

### Risk Manager Position Limit (CRITICAL)
The CLI arg `--live-max-position-usd` defaults to $200 in `src/config.rs`. This is a HARD CAP enforced by `src/execution/risk.rs` — any trade above this gets silently rejected with "position limit exceeded". The strategy's own sizing logic (bankroll %, tiered fractions) is irrelevant if this cap is lower.

**If trades aren't going through, check this first.**

Related env vars:
- `MAX_POSITION_USD` — config-level max (default $1000, capped by live limit)
- `MAX_TOTAL_EXPOSURE_USD` — total across all positions (default $5000, capped at 5x live limit)
- `BANKROLL` — ORB strategy bankroll for percentage-based sizing

### Duplicate Bot Instances
Always use `scripts/run_bot.sh` to launch — it manages a PID file at `data/bot.pid` and kills stale processes. Running `cargo run` directly risks duplicates.

### Polymarket API Downtime
The heartbeat endpoint goes down periodically (returns HTML instead of JSON). Orders placed during downtime may silently fail. The bot logs `heartbeat failed` — if you see this continuously, orders aren't going through.

### Rejection Log File Size
`data/orb_rejections.csv` grows fast (~1M rows/hour). The logger now filters moves < 0.03% but can still grow large. Delete it periodically: `rm data/orb_rejections.csv`

### Entered Markets HashSet
`entered_markets` in the ORB strategy grows forever — each condition_id is added permanently. This prevents duplicate orders but also means the HashSet grows by ~500 entries/day. Not a memory issue but worth knowing.

## Current Strategy Parameters

### Entry Filters (in order)
1. 5-minute windows only (no 15m)
2. BTC, ETH, SOL, XRP
3. Min move: BTC 0.05%, ETH 0.08%, SOL/XRP 0.10%
4. Flow >= 0.20 (direction must match)
5. Momentum: allow 20% pullback, block >20% reversal
6. Price cap: 56c max entry
7. $200 hard cap per trade
8. $200 max total exposure

### What's NOT filtering (data only for RL)
- RSI (14-period on 1m candles)
- MACD (12, 26, 9 on 1m candles)
- Funding rate
- Regime (30-candle trend)
- Chainlink oracle

### Risk Management
- Circuit breaker: 2 losses = slow mode, 4 losses = 30min pause
- Stale order auto-cancel after 30 seconds
- DOWN_ONLY mode available via env var

## Key Learnings from Live Trading

### What Works
- Velocity entries at sub-50c = 89-95% win rate
- Binance spot price is fastest signal
- Flow >= 0.20 confirms real volume
- 5-minute windows = fast resolution, more cycles

### What Doesn't Work
- Entries above 60c = net negative P&L
- Oracle/Chainlink check blocks too many trades (slow updates)
- RSI/MACD as hard filters on 5m timeframe
- 15m windows = double exposure on same move
- +5c aggressive pricing wastes money

### Performance
- $400 -> $2,219 in first 24 hours
- 77% win rate on 297 resolved trades (deduplicated)
- Best edge: entries under 30c (95% win, +$424)
- BTC is primary profit driver (+$2,007)
- SOL (89%) and XRP (90%) also strong

## File Locations
- Bot log: `data/bot_orb.log`
- Trade entries: `data/orb_trades.csv`
- Trade exits: `data/orb_exits.csv`
- Rejections: `data/orb_rejections.csv`
- VPS data: `data/polymarket/*.csv` (10 files)
- Trade history: `data/trades_history.csv` (from backfill script)
- Dashboard: `http://localhost:3000`
