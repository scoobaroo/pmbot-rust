# Polymarket Data Collector — VPS Commands

## Server
```
ssh root@your-vps-ip
cd ~/pmbot-rust
```

## Status

```bash
# Is collector running?
ps aux | grep collect_polymarket | grep -v grep

# Live log output
tail -f data/collector.log

# Last 20 log lines
tail -20 data/collector.log

# How much data collected
wc -l data/polymarket/*.csv

# Disk usage
du -sh data/polymarket/
```

## Start / Stop / Restart

```bash
# Start
cd ~/pmbot-rust
nohup venv/bin/python scripts/collect_polymarket_data.py > data/collector.log 2>&1 &

# Stop
kill $(pgrep -f collect_polymarket)

# Restart
kill $(pgrep -f collect_polymarket); sleep 1
nohup venv/bin/python scripts/collect_polymarket_data.py > data/collector.log 2>&1 &
```

## View Data

```bash
# Binance prices (real-time BTC/ETH)
tail -10 data/polymarket/binance.csv

# Buy/sell flow (aggTrade volume per second)
tail -10 data/polymarket/flow.csv

# Funding rate (perpetual futures)
tail -10 data/polymarket/funding.csv

# Polymarket orderbook snapshots
tail -10 data/polymarket/ticks.csv

# Discovered markets
tail -10 data/polymarket/markets.csv

# Resolutions (UP/DOWN outcomes)
cat data/polymarket/resolutions.csv

# Count resolutions
grep -c "UP\|DOWN" data/polymarket/resolutions.csv
```

## Quick Analysis

```bash
# BTC price right now
tail -1 data/polymarket/binance.csv | grep BTC

# Current funding rate
tail -1 data/polymarket/funding.csv | grep BTC

# Recent buy vs sell flow
tail -20 data/polymarket/flow.csv | grep BTC

# Win rate of UP vs DOWN resolutions
echo "UP:"; grep -c ",UP," data/polymarket/resolutions.csv
echo "DOWN:"; grep -c ",DOWN," data/polymarket/resolutions.csv

# How many markets per hour
awk -F, '{print substr($1,1,13)}' data/polymarket/markets.csv | sort | uniq -c | tail -10

# Data file sizes
ls -lh data/polymarket/*.csv
```

## Download Data to Mac

```bash
# Run from your Mac (not the VPS):
scp -r root@your-vps-ip:~/pmbot-rust/data/polymarket/ ~/Desktop/polymarket-data/

# Download just one file
scp root@your-vps-ip:~/pmbot-rust/data/polymarket/resolutions.csv ~/Desktop/
```

## Update Collector

```bash
cd ~/pmbot-rust
git pull
kill $(pgrep -f collect_polymarket)
sleep 1
nohup venv/bin/python scripts/collect_polymarket_data.py > data/collector.log 2>&1 &
```

## Auto-Start on Reboot

```bash
crontab -e
# Add this line:
@reboot cd /root/pmbot-rust && /root/pmbot-rust/venv/bin/python scripts/collect_polymarket_data.py >> data/collector.log 2>&1
```

## Troubleshooting

```bash
# Collector died? Check last error
tail -50 data/collector.log | grep -i "error\|traceback\|exception"

# Port blocked? Test Binance WS
python3 -c "import websocket; ws=websocket.create_connection('wss://stream.binance.com:9443/ws'); print('OK'); ws.close()"

# Disk full?
df -h

# Clear old data (keep last 24h)
# WARNING: deletes data
head -1 data/polymarket/binance.csv > /tmp/header.csv
tail -86400 data/polymarket/binance.csv >> /tmp/header.csv
mv /tmp/header.csv data/polymarket/binance.csv
```

## Data Files Reference

| File | Contents | Frequency |
|------|----------|-----------|
| `binance.csv` | BTC/ETH price, bid, ask, 24h volume | ~1/sec |
| `flow.csv` | Buy vs sell volume, net flow, VWAP | 1/sec |
| `funding.csv` | Funding rate, mark price, index price | 1/sec |
| `ticks.csv` | Polymarket orderbook (bid/ask/mid/depth) | 2/sec |
| `markets.csv` | Discovered UpDown markets + metadata | On discovery |
| `resolutions.csv` | Market outcomes (UP/DOWN) | On expiry |
