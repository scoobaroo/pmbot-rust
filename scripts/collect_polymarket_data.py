#!/usr/bin/env python3
"""
Polymarket UpDown Market Data Collector

Collects orderbook snapshots, price ticks, volume, and resolution outcomes
for BTC/ETH UpDown markets. Saves to CSV files for offline analysis.

Usage:
    python3 scripts/collect_polymarket_data.py

Data files (in data/polymarket/):
    markets.csv       - All discovered markets with metadata
    orderbook.csv     - Orderbook snapshots (best bid/ask, depth, spread)
    resolutions.csv   - Market outcomes (up/down, final prices)
    ticks.csv         - Price ticks with implied probabilities

Deployment on VPS:
    nohup python3 scripts/collect_polymarket_data.py > data/collector.log 2>&1 &

Requirements:
    pip install requests
"""

import csv
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
DATA_DIR = Path("data/polymarket")
ASSETS = [("btc", "BTC-USD"), ("eth", "ETH-USD")]
WINDOWS = [(300, "5m"), (900, "15m")]
BOOK_INTERVAL = 2  # seconds between orderbook snapshots

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("collector")

# ---------------------------------------------------------------------------
# CSV Setup
# ---------------------------------------------------------------------------
DATA_DIR.mkdir(parents=True, exist_ok=True)

MARKETS_HEADER = [
    "timestamp", "condition_id", "question", "symbol", "window_secs",
    "window_start_ts", "token_id_yes", "token_id_no",
    "implied_yes", "implied_no", "price_to_beat",
]
TICKS_HEADER = [
    "timestamp", "condition_id", "symbol", "window_secs",
    "yes_bid", "yes_ask", "yes_mid", "no_bid", "no_ask", "no_mid",
    "yes_bid_depth", "yes_ask_depth", "no_bid_depth", "no_ask_depth",
    "elapsed_secs", "time_remaining_secs",
]
RESOLUTIONS_HEADER = [
    "timestamp", "condition_id", "question", "symbol", "window_secs",
    "window_start_ts", "outcome", "final_yes_mid", "final_no_mid",
    "price_to_beat",
]


def open_csv(filename: str, header: list[str]):
    path = DATA_DIR / filename
    exists = path.exists() and path.stat().st_size > 0
    f = open(path, "a", newline="")
    writer = csv.writer(f)
    if not exists:
        writer.writerow(header)
        f.flush()
    return f, writer


markets_f, markets_w = open_csv("markets.csv", MARKETS_HEADER)
ticks_f, ticks_w = open_csv("ticks.csv", TICKS_HEADER)
resolutions_f, resolutions_w = open_csv("resolutions.csv", RESOLUTIONS_HEADER)

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
active_markets: dict = {}  # condition_id -> market dict
seen_condition_ids: set = set()  # all condition_ids we've ever seen
stats = {"markets": 0, "ticks": 0, "resolutions": 0}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def current_window_start(window_secs: int) -> int:
    now = int(time.time())
    return now - (now % window_secs)


def fetch_book(token_id: str) -> dict | None:
    try:
        r = requests.get(f"{CLOB_API}/book", params={"token_id": token_id}, timeout=5)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def parse_book(book: dict | None) -> tuple[float, float, float, float]:
    """Returns (best_bid, best_ask, mid, total_depth)."""
    if not book:
        return 0, 0, 0, 0
    bids = book.get("bids", [])
    asks = book.get("asks", [])
    best_bid = float(bids[0]["price"]) if bids else 0
    best_ask = float(asks[0]["price"]) if asks else 0
    mid = (best_bid + best_ask) / 2 if best_bid and best_ask else 0
    depth = sum(float(l.get("size", 0)) for l in bids)
    return best_bid, best_ask, mid, depth


# ---------------------------------------------------------------------------
# Market Discovery (slug-based, same as Rust bot)
# ---------------------------------------------------------------------------
def discover_markets() -> list[dict]:
    found = []
    for window_secs, label in WINDOWS:
        ws_start = current_window_start(window_secs)
        for slug_prefix, symbol in ASSETS:
            slug = f"{slug_prefix}-updown-{label}-{ws_start}"
            try:
                r = requests.get(
                    f"{GAMMA_API}/events",
                    params={"slug": slug},
                    timeout=10,
                )
                r.raise_for_status()
                events = r.json()
            except Exception as e:
                log.debug(f"Failed to fetch {slug}: {e}")
                continue

            for event in events:
                nested = event.get("markets", [])
                price_to_beat = None
                em = event.get("eventMetadata")
                if em:
                    ptb = em.get("priceToBeat")
                    if ptb is not None:
                        try:
                            price_to_beat = float(ptb)
                        except (ValueError, TypeError):
                            pass

                for m in nested:
                    cid = m.get("conditionId", "")
                    if not cid or cid in seen_condition_ids:
                        continue

                    outcomes = m.get("outcomes", "[]")
                    if isinstance(outcomes, str):
                        import json
                        outcomes = json.loads(outcomes)
                    tokens_str = m.get("clobTokenIds", "[]")
                    if isinstance(tokens_str, str):
                        import json
                        tokens = json.loads(tokens_str)
                    else:
                        tokens = tokens_str

                    if len(tokens) < 2:
                        continue

                    prices_str = m.get("outcomePrices", "[]")
                    if isinstance(prices_str, str):
                        import json
                        prices = json.loads(prices_str)
                    else:
                        prices = prices_str

                    yes_price = float(prices[0]) if prices else 0.5
                    no_price = float(prices[1]) if len(prices) > 1 else 1 - yes_price

                    found.append({
                        "condition_id": cid,
                        "question": m.get("question", ""),
                        "symbol": symbol,
                        "window_secs": window_secs,
                        "window_start_ts": ws_start,
                        "token_id_yes": tokens[0],
                        "token_id_no": tokens[1],
                        "yes_price": yes_price,
                        "no_price": no_price,
                        "price_to_beat": price_to_beat,
                    })
    return found


# ---------------------------------------------------------------------------
# Recording
# ---------------------------------------------------------------------------
def record_market(m: dict):
    now = datetime.now(timezone.utc).isoformat()
    markets_w.writerow([
        now, m["condition_id"], m["question"], m["symbol"],
        m["window_secs"], m["window_start_ts"],
        m["token_id_yes"][:30], m["token_id_no"][:30],
        f"{m['yes_price']:.4f}", f"{m['no_price']:.4f}",
        m.get("price_to_beat", ""),
    ])
    markets_f.flush()


def record_tick(m: dict, yes_book: dict | None, no_book: dict | None):
    now = datetime.now(timezone.utc)
    now_ts = int(now.timestamp())
    elapsed = now_ts - m["window_start_ts"]
    remaining = m["window_secs"] - elapsed

    yb, ya, ym, yd = parse_book(yes_book)
    nb, na, nm, nd = parse_book(no_book)

    # Update stored prices
    if ym > 0:
        m["yes_price"] = ym
    if nm > 0:
        m["no_price"] = nm

    ticks_w.writerow([
        now.isoformat(), m["condition_id"], m["symbol"], m["window_secs"],
        f"{yb:.4f}", f"{ya:.4f}", f"{ym:.4f}",
        f"{nb:.4f}", f"{na:.4f}", f"{nm:.4f}",
        f"{yd:.1f}", f"{0:.1f}",  # yes depth
        f"{nd:.1f}", f"{0:.1f}",  # no depth
        elapsed, max(0, remaining),
    ])


def record_resolution(m: dict):
    now = datetime.now(timezone.utc).isoformat()
    outcome = "UP" if m["yes_price"] > 0.5 else "DOWN"
    resolutions_w.writerow([
        now, m["condition_id"], m["question"], m["symbol"],
        m["window_secs"], m["window_start_ts"],
        outcome, f"{m['yes_price']:.4f}", f"{m['no_price']:.4f}",
        m.get("price_to_beat", ""),
    ])
    resolutions_f.flush()
    return outcome


# ---------------------------------------------------------------------------
# Main Loop
# ---------------------------------------------------------------------------
def main():
    log.info("Polymarket Data Collector starting")
    log.info(f"Assets: {[s for _, s in ASSETS]}, Windows: {[l for _, l in WINDOWS]}")
    log.info(f"Data dir: {DATA_DIR.resolve()}")

    last_discovery = 0
    last_status = 0

    try:
        while True:
            now = time.time()

            # Discover new markets every 5s
            if now - last_discovery >= 5:
                last_discovery = now
                new_markets = discover_markets()
                for m in new_markets:
                    cid = m["condition_id"]
                    seen_condition_ids.add(cid)
                    active_markets[cid] = m
                    record_market(m)
                    stats["markets"] += 1
                    log.info(
                        f"NEW {m['symbol']} {m['question'][:55]} "
                        f"yes={m['yes_price']:.2f} window={m['window_secs']}s"
                    )

            # Fetch orderbooks and record ticks
            now_ts = int(now)
            expired = []
            for cid, m in active_markets.items():
                end_ts = m["window_start_ts"] + m["window_secs"]
                if now_ts > end_ts + 10:
                    # Market expired — record resolution
                    outcome = record_resolution(m)
                    stats["resolutions"] += 1
                    expired.append(cid)
                    log.info(
                        f"RESOLVED {m['symbol']} -> {outcome} "
                        f"yes={m['yes_price']:.2f} {m['question'][:40]}"
                    )
                    continue

                # Fetch orderbooks for both sides
                yes_book = fetch_book(m["token_id_yes"])
                no_book = fetch_book(m["token_id_no"])
                record_tick(m, yes_book, no_book)
                stats["ticks"] += 1

            for cid in expired:
                del active_markets[cid]

            # Periodic flush and status
            if now - last_status >= 60:
                last_status = now
                ticks_f.flush()
                log.info(
                    f"Status: {len(active_markets)} active, "
                    f"{stats['markets']} discovered, {stats['ticks']} ticks, "
                    f"{stats['resolutions']} resolved"
                )

            time.sleep(BOOK_INTERVAL)

    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        ticks_f.flush()
        markets_f.close()
        ticks_f.close()
        resolutions_f.close()
        log.info(f"Final: {stats}")


if __name__ == "__main__":
    main()

# ---------------------------------------------------------------------------
# Systemd unit (save as /etc/systemd/system/polymarket-collector.service):
#
# [Unit]
# Description=Polymarket Data Collector
# After=network.target
#
# [Service]
# Type=simple
# User=deploy
# WorkingDirectory=/home/deploy/pmbot-rust
# ExecStart=/usr/bin/python3 scripts/collect_polymarket_data.py
# Restart=always
# RestartSec=10
#
# [Install]
# WantedBy=multi-user.target
# ---------------------------------------------------------------------------
