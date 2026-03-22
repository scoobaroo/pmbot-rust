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
    pip install requests websocket-client
"""

import csv
import json
import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import websocket

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


BINANCE_HEADER = [
    "timestamp", "symbol", "price", "bid", "ask", "volume_24h",
    "quote_volume_24h", "trades_24h", "price_change_pct",
]
FLOW_HEADER = [
    "timestamp", "symbol", "buy_volume", "sell_volume", "net_flow",
    "buy_trades", "sell_trades", "vwap_buy", "vwap_sell",
]
FUNDING_HEADER = [
    "timestamp", "symbol", "funding_rate", "mark_price", "index_price",
    "next_funding_time",
]

markets_f, markets_w = open_csv("markets.csv", MARKETS_HEADER)
ticks_f, ticks_w = open_csv("ticks.csv", TICKS_HEADER)
resolutions_f, resolutions_w = open_csv("resolutions.csv", RESOLUTIONS_HEADER)
binance_f, binance_w = open_csv("binance.csv", BINANCE_HEADER)
flow_f, flow_w = open_csv("flow.csv", FLOW_HEADER)
funding_f, funding_w = open_csv("funding.csv", FUNDING_HEADER)
binance_lock = threading.Lock()

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
active_markets: dict = {}  # condition_id -> market dict
seen_condition_ids: set = set()  # all condition_ids we've ever seen
stats = {"markets": 0, "ticks": 0, "resolutions": 0, "binance_ticks": 0, "flow_ticks": 0, "funding_ticks": 0}
latest_binance: dict = {}  # symbol -> {price, bid, ask, ...}
latest_funding: dict = {}  # symbol -> {rate, mark, index}

# Rolling 1-second flow buckets for aggTrade aggregation
flow_buckets: dict = {}  # symbol -> {buy_vol, sell_vol, buy_cost, sell_cost, buy_n, sell_n, last_flush}
FLOW_FLUSH_INTERVAL = 1.0  # flush flow data every 1 second

# ---------------------------------------------------------------------------
# Binance WebSocket (runs in background thread)
# ---------------------------------------------------------------------------
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
BINANCE_SYMBOLS = ["btcusdt", "ethusdt"]
BINANCE_SYMBOL_MAP = {"BTCUSDT": "BTC-USD", "ETHUSDT": "ETH-USD"}


def flush_flow_bucket(symbol: str):
    """Write accumulated flow data for a symbol and reset the bucket."""
    bucket = flow_buckets.get(symbol)
    if not bucket or (bucket["buy_n"] == 0 and bucket["sell_n"] == 0):
        return

    buy_vol = bucket["buy_vol"]
    sell_vol = bucket["sell_vol"]
    net_flow = buy_vol - sell_vol
    vwap_buy = bucket["buy_cost"] / buy_vol if buy_vol > 0 else 0
    vwap_sell = bucket["sell_cost"] / sell_vol if sell_vol > 0 else 0

    now = datetime.now(timezone.utc).isoformat()
    with binance_lock:
        flow_w.writerow([
            now, symbol,
            f"{buy_vol:.4f}", f"{sell_vol:.4f}", f"{net_flow:.4f}",
            bucket["buy_n"], bucket["sell_n"],
            f"{vwap_buy:.2f}", f"{vwap_sell:.2f}",
        ])
        stats["flow_ticks"] += 1
        if stats["flow_ticks"] % 100 == 0:
            flow_f.flush()

    # Reset bucket
    flow_buckets[symbol] = {
        "buy_vol": 0, "sell_vol": 0, "buy_cost": 0, "sell_cost": 0,
        "buy_n": 0, "sell_n": 0, "last_flush": time.time(),
    }


def start_binance_spot_ws():
    """Spot streams: ticker (price) + aggTrade (buy/sell flow)."""
    ticker_streams = [f"{s}@ticker" for s in BINANCE_SYMBOLS]
    agg_streams = [f"{s}@aggTrade" for s in BINANCE_SYMBOLS]
    all_streams = "/".join(ticker_streams + agg_streams)
    url = f"wss://stream.binance.com:9443/stream?streams={all_streams}"

    def on_message(ws, message):
        try:
            msg = json.loads(message)
            stream = msg.get("stream", "")
            data = msg.get("data", msg)
            symbol_raw = data.get("s", "")
            symbol = BINANCE_SYMBOL_MAP.get(symbol_raw, symbol_raw)

            if "@ticker" in stream:
                # Price tick
                tick = {
                    "price": float(data.get("c", 0)),
                    "bid": float(data.get("b", 0)),
                    "ask": float(data.get("a", 0)),
                    "volume_24h": float(data.get("v", 0)),
                    "quote_volume_24h": float(data.get("q", 0)),
                    "trades_24h": int(data.get("n", 0)),
                    "price_change_pct": float(data.get("P", 0)),
                }
                latest_binance[symbol] = tick

                now = datetime.now(timezone.utc).isoformat()
                with binance_lock:
                    binance_w.writerow([
                        now, symbol,
                        f"{tick['price']:.2f}", f"{tick['bid']:.2f}", f"{tick['ask']:.2f}",
                        f"{tick['volume_24h']:.2f}", f"{tick['quote_volume_24h']:.0f}",
                        tick["trades_24h"], f"{tick['price_change_pct']:.2f}",
                    ])
                    stats["binance_ticks"] += 1
                    if stats["binance_ticks"] % 100 == 0:
                        binance_f.flush()

            elif "@aggTrade" in stream:
                # Aggregate trade — accumulate into flow bucket
                price = float(data.get("p", 0))
                qty = float(data.get("q", 0))
                is_buyer_maker = data.get("m", False)  # True = sell, False = buy

                if symbol not in flow_buckets:
                    flow_buckets[symbol] = {
                        "buy_vol": 0, "sell_vol": 0, "buy_cost": 0, "sell_cost": 0,
                        "buy_n": 0, "sell_n": 0, "last_flush": time.time(),
                    }

                bucket = flow_buckets[symbol]
                if is_buyer_maker:
                    # Seller initiated (taker sell)
                    bucket["sell_vol"] += qty
                    bucket["sell_cost"] += qty * price
                    bucket["sell_n"] += 1
                else:
                    # Buyer initiated (taker buy)
                    bucket["buy_vol"] += qty
                    bucket["buy_cost"] += qty * price
                    bucket["buy_n"] += 1

                # Flush every second
                if time.time() - bucket["last_flush"] >= FLOW_FLUSH_INTERVAL:
                    flush_flow_bucket(symbol)

        except Exception as e:
            log.debug(f"Binance spot WS parse error: {e}")

    def on_error(ws, error):
        log.warning(f"Binance spot WS error: {error}")

    def on_close(ws, close_status, close_msg):
        log.warning(f"Binance spot WS closed: {close_status} {close_msg}")
        time.sleep(3)
        start_binance_spot_ws()

    def on_open(ws):
        log.info("Binance spot WebSocket connected (ticker + aggTrade)")

    ws = websocket.WebSocketApp(url, on_message=on_message, on_error=on_error,
                                 on_close=on_close, on_open=on_open)
    threading.Thread(target=ws.run_forever, daemon=True).start()


def start_binance_futures_ws():
    """Futures streams: mark price (funding rate, mark/index price) every 1s."""
    futures_symbols = ["btcusdt", "ethusdt"]
    streams = "/".join(f"{s}@markPrice@1s" for s in futures_symbols)
    url = f"wss://fstream.binance.com/stream?streams={streams}"

    def on_message(ws, message):
        try:
            msg = json.loads(message)
            data = msg.get("data", msg)
            symbol_raw = data.get("s", "")
            symbol = BINANCE_SYMBOL_MAP.get(symbol_raw, symbol_raw)

            funding_rate = float(data.get("r", 0))
            mark_price = float(data.get("p", 0))
            index_price = float(data.get("i", 0))
            next_funding = int(data.get("T", 0))

            latest_funding[symbol] = {
                "rate": funding_rate,
                "mark": mark_price,
                "index": index_price,
            }

            now = datetime.now(timezone.utc).isoformat()
            with binance_lock:
                funding_w.writerow([
                    now, symbol,
                    f"{funding_rate:.8f}", f"{mark_price:.2f}", f"{index_price:.2f}",
                    next_funding,
                ])
                stats["funding_ticks"] += 1
                if stats["funding_ticks"] % 100 == 0:
                    funding_f.flush()

        except Exception as e:
            log.debug(f"Binance futures WS parse error: {e}")

    def on_error(ws, error):
        log.warning(f"Binance futures WS error: {error}")

    def on_close(ws, close_status, close_msg):
        log.warning(f"Binance futures WS closed: {close_status} {close_msg}")
        time.sleep(3)
        start_binance_futures_ws()

    def on_open(ws):
        log.info("Binance futures WebSocket connected (funding + mark price)")

    ws = websocket.WebSocketApp(url, on_message=on_message, on_error=on_error,
                                 on_close=on_close, on_open=on_open)
    threading.Thread(target=ws.run_forever, daemon=True).start()

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

    # Start Binance WebSockets for real-time data
    log.info("Starting Binance spot WS (price + aggTrade flow)...")
    start_binance_spot_ws()
    log.info("Starting Binance futures WS (funding rate + mark price)...")
    start_binance_futures_ws()

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
                with binance_lock:
                    binance_f.flush()
                    flow_f.flush()
                    funding_f.flush()
                prices = " | ".join(
                    f"{s}=${t['price']:.0f}" for s, t in latest_binance.items()
                )
                funding_info = " | ".join(
                    f"{s} fr={t['rate']:.6f}" for s, t in latest_funding.items()
                )
                log.info(
                    f"Status: {len(active_markets)} active, "
                    f"{stats['markets']} disc, {stats['ticks']} poly, "
                    f"{stats['binance_ticks']} price, {stats['flow_ticks']} flow, "
                    f"{stats['funding_ticks']} funding, "
                    f"{stats['resolutions']} resolved | {prices} | {funding_info}"
                )

            time.sleep(BOOK_INTERVAL)

    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        ticks_f.flush()
        with binance_lock:
            binance_f.flush()
            flow_f.flush()
            funding_f.flush()
        for f in [markets_f, ticks_f, resolutions_f, binance_f, flow_f, funding_f]:
            f.close()
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
