#!/usr/bin/env python3
"""
Backfill trade history from Polymarket API.

Pulls all trades and redemptions for a wallet, matches entries to outcomes,
and generates a clean CSV for RL training and analysis.

Usage:
    python3 scripts/backfill_trades.py [wallet_address]

    # Default: uses COPYTRADE_FUNDER_ADDRESS from .env
    python3 scripts/backfill_trades.py

    # Specific wallet:
    python3 scripts/backfill_trades.py 0x961ee89122a43c9b783805c6b52bde02bd8457af

Output: data/trades_history.csv
"""

import csv
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests

DATA_API = "https://data-api.polymarket.com"
OUTPUT = Path("data/trades_history.csv")

HEADER = [
    "timestamp", "condition_id", "question", "symbol", "outcome_bought",
    "side", "num_fills", "total_size", "total_usdc", "avg_price",
    "result", "pnl_usdc", "pnl_pct", "hold_secs",
    "window_secs", "slug",
]


def get_wallet():
    """Get wallet from args or .env."""
    if len(sys.argv) > 1:
        return sys.argv[1].lower()
    # Try .env
    env_path = Path(".env")
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("COPYTRADE_FUNDER_ADDRESS="):
                return line.split("=", 1)[1].strip().lower()
    print("Usage: python3 scripts/backfill_trades.py <wallet_address>")
    sys.exit(1)


def fetch_all_activity(wallet: str) -> list[dict]:
    """Fetch all activity pages from Polymarket API."""
    all_data = []
    offset = 0
    limit = 500
    while True:
        url = f"{DATA_API}/activity?user={wallet}&limit={limit}&offset={offset}&sortDirection=ASC"
        print(f"  Fetching offset={offset}...", end=" ")
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        print(f"{len(data)} events")
        if not data:
            break
        all_data.extend(data)
        if len(data) < limit:
            break
        offset += limit
        time.sleep(0.5)  # rate limit
    return all_data


def parse_symbol(title: str) -> str:
    title_lower = title.lower()
    if "bitcoin" in title_lower or "btc" in title_lower:
        return "BTC-USD"
    elif "ethereum" in title_lower or "ether" in title_lower:
        return "ETH-USD"
    elif "solana" in title_lower or "sol" in title_lower:
        return "SOL-USD"
    elif "xrp" in title_lower:
        return "XRP-USD"
    elif "dogecoin" in title_lower or "doge" in title_lower:
        return "DOGE-USD"
    elif "bnb" in title_lower:
        return "BNB-USD"
    elif "hyperliquid" in title_lower:
        return "HYPE-USD"
    return "UNKNOWN"


def parse_window(title: str, slug: str) -> int:
    if "5m" in slug or ("-" in title and len(title.split("-")[-1].strip()) < 15):
        return 300
    if "15m" in slug:
        return 900
    return 0


def main():
    wallet = get_wallet()
    print(f"Backfilling trades for: {wallet}")
    print()

    # Fetch all activity
    print("Fetching activity from Polymarket API...")
    activity = fetch_all_activity(wallet)
    print(f"Total events: {len(activity)}")
    print()

    # Group by condition_id
    trades_by_cid: dict[str, list] = defaultdict(list)
    redeems_by_cid: dict[str, dict] = {}

    for event in activity:
        cid = event.get("conditionId", "")
        etype = event.get("type", "")
        if etype == "TRADE":
            trades_by_cid[cid].append(event)
        elif etype == "REDEEM":
            redeems_by_cid[cid] = event

    print(f"Unique markets traded: {len(trades_by_cid)}")
    print(f"Markets redeemed: {len(redeems_by_cid)}")
    print()

    # Build positions
    positions = []
    for cid, fills in trades_by_cid.items():
        # Aggregate fills into one position
        buy_fills = [f for f in fills if f.get("side") == "BUY"]
        if not buy_fills:
            continue

        total_usdc = sum(float(f.get("usdcSize", 0) or 0) for f in buy_fills)
        total_size = sum(float(f.get("size", 0) or 0) for f in buy_fills)
        avg_price = total_usdc / total_size if total_size > 0 else 0

        first = buy_fills[0]
        last = buy_fills[-1]
        title = first.get("title", "") or ""
        slug = first.get("slug", "") or ""
        outcome = first.get("outcome", "") or ""
        side = "BUY" if outcome in ("Up", "Yes") else "SELL"
        symbol = parse_symbol(title)
        window_secs = parse_window(title, slug)
        entry_ts = first.get("timestamp", 0)

        # Check redemption
        redeem = redeems_by_cid.get(cid)
        if redeem:
            redeem_size = float(redeem.get("size", 0) or 0)
            redeem_usdc = float(redeem.get("usdcSize", 0) or 0)
            redeem_ts = redeem.get("timestamp", 0)
            hold_secs = redeem_ts - entry_ts if redeem_ts and entry_ts else 0

            if redeem_size > 0 or redeem_usdc > 0:
                result = "WON"
                # Payout is $1 per token
                pnl = total_size - total_usdc  # tokens * $1 - cost
                pnl_pct = (pnl / total_usdc * 100) if total_usdc > 0 else 0
            else:
                result = "LOST"
                pnl = -total_usdc
                pnl_pct = -100.0
        else:
            result = "OPEN"
            pnl = 0
            pnl_pct = 0
            hold_secs = 0

        ts = datetime.fromtimestamp(entry_ts, tz=timezone.utc).isoformat() if entry_ts else ""

        positions.append({
            "timestamp": ts,
            "condition_id": cid,
            "question": title[:80],
            "symbol": symbol,
            "outcome_bought": outcome,
            "side": side,
            "num_fills": len(buy_fills),
            "total_size": total_size,
            "total_usdc": total_usdc,
            "avg_price": avg_price,
            "result": result,
            "pnl_usdc": pnl,
            "pnl_pct": pnl_pct,
            "hold_secs": hold_secs,
            "window_secs": window_secs,
            "slug": slug,
        })

    # Sort by timestamp
    positions.sort(key=lambda p: p["timestamp"])

    # Write CSV
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)
        writer.writeheader()
        writer.writerows(positions)

    # Summary
    resolved = [p for p in positions if p["result"] in ("WON", "LOST")]
    wins = [p for p in resolved if p["result"] == "WON"]
    losses = [p for p in resolved if p["result"] == "LOST"]
    open_pos = [p for p in positions if p["result"] == "OPEN"]
    total_pnl = sum(p["pnl_usdc"] for p in resolved)
    total_spent = sum(p["total_usdc"] for p in resolved)
    win_rate = len(wins) / len(resolved) * 100 if resolved else 0

    print(f"=== Trade History Summary ===")
    print(f"Total positions: {len(positions)}")
    print(f"Resolved: {len(resolved)} ({len(wins)}W / {len(losses)}L)")
    print(f"Open: {len(open_pos)}")
    print(f"Win rate: {win_rate:.1f}%")
    print(f"Total spent: ${total_spent:.2f}")
    print(f"Total P&L: ${total_pnl:+.2f}")
    print(f"Avg entry price: ${sum(p['avg_price'] for p in resolved)/len(resolved):.3f}" if resolved else "")
    print(f"Avg win: ${sum(p['pnl_usdc'] for p in wins)/len(wins):+.2f}" if wins else "")
    print(f"Avg loss: ${sum(p['pnl_usdc'] for p in losses)/len(losses):+.2f}" if losses else "")
    print()

    # By symbol
    print("By symbol:")
    symbols = set(p["symbol"] for p in resolved)
    for sym in sorted(symbols):
        sym_pos = [p for p in resolved if p["symbol"] == sym]
        sym_wins = [p for p in sym_pos if p["result"] == "WON"]
        sym_pnl = sum(p["pnl_usdc"] for p in sym_pos)
        wr = len(sym_wins) / len(sym_pos) * 100 if sym_pos else 0
        print(f"  {sym:8} {len(sym_pos):3} trades | {wr:.0f}% win | ${sym_pnl:+.2f}")

    # By window
    print("\nBy window:")
    for ws in [300, 900]:
        ws_pos = [p for p in resolved if p["window_secs"] == ws]
        ws_wins = [p for p in ws_pos if p["result"] == "WON"]
        ws_pnl = sum(p["pnl_usdc"] for p in ws_pos)
        wr = len(ws_wins) / len(ws_pos) * 100 if ws_pos else 0
        label = "5m" if ws == 300 else "15m"
        print(f"  {label:4} {len(ws_pos):3} trades | {wr:.0f}% win | ${ws_pnl:+.2f}")

    # By entry price tier
    print("\nBy entry price:")
    tiers = [(0, 0.3, "<30c"), (0.3, 0.5, "30-50c"), (0.5, 0.7, "50-70c"), (0.7, 1.0, ">70c")]
    for low, high, label in tiers:
        tier_pos = [p for p in resolved if low <= p["avg_price"] < high]
        tier_wins = [p for p in tier_pos if p["result"] == "WON"]
        tier_pnl = sum(p["pnl_usdc"] for p in tier_pos)
        wr = len(tier_wins) / len(tier_pos) * 100 if tier_pos else 0
        print(f"  {label:6} {len(tier_pos):3} trades | {wr:.0f}% win | ${tier_pnl:+.2f}")

    print(f"\nOutput: {OUTPUT}")


if __name__ == "__main__":
    main()
