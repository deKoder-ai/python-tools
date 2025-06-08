#!/usr/bin/env python3
import argparse
import time
import os
import pandas as pd
import ccxt
from datetime import datetime

# Config
RATE_LIMIT_DELAY = 0.5  # Safe for Bybit
BYBIT_MAX_CANDLES_PER_FETCH = 1000

def parse_args():
    parser = argparse.ArgumentParser(description="Fetching Bybit candles blud.")
    parser.add_argument("symbol", type=str, help="Symbol (e.g., 'BTC/USDT:USDT')")
    parser.add_argument("timeframe", type=str, help="Timeframe (e.g., '5m', '1h')")
    return parser.parse_args()

def validate_timeframe(timeframe):
    valid = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h",
             "1d", "3d", "1w", "1M"]
    if timeframe not in valid:
        raise ValueError(f"Invalid timeframe '{timeframe}'. Try: {valid}")

def sanitize_filename(symbol, timeframe):
    base = symbol.replace("/", "-").replace(":", "-")
    return f"{base}_{timeframe}.csv"

def fetch_candles(exchange, symbol, timeframe, since=None, max_retries=5):
    """Fetch candles with retry logic."""
    for attempt in range(1, max_retries + 1):
        try:
            time.sleep(RATE_LIMIT_DELAY)
            candles = exchange.fetch_ohlcv(
                symbol,
                timeframe=timeframe,
                since=since,
                limit=BYBIT_MAX_CANDLES_PER_FETCH
            )
            return candles
        except Exception as e:
            print(f"Attempt {attempt}/{max_retries}: Error fetching candles: {e}")
            time.sleep(2 ** attempt)  # Exponential backoff
    raise ConnectionError(f"Failed to fetch candles after {max_retries} attempts.")

def main():
    print("\n[+] Init candle fetcher. Big up Bybit mandem!\n")

    args = parse_args()
    symbol = args.symbol
    timeframe = args.timeframe

    try:
        validate_timeframe(timeframe)
    except ValueError as e:
        print(f"🚨 Error: {e}")
        return

    exchange = ccxt.bybit({"enableRateLimit": True})
    filename = sanitize_filename(symbol, timeframe)

    # Determine starting point
    if os.path.exists(filename):
        df_old = pd.read_csv(filename)
        last_timestamp = int(df_old["time"].iloc[-1])
        print(f"CSV exists blud—appending fresh candles to '{filename}'...")
    else:
        df_old = pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])
        last_timestamp = None
        print(f"New CSV ting: '{filename}'. Fetching all candles from genesis...")

    all_candles = []
    previous_last = None
    while True:
        since = last_timestamp if last_timestamp else exchange.parse8601('2010-01-01T00:00:00Z')
        print(f"Fetching from timestamp: {datetime.utcfromtimestamp(since / 1000).strftime('%Y-%m-%d %H:%M:%S')} UTC...")

        candles = fetch_candles(exchange, symbol, timeframe, since)

        if not candles:
            print("No more candles left. Job's a good'un!")
            break

        # Remove unclosed candle
        current_time = exchange.milliseconds()
        if candles[-1][0] >= current_time:
            candles.pop()
            print("Yo, binned dat unclosed candle. Keepin' it clean.")

        # Exit if stuck in loop
        if previous_last and candles[-1][0] <= previous_last:
            print("Reached end of history. Big up!")
            break

        all_candles.extend(candles)
        previous_last = candles[-1][0]
        last_timestamp = candles[-1][0] + 1

        latest_time = datetime.utcfromtimestamp(candles[-1][0] / 1000).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"Added {len(candles)} candles (up to {latest_time}). Keepin' it lit...")

    # Merge old + new
    df_new = pd.DataFrame(all_candles, columns=["time", "open", "high", "low", "close", "volume"])
    df_final = pd.concat([df_old, df_new]) if not df_old.empty else df_new

    df_final = df_final.drop_duplicates("time").sort_values("time").reset_index(drop=True)

    df_final.to_csv(filename, index=False, float_format="%.8f")
    print(f"\n✅ Done fam! Saved {len(df_final)} candles to '{filename}'.")
    print(f"Total fetched: {len(all_candles)} new candles. Big ups!\n")

if __name__ == "__main__":
    main()