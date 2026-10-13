"""
Tiger Cloud Financial Analytics & Candlestick OHLCV Inspector
=============================================================
Challenge: Ingest real-time financial data into Tiger Cloud
MLH Global Hack Week: Hacktoberfest

Features:
- Queries TimescaleDB Hypertable `crypto_ticks`
- Evaluates Continuous Aggregates (`one_minute_candle` and `one_day_candle`)
- Computes real-time OHLCV metrics, volatility, volume, and latency
- Inspects hypertable chunk distribution and compression stats
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def main():
    service_url = os.getenv("TIMESCALE_SERVICE_URL")
    if not service_url:
        print("[!] ERROR: TIMESCALE_SERVICE_URL not found in environment.", file=sys.stderr)
        sys.exit(1)

    print("=" * 80)
    print("  TIGER CLOUD FINANCIAL DATA PIPELINE — REAL-TIME ANALYTICS INSPECTOR")
    print("=" * 80)

    conn = psycopg2.connect(service_url)
    with conn.cursor() as cur:
        # 1. Hypertable row count & summary
        cur.execute("""
            SELECT 
                COUNT(*) as total_ticks,
                COUNT(DISTINCT symbol) as unique_symbols,
                MIN("time") as earliest_tick,
                MAX("time") as latest_tick
            FROM crypto_ticks;
        """)
        total_ticks, unique_symbols, earliest_tick, latest_tick = cur.fetchone()

        print(f"\n[+] CRYPTO_TICKS HYPERTABLE SUMMARY:")
        print(f"    - Total Real-Time Ticks Ingested: {total_ticks:,}")
        print(f"    - Tracked Assets / Symbols:      {unique_symbols}")
        print(f"    - Earliest Tick Timestamp:       {earliest_tick}")
        print(f"    - Latest Tick Timestamp:         {latest_tick}")

        # 2. Per-symbol real-time price & volume summary
        cur.execute("""
            SELECT 
                t.symbol,
                COALESCE(a.name, 'Unknown') as asset_name,
                COUNT(t.*) as ticks,
                ROUND(MIN(t.price)::numeric, 2) as min_price,
                ROUND(MAX(t.price)::numeric, 2) as max_price,
                ROUND(AVG(t.price)::numeric, 2) as avg_price,
                ROUND((MAX(t.price) - MIN(t.price))::numeric, 2) as spread,
                ROUND(MAX(t.day_volume)::numeric, 0) as latest_vol
            FROM crypto_ticks t
            LEFT JOIN crypto_assets a ON t.symbol = a.symbol
            GROUP BY t.symbol, a.name
            ORDER BY ticks DESC;
        """)
        rows = cur.fetchall()

        print("\n[+] REAL-TIME MARKET ASSET SNAPSHOT:")
        print("-" * 80)
        print(f"{'Symbol':<10} | {'Name':<15} | {'Ticks':<8} | {'Min Price':<11} | {'Max Price':<11} | {'Avg Price':<11} | {'Spread':<8}")
        print("-" * 80)
        for r in rows:
            print(f"{r[0]:<10} | {r[1]:<15} | {r[2]:<8} | ${r[3]:<10} | ${r[4]:<10} | ${r[5]:<10} | ${r[6]:<8}")
        print("-" * 80)

        # 3. Continuous Aggregate 1-minute OHLCV candles
        print("\n[+] CONTINUOUS AGGREGATE — 1-MINUTE OHLCV CANDLES (`one_minute_candle`):")
        cur.execute("""
            SELECT 
                bucket,
                symbol,
                ROUND(open::numeric, 2) as open,
                ROUND(high::numeric, 2) as high,
                ROUND(low::numeric, 2) as low,
                ROUND(close::numeric, 2) as close,
                ROUND(day_volume::numeric, 0) as day_volume
            FROM one_minute_candle
            ORDER BY bucket DESC, symbol ASC
            LIMIT 10;
        """)
        cagg_rows = cur.fetchall()

        if cagg_rows:
            print("-" * 80)
            print(f"{'Time Bucket':<24} | {'Symbol':<10} | {'Open':<10} | {'High':<10} | {'Low':<10} | {'Close':<10} | {'Volume':<10}")
            print("-" * 80)
            for c in cagg_rows:
                print(f"{str(c[0]):<24} | {c[1]:<10} | ${c[2]:<9} | ${c[3]:<9} | ${c[4]:<9} | ${c[5]:<9} | {c[6]:<10}")
            print("-" * 80)
        else:
            print("    [!] No materialized CAGG rows yet (refresh policy pending or fresh ticks).")

        # 4. TimescaleDB Chunks and Hypertable metadata
        print("\n[+] TIMESCALEDB CHUNKS & COMPRESSION METADATA:")
        cur.execute("""
            SELECT 
                hypertable_name,
                num_chunks,
                compression_enabled
            FROM timescaledb_information.hypertables
            WHERE hypertable_name = 'crypto_ticks';
        """)
        ht_info = cur.fetchone()
        if ht_info:
            print(f"    - Hypertable:          {ht_info[0]}")
            print(f"    - Active Chunks:       {ht_info[1]}")
            print(f"    - Compression Enabled: {ht_info[2]}")

    conn.close()
    print("\n[✓] Analytics inspection completed successfully.")


if __name__ == "__main__":
    main()
