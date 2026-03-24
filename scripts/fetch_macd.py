#!/usr/bin/env python3
"""
Fetch MACD indicator data from Massive REST API.

Fetches daily and weekly MACD (12/26/9) for all active tickers and updates
the existing minervini_metrics rows with the latest values.

Must run AFTER minervini.py so that minervini_metrics rows already exist.

API endpoint: GET /v1/indicators/macd/{stockTicker}

Usage:
    python fetch_macd.py                # All tickers
    python fetch_macd.py --ticker AAPL  # Single ticker
"""

import requests
import psycopg2
from psycopg2 import pool
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import sys
import argparse

BATCH_SIZE = 10
BATCH_DELAY_MS = 100
LOOKBACK_DAYS = 90

API_BASE_URL = "https://api.massive.com/v1/indicators/macd"


def _load_api_key():
    try:
        key_file = Path.home() / ".massive-api/api_key.txt"
        if key_file.exists():
            return key_file.read_text().strip()
        else:
            print(f"Error: API key file not found at {key_file}")
            sys.exit(1)
    except Exception as e:
        print(f"Error reading API key file: {e}")
        sys.exit(1)


API_KEY = _load_api_key()


def get_db_connection():
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='stocks',
            user='stocks',
            password='stocks'
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error: Could not connect to PostgreSQL: {e}")
        sys.exit(1)


def get_db_connection_pool(min_conn=1, max_conn=BATCH_SIZE):
    try:
        return pool.ThreadedConnectionPool(
            min_conn,
            max_conn,
            host='localhost',
            port=5432,
            database='stocks',
            user='stocks',
            password='stocks'
        )
    except psycopg2.Error as e:
        print(f"Error: Could not create connection pool: {e}")
        sys.exit(1)


def get_all_tickers(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT symbol FROM ticker_details
        WHERE active = true
        AND market = 'stocks'
        AND locale = 'us'
        ORDER BY market_cap DESC NULLS LAST
    """)
    tickers = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tickers


def get_latest_metrics_date(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(date) FROM minervini_metrics")
    row = cursor.fetchone()
    cursor.close()
    return row[0] if row and row[0] else None


def fetch_macd(ticker, timespan, from_date):
    """
    Fetch latest MACD value for a ticker at a given timespan.

    Returns dict with 'value' and 'signal', or None on error.
    """
    url = f"{API_BASE_URL}/{ticker}"
    params = {
        'timespan': timespan,
        'timestamp.gte': from_date,
        'order': 'desc',
        'limit': 1,
        'adjusted': 'true',
        'series_type': 'close',
        'short_window': 12,
        'long_window': 26,
        'signal_window': 9,
        'apiKey': API_KEY
    }

    try:
        response = requests.get(url, params=params, timeout=30)

        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'OK':
                values = data.get('results', {}).get('values', [])
                if values:
                    latest = values[0]
                    return {
                        'value': latest.get('value'),
                        'signal': latest.get('signal'),
                    }
            return None
        elif response.status_code == 404:
            return None
        else:
            return None

    except requests.exceptions.Timeout:
        print(f"  Timeout: {ticker} ({timespan})")
        return None
    except Exception as e:
        print(f"  Error: {ticker} ({timespan}) - {str(e)[:50]}")
        return None


def process_ticker(ticker, from_date, metrics_date, db_pool):
    """Fetch daily + weekly MACD and update the minervini_metrics row."""
    conn = None
    try:
        daily = fetch_macd(ticker, 'day', from_date)
        weekly = fetch_macd(ticker, 'week', from_date)

        if daily is None and weekly is None:
            return {'ticker': ticker, 'success': False, 'reason': 'no_data'}

        conn = db_pool.getconn()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE minervini_metrics
            SET macd_daily_value = %s,
                macd_daily_signal = %s,
                macd_weekly_value = %s,
                macd_weekly_signal = %s
            WHERE symbol = %s AND date = %s
        """, (
            daily['value'] if daily else None,
            daily['signal'] if daily else None,
            weekly['value'] if weekly else None,
            weekly['signal'] if weekly else None,
            ticker,
            metrics_date
        ))
        conn.commit()
        cursor.close()

        return {'ticker': ticker, 'success': True}

    except Exception as e:
        if conn:
            conn.rollback()
        return {'ticker': ticker, 'success': False, 'reason': str(e)[:50]}
    finally:
        if conn:
            db_pool.putconn(conn)


def process_batch(tickers_batch, from_date, metrics_date, db_pool):
    stats = {'success': 0, 'failed': 0}

    with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
        futures = {
            executor.submit(process_ticker, ticker, from_date, metrics_date, db_pool): ticker
            for ticker in tickers_batch
        }

        for future in as_completed(futures):
            result = future.result()
            if result['success']:
                stats['success'] += 1
                print(f"  ✓ {result['ticker']}")
            else:
                stats['failed'] += 1

    return stats


def diagnose(ticker='AAPL'):
    """Run a full diagnostic on the MACD pipeline for a single ticker."""
    print("=" * 80)
    print(f"MACD DIAGNOSTIC for {ticker}")
    print("=" * 80)

    conn = get_db_connection()
    metrics_date = get_latest_metrics_date(conn)
    if not metrics_date:
        print("FAIL: No minervini_metrics rows found.")
        return
    print(f"\n1. Metrics date: {metrics_date}")

    from_date = (metrics_date - timedelta(days=LOOKBACK_DAYS)).strftime('%Y-%m-%d')
    print(f"   Lookback from: {from_date}")

    # Check if row exists
    cursor = conn.cursor()
    cursor.execute(
        "SELECT macd_daily_value, macd_daily_signal, macd_weekly_value, macd_weekly_signal "
        "FROM minervini_metrics WHERE symbol = %s AND date = %s",
        (ticker, metrics_date)
    )
    row = cursor.fetchone()
    print(f"\n2. Database row for {ticker} on {metrics_date}:")
    if row:
        print(f"   macd_daily_value  = {row[0]}")
        print(f"   macd_daily_signal = {row[1]}")
        print(f"   macd_weekly_value = {row[2]}")
        print(f"   macd_weekly_signal= {row[3]}")
        all_null = all(v is None for v in row)
        if all_null:
            print("   WARNING: All MACD columns are NULL")
    else:
        print(f"   WARNING: No minervini_metrics row found for {ticker} on {metrics_date}")

    # Count overall MACD population
    cursor.execute(
        "SELECT COUNT(*) FROM minervini_metrics WHERE date = %s AND macd_daily_value IS NOT NULL",
        (metrics_date,)
    )
    populated = cursor.fetchone()[0]
    cursor.execute(
        "SELECT COUNT(*) FROM minervini_metrics WHERE date = %s",
        (metrics_date,)
    )
    total = cursor.fetchone()[0]
    print(f"\n3. MACD population: {populated}/{total} rows have non-NULL macd_daily_value")

    # Count bullish
    cursor.execute("""
        SELECT COUNT(*) FROM minervini_metrics
        WHERE date = %s
          AND macd_daily_value > 0
          AND macd_daily_value > macd_daily_signal
          AND macd_weekly_value > 0
          AND macd_weekly_value > macd_weekly_signal
    """, (metrics_date,))
    bullish = cursor.fetchone()[0]
    print(f"   Bullish MACD (both timeframes): {bullish}")

    cursor.close()
    conn.close()

    # Test API
    print(f"\n4. Testing API for {ticker}...")
    for ts in ('day', 'week'):
        url = f"{API_BASE_URL}/{ticker}"
        params = {
            'timespan': ts,
            'timestamp.gte': from_date,
            'order': 'desc',
            'limit': 1,
            'adjusted': 'true',
            'series_type': 'close',
            'short_window': 12,
            'long_window': 26,
            'signal_window': 9,
            'apiKey': API_KEY
        }
        try:
            resp = requests.get(url, params=params, timeout=30)
            print(f"\n   [{ts}] HTTP {resp.status_code}")
            data = resp.json()
            print(f"   Status: {data.get('status')}")
            print(f"   Request ID: {data.get('request_id', 'N/A')}")
            values = data.get('results', {}).get('values', [])
            if values:
                latest = values[0]
                print(f"   Latest entry keys: {list(latest.keys())}")
                print(f"   value  = {latest.get('value')}")
                print(f"   signal = {latest.get('signal')}")
            else:
                print(f"   WARNING: No values returned")
                print(f"   Full results: {data.get('results', {})}")
        except Exception as e:
            print(f"   ERROR: {e}")

    print("\n" + "=" * 80)


def main():
    parser = argparse.ArgumentParser(description='Fetch MACD indicator data')
    parser.add_argument('--ticker', type=str, help='Fetch single ticker only')
    parser.add_argument('--diagnose', type=str, nargs='?', const='AAPL',
                        help='Run diagnostic for a ticker (default: AAPL)')
    args = parser.parse_args()

    if args.diagnose:
        diagnose(args.diagnose.upper())
        return

    print("=" * 80)
    print("MACD Indicator Fetcher")
    print("Using Massive REST API: /v1/indicators/macd")
    print("=" * 80)

    conn = get_db_connection()

    metrics_date = get_latest_metrics_date(conn)
    if not metrics_date:
        print("Error: No minervini_metrics rows found. Run minervini.py first.")
        sys.exit(1)
    print(f"\nMetrics date: {metrics_date}")

    from_date = (metrics_date - timedelta(days=LOOKBACK_DAYS)).strftime('%Y-%m-%d')
    print(f"MACD lookback from: {from_date}")

    if args.ticker:
        tickers = [args.ticker.upper()]
        print(f"Single ticker mode: {args.ticker.upper()}")
    else:
        tickers = get_all_tickers(conn)
        print(f"Found {len(tickers)} tickers")

    conn.close()

    if not tickers:
        print("Error: No tickers found")
        sys.exit(1)

    db_pool = get_db_connection_pool()

    print(f"\nFetching MACD data (daily + weekly)...")
    print("-" * 80)

    total_stats = {'success': 0, 'failed': 0}
    batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]

    start_time = time.time()

    try:
        for i, batch in enumerate(batches, 1):
            print(f"\nBatch {i}/{len(batches)} ({len(batch)} tickers):")

            batch_stats = process_batch(batch, from_date, metrics_date, db_pool)

            total_stats['success'] += batch_stats['success']
            total_stats['failed'] += batch_stats['failed']

            if i % 10 == 0:
                elapsed = time.time() - start_time
                rate = total_stats['success'] / elapsed if elapsed > 0 else 0
                eta = (len(tickers) - (i * BATCH_SIZE)) / rate / 60 if rate > 0 else 0
                print(f"\n  Progress: {i * BATCH_SIZE}/{len(tickers)} tickers | "
                      f"ETA: {eta:.1f} min")

            if i < len(batches):
                time.sleep(BATCH_DELAY_MS / 1000)

    except KeyboardInterrupt:
        print(f"\n\nInterrupted at batch {i}/{len(batches)}")
    finally:
        db_pool.closeall()

    elapsed = time.time() - start_time
    print("\n" + "=" * 80)
    print("Summary:")
    print(f"  Total tickers: {len(tickers)}")
    print(f"  Successful: {total_stats['success']}")
    print(f"  Failed: {total_stats['failed']}")
    print(f"  Elapsed time: {elapsed/60:.1f} minutes")
    print("=" * 80)


if __name__ == "__main__":
    main()
