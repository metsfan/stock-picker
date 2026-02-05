#!/usr/bin/env python3
"""
Fetch split-adjusted stock price data from Massive REST API.

This script replaces the S3 flat file approach to ensure all price data
is properly adjusted for stock splits. Uses the /v2/aggs/ticker endpoint
which returns split-adjusted data by default.

Features:
- Fetches 15 months (450 days) of daily OHLCV data
- Split-adjusted prices (critical for accurate MA calculations)
- 10 concurrent requests for speed
- Full refresh on each run (replaces existing data)

Usage:
    python fetch_stock_prices.py              # Fetch all tickers
    python fetch_stock_prices.py --ticker AAPL  # Fetch single ticker
    python fetch_stock_prices.py --days 500   # Custom lookback period
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

# Configuration
BATCH_SIZE = 10  # Concurrent requests
BATCH_DELAY_MS = 100  # Delay between batches
DEFAULT_LOOKBACK_DAYS = 450  # ~15 months of calendar days


def _load_api_key():
    """Load Massive API key from ~/.massive-api/api_key.txt"""
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
API_BASE_URL = "https://api.massive.com/v2/aggs/ticker"


def get_db_connection():
    """Get PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='stocks',
            user='postgres',
            password='adamesk'
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error: Could not connect to PostgreSQL: {e}")
        sys.exit(1)


def get_db_connection_pool(min_conn=1, max_conn=BATCH_SIZE):
    """Get PostgreSQL connection pool for concurrent operations."""
    try:
        return pool.ThreadedConnectionPool(
            min_conn,
            max_conn,
            host='localhost',
            port=5432,
            database='stocks',
            user='postgres',
            password='adamesk'
        )
    except psycopg2.Error as e:
        print(f"Error: Could not create connection pool: {e}")
        sys.exit(1)


def get_all_tickers(conn):
    """
    Get list of all tickers to fetch.
    
    Prioritizes ticker_details table (has market cap info for filtering),
    falls back to stock_prices if ticker_details is empty.
    """
    cursor = conn.cursor()
    
    # First try ticker_details (filtered for active US stocks)
    cursor.execute("""
        SELECT symbol FROM ticker_details 
        WHERE active = true 
        AND market = 'stocks'
        AND locale = 'us'
        ORDER BY market_cap DESC NULLS LAST
    """)
    
    tickers = [row[0] for row in cursor.fetchall()]
    
    if not tickers:
        # Fallback to stock_prices
        cursor.execute("""
            SELECT DISTINCT symbol FROM stock_prices ORDER BY symbol
        """)
        tickers = [row[0] for row in cursor.fetchall()]
    
    cursor.close()
    return tickers


def fetch_ticker_prices(ticker, from_date, to_date):
    """
    Fetch daily OHLCV data for a ticker from Massive API.
    
    Args:
        ticker: Stock symbol
        from_date: Start date (YYYY-MM-DD)
        to_date: End date (YYYY-MM-DD)
        
    Returns:
        tuple: (ticker, list of price records) or (ticker, None) on error
    """
    url = f"{API_BASE_URL}/{ticker}/range/1/day/{from_date}/{to_date}"
    params = {
        'adjusted': 'true',  # Split-adjusted prices (default, but explicit)
        'sort': 'asc',
        'limit': 50000,  # Max limit to get all data
        'apiKey': API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get('status') == 'OK':
                results = data.get('results', [])
                return (ticker, results)
            else:
                return (ticker, None)
        elif response.status_code == 404:
            # Ticker not found (delisted, etc.)
            return (ticker, [])
        else:
            return (ticker, None)
            
    except requests.exceptions.Timeout:
        print(f"  ✗ {ticker}: Request timeout")
        return (ticker, None)
    except Exception as e:
        print(f"  ✗ {ticker}: Error - {str(e)[:50]}")
        return (ticker, None)


def save_ticker_prices(conn, ticker, prices):
    """
    Save price data for a ticker to the database.
    
    Deletes existing data for this ticker first (full refresh).
    
    Args:
        conn: Database connection
        ticker: Stock symbol
        prices: List of price records from API
        
    Returns:
        int: Number of records saved
    """
    if not prices:
        return 0
    
    cursor = conn.cursor()
    
    try:
        # Delete existing data for this ticker
        cursor.execute("DELETE FROM stock_prices WHERE symbol = %s", (ticker,))
        
        # Prepare batch insert
        records = []
        for bar in prices:
            # Convert Unix milliseconds to date
            timestamp_ms = bar.get('t')
            if timestamp_ms:
                bar_date = datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d')
            else:
                continue
            
            records.append((
                ticker,
                bar_date,
                bar.get('o'),  # open
                bar.get('h'),  # high
                bar.get('l'),  # low
                bar.get('c'),  # close
                int(bar.get('v', 0))  # volume
            ))
        
        if records:
            cursor.executemany("""
                INSERT INTO stock_prices (symbol, date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, date) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
            """, records)
        
        conn.commit()
        cursor.close()
        return len(records)
        
    except Exception as e:
        print(f"  Error saving {ticker}: {e}")
        conn.rollback()
        cursor.close()
        return 0


def process_ticker(ticker, from_date, to_date, db_pool):
    """
    Process a single ticker: fetch and save price data.
    
    Args:
        ticker: Stock symbol
        from_date: Start date
        to_date: End date
        db_pool: Database connection pool
        
    Returns:
        dict: Result with ticker, success status, record count
    """
    conn = None
    try:
        # Fetch prices from API
        _, prices = fetch_ticker_prices(ticker, from_date, to_date)
        
        if prices is None:
            return {'ticker': ticker, 'success': False, 'records': 0, 'error': 'API error'}
        
        if not prices:
            return {'ticker': ticker, 'success': True, 'records': 0, 'error': 'No data'}
        
        # Save to database
        conn = db_pool.getconn()
        saved = save_ticker_prices(conn, ticker, prices)
        
        return {'ticker': ticker, 'success': True, 'records': saved}
        
    except Exception as e:
        return {'ticker': ticker, 'success': False, 'records': 0, 'error': str(e)[:50]}
    finally:
        if conn:
            db_pool.putconn(conn)


def process_batch(tickers_batch, from_date, to_date, db_pool):
    """
    Process a batch of tickers concurrently.
    
    Args:
        tickers_batch: List of ticker symbols
        from_date: Start date
        to_date: End date
        db_pool: Database connection pool
        
    Returns:
        dict: Batch statistics
    """
    stats = {'success': 0, 'failed': 0, 'records': 0}
    
    with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
        futures = {
            executor.submit(process_ticker, ticker, from_date, to_date, db_pool): ticker
            for ticker in tickers_batch
        }
        
        for future in as_completed(futures):
            result = future.result()
            
            if result['success'] and result['records'] > 0:
                stats['success'] += 1
                stats['records'] += result['records']
                print(f"  ✓ {result['ticker']}: {result['records']} bars")
            elif result['success'] and result['records'] == 0:
                stats['success'] += 1
                # Don't print for tickers with no data (reduces noise)
            else:
                stats['failed'] += 1
                if 'error' in result:
                    print(f"  ✗ {result['ticker']}: {result['error']}")
    
    return stats


def main():
    """Main function to fetch stock prices."""
    parser = argparse.ArgumentParser(description='Fetch split-adjusted stock prices')
    parser.add_argument('--ticker', type=str, help='Fetch single ticker only')
    parser.add_argument('--days', type=int, default=DEFAULT_LOOKBACK_DAYS,
                        help=f'Lookback days (default: {DEFAULT_LOOKBACK_DAYS})')
    args = parser.parse_args()
    
    print("=" * 80)
    print("Stock Price Fetcher (Split-Adjusted)")
    print("Using Massive REST API with adjusted=true")
    print("=" * 80)
    
    # Calculate date range
    to_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    from_date = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')
    
    print(f"\nDate range: {from_date} to {to_date} ({args.days} days)")
    print(f"Concurrent requests: {BATCH_SIZE}")
    
    # Connect to database
    print("\n1. Connecting to database...")
    conn = get_db_connection()
    print("   ✓ Database connected")
    
    # Get tickers
    print("\n2. Getting ticker list...")
    if args.ticker:
        tickers = [args.ticker.upper()]
        print(f"   → Single ticker mode: {args.ticker.upper()}")
    else:
        tickers = get_all_tickers(conn)
        print(f"   ✓ Found {len(tickers)} tickers")
    
    conn.close()
    
    if not tickers:
        print("   Error: No tickers found")
        sys.exit(1)
    
    # Create connection pool
    print("\n3. Creating connection pool...")
    db_pool = get_db_connection_pool()
    print(f"   ✓ Pool created (max {BATCH_SIZE} connections)")
    
    # Process tickers in batches
    print(f"\n4. Fetching price data...")
    print("-" * 80)
    
    total_stats = {'success': 0, 'failed': 0, 'records': 0}
    batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    
    start_time = time.time()
    
    try:
        for i, batch in enumerate(batches, 1):
            print(f"\nBatch {i}/{len(batches)} ({len(batch)} tickers):")
            
            batch_stats = process_batch(batch, from_date, to_date, db_pool)
            
            total_stats['success'] += batch_stats['success']
            total_stats['failed'] += batch_stats['failed']
            total_stats['records'] += batch_stats['records']
            
            # Progress update every 10 batches
            if i % 10 == 0:
                elapsed = time.time() - start_time
                rate = total_stats['success'] / elapsed if elapsed > 0 else 0
                eta = (len(tickers) - (i * BATCH_SIZE)) / rate / 60 if rate > 0 else 0
                print(f"\n  Progress: {i * BATCH_SIZE}/{len(tickers)} tickers | "
                      f"{total_stats['records']:,} records | "
                      f"ETA: {eta:.1f} min")
            
            # Delay between batches
            if i < len(batches):
                time.sleep(BATCH_DELAY_MS / 1000)
                
    except KeyboardInterrupt:
        print(f"\n\nInterrupted at batch {i}/{len(batches)}")
        print("Partial progress saved.")
    finally:
        db_pool.closeall()
    
    # Summary
    elapsed = time.time() - start_time
    print("\n" + "=" * 80)
    print("Summary:")
    print(f"  Total tickers: {len(tickers)}")
    print(f"  Successful: {total_stats['success']}")
    print(f"  Failed: {total_stats['failed']}")
    print(f"  Total records: {total_stats['records']:,}")
    print(f"  Elapsed time: {elapsed/60:.1f} minutes")
    if total_stats['success'] > 0:
        print(f"  Avg records/ticker: {total_stats['records']/total_stats['success']:.0f}")
    print("=" * 80)


if __name__ == "__main__":
    main()
