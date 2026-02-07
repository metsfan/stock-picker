#!/usr/bin/env python3
"""
Fetch earnings data from Benzinga API (via Massive/Polygon)

Retrieves historical and upcoming earnings announcements for all stocks
in the database, including EPS, revenue, analyst estimates, and surprises.

This data is critical for Minervini's fundamental analysis:
- EPS growth rates (QoQ and YoY)
- Earnings surprises (beat/miss patterns)
- Consistency of estimates beats
- Upcoming earnings dates (avoid buying before earnings)

API Documentation: https://massive.com/docs/rest/partners/benzinga/earnings
"""

import requests
import psycopg2
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import sys

# Concurrency configuration
BATCH_SIZE = 10  # Number of concurrent requests
BATCH_DELAY_MS = 100  # Delay between batches in milliseconds

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

# API Configuration
API_KEY = _load_api_key()
API_BASE_URL = "https://api.massive.com/benzinga/v1/earnings"

def get_db_connection():
    """Get PostgreSQL database connection."""
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


def get_all_tickers(conn):
    """Get all unique tickers from the database."""
    cursor = conn.cursor()
    
    # Get tickers from stock_prices (most comprehensive list)
    cursor.execute("""
        SELECT DISTINCT symbol 
        FROM stock_prices 
        ORDER BY symbol
    """)
    
    tickers = [row[0] for row in cursor.fetchall()]
    cursor.close()
    
    return tickers


def get_tickers_needing_update(conn, all_tickers):
    """
    Filter tickers to only those that need earnings updates.
    
    Returns tickers that either:
    - Have no earnings data at all, OR
    - Have data but the most recent earnings date is older than 90 days
    
    Args:
        conn: Database connection
        all_tickers: List of ticker symbols
        
    Returns:
        tuple: (tickers_to_fetch, tickers_with_data_count)
    """
    cursor = conn.cursor()
    
    # Get the most recent earnings date for each ticker that has data
    cursor.execute("""
        SELECT ticker, MAX(date) as latest_date
        FROM earnings
        GROUP BY ticker
    """)
    
    latest_dates = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.close()
    
    tickers_to_fetch = []
    tickers_with_data_count = 0
    current_date = datetime.now().date()
    
    for ticker in all_tickers:
        latest_date = latest_dates.get(ticker)
        
        if latest_date is None:
            # No data at all - definitely fetch
            tickers_to_fetch.append((ticker, None))
        else:
            # Calculate how old the data is
            days_old = (current_date - latest_date).days
            
            # Fetch if data is older than 90 days (roughly 1 quarter)
            if days_old > 90:
                tickers_to_fetch.append((ticker, latest_date))
            else:
                tickers_with_data_count += 1
    
    return tickers_to_fetch, tickers_with_data_count


def fetch_earnings_for_ticker(ticker, start_date=None, end_date=None):
    """
    Fetch earnings data for a specific ticker.
    
    Args:
        ticker: Stock symbol
        start_date: Start date (YYYY-MM-DD) for historical data
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        list: Earnings records
    """
    params = {
        'ticker': ticker,
        'sort': 'date.desc',
        'limit': 100,  # Get up to 100 earnings (many years of history)
        'apiKey': API_KEY
    }
    
    if start_date:
        params['date.gte'] = start_date
    if end_date:
        params['date.lte'] = end_date
    
    try:
        response = requests.get(API_BASE_URL, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get('status') == 'OK':
                return data.get('results', [])
            else:
                print(f"  ⚠ {ticker}: API returned status {data.get('status', 'UNKNOWN')}")
                return []
        elif response.status_code == 404:
            # No earnings data for this ticker (common for new listings)
            return []
        else:
            print(f"  ✗ {ticker}: HTTP {response.status_code}")
            return []
            
    except requests.exceptions.Timeout:
        print(f"  ✗ {ticker}: Request timeout")
        return []
    except Exception as e:
        print(f"  ✗ {ticker}: Error - {str(e)[:100]}")
        return []


def upsert_earnings_record(conn, record):
    """
    Insert or update an earnings record in the database.
    
    Args:
        conn: Database connection
        record: Earnings record from API
        
    Returns:
        bool: True if successful, False otherwise
    """
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO earnings (
                ticker, date, time, fiscal_year, fiscal_period, currency,
                actual_eps, estimated_eps, eps_surprise, eps_surprise_percent, eps_method,
                actual_revenue, estimated_revenue, revenue_surprise, 
                revenue_surprise_percent, revenue_method,
                previous_eps, previous_revenue, company_name, importance,
                date_status, notes, benzinga_id, last_updated, fetched_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (benzinga_id) WHERE benzinga_id IS NOT NULL
            DO UPDATE SET
                ticker = EXCLUDED.ticker,
                date = EXCLUDED.date,
                time = EXCLUDED.time,
                fiscal_year = EXCLUDED.fiscal_year,
                fiscal_period = EXCLUDED.fiscal_period,
                currency = EXCLUDED.currency,
                actual_eps = EXCLUDED.actual_eps,
                estimated_eps = EXCLUDED.estimated_eps,
                eps_surprise = EXCLUDED.eps_surprise,
                eps_surprise_percent = EXCLUDED.eps_surprise_percent,
                eps_method = EXCLUDED.eps_method,
                actual_revenue = EXCLUDED.actual_revenue,
                estimated_revenue = EXCLUDED.estimated_revenue,
                revenue_surprise = EXCLUDED.revenue_surprise,
                revenue_surprise_percent = EXCLUDED.revenue_surprise_percent,
                revenue_method = EXCLUDED.revenue_method,
                previous_eps = EXCLUDED.previous_eps,
                previous_revenue = EXCLUDED.previous_revenue,
                company_name = EXCLUDED.company_name,
                importance = EXCLUDED.importance,
                date_status = EXCLUDED.date_status,
                notes = EXCLUDED.notes,
                last_updated = EXCLUDED.last_updated,
                fetched_at = EXCLUDED.fetched_at
        """, (
            record.get('ticker'),
            record.get('date'),
            record.get('time'),
            record.get('fiscal_year'),
            record.get('fiscal_period'),
            record.get('currency'),
            record.get('actual_eps'),
            record.get('estimated_eps'),
            record.get('eps_surprise'),
            record.get('eps_surprise_percent'),
            record.get('eps_method'),
            record.get('actual_revenue'),
            record.get('estimated_revenue'),
            record.get('revenue_surprise'),
            record.get('revenue_surprise_percent'),
            record.get('revenue_method'),
            record.get('previous_eps'),
            record.get('previous_revenue'),
            record.get('company_name'),
            record.get('importance'),
            record.get('date_status'),
            record.get('notes'),
            record.get('benzinga_id'),
            record.get('last_updated'),
            datetime.now()
        ))
        
        conn.commit()
        cursor.close()
        return True
        
    except Exception as e:
        print(f"  Error saving earnings record: {e}")
        conn.rollback()
        cursor.close()
        return False


def process_ticker_earnings(ticker_data, conn, default_start_date, end_date):
    """
    Process earnings for a single ticker (for concurrent execution).
    
    Args:
        ticker_data: Tuple of (ticker, latest_date)
        conn: Database connection
        default_start_date: Default start date for fetching
        end_date: End date for fetching
        
    Returns:
        dict: Results with ticker, success, saved_count
    """
    ticker, latest_date = ticker_data
    
    try:
        # Determine start date for this ticker
        if latest_date:
            # Add 1 day to avoid re-fetching the same date
            start_date = (latest_date + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            # No existing data, use default start date
            start_date = default_start_date
        
        # Fetch earnings for this ticker
        earnings = fetch_earnings_for_ticker(ticker, start_date, end_date)
        
        if earnings:
            # Save each earnings record
            saved_count = 0
            for record in earnings:
                if upsert_earnings_record(conn, record):
                    saved_count += 1
            
            return {
                'ticker': ticker,
                'success': True,
                'saved_count': saved_count
            }
        else:
            return {
                'ticker': ticker,
                'success': False,
                'saved_count': 0
            }
            
    except Exception as e:
        return {
            'ticker': ticker,
            'success': False,
            'error': str(e)[:100],
            'saved_count': 0
        }


def process_batch(conn, tickers_batch, default_start_date, end_date):
    """
    Process a batch of tickers concurrently.
    
    Args:
        conn: Database connection
        tickers_batch: List of (ticker, latest_date) tuples
        default_start_date: Default start date for fetching
        end_date: End date for fetching
        
    Returns:
        dict: Statistics of the batch processing
    """
    stats = {'with_data': 0, 'without_data': 0, 'total_records': 0, 'errors': 0}
    
    with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
        # Submit all tasks
        futures = {
            executor.submit(
                process_ticker_earnings, 
                ticker_data, 
                conn, 
                default_start_date, 
                end_date
            ): ticker_data[0]
            for ticker_data in tickers_batch
        }
        
        # Process results as they complete
        for future in as_completed(futures):
            result = future.result()
            
            if result['success']:
                stats['with_data'] += 1
                stats['total_records'] += result['saved_count']
                print(f"  ✓ {result['ticker']}: {result['saved_count']} earnings records")
            else:
                if 'error' in result:
                    stats['errors'] += 1
                    print(f"  ✗ {result['ticker']}: {result['error']}")
                else:
                    stats['without_data'] += 1
    
    return stats


def main():
    """Main function to fetch and store earnings data."""
    print("=" * 80)
    print("Benzinga Earnings Data Fetcher")
    print("=" * 80)
    
    # Connect to database
    print("\n1. Connecting to database...")
    conn = get_db_connection()
    print("   ✓ Database connected")
    
    # Get all tickers
    print("\n2. Fetching ticker list...")
    all_tickers = get_all_tickers(conn)
    print(f"   ✓ Found {len(all_tickers)} tickers")
    
    # Filter to only tickers needing updates
    print("\n3. Checking for existing earnings data...")
    tickers_to_fetch, tickers_up_to_date = get_tickers_needing_update(conn, all_tickers)
    
    print(f"   ✓ {len(tickers_to_fetch)} tickers need updates")
    print(f"   ✓ {tickers_up_to_date} tickers already have recent data (< 90 days old)")
    
    if tickers_to_fetch and len(tickers_to_fetch) <= 10:
        # Show which tickers are being fetched if there are only a few
        ticker_names = [t[0] for t in tickers_to_fetch[:10]]
        print(f"   → Fetching: {', '.join(ticker_names)}")
    
    if not tickers_to_fetch:
        print("\n   All tickers have recent earnings data!")
        print("   Nothing to fetch.")
        conn.close()
        return
    
    # Date range for historical data
    # Fetch last 3 years of earnings (typically 12 quarters)
    end_date = datetime.now().strftime("%Y-%m-%d")
    default_start_date = (datetime.now() - timedelta(days=3*365)).strftime("%Y-%m-%d")
    
    print(f"\n4. Fetching earnings data (default since {default_start_date})...")
    print(f"   For tickers with existing data, only fetching new records")
    print(f"   Fetching {len(tickers_to_fetch)} tickers with {BATCH_SIZE} concurrent requests")
    print("-" * 80)
    
    total_earnings = 0
    tickers_with_data = 0
    tickers_without_data = 0
    errors = 0
    
    # Process tickers in batches of BATCH_SIZE
    num_batches = (len(tickers_to_fetch) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for batch_num in range(num_batches):
        start_idx = batch_num * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, len(tickers_to_fetch))
        batch = tickers_to_fetch[start_idx:end_idx]
        
        try:
            print(f"\n  Batch {batch_num + 1}/{num_batches} ({len(batch)} tickers)...")
            
            # Process batch concurrently
            batch_stats = process_batch(conn, batch, default_start_date, end_date)
            
            # Update totals
            tickers_with_data += batch_stats['with_data']
            tickers_without_data += batch_stats['without_data']
            total_earnings += batch_stats['total_records']
            errors += batch_stats['errors']
            
            # Progress checkpoint every 10 batches
            if (batch_num + 1) % 10 == 0:
                processed = end_idx
                print(f"\n  Progress checkpoint: {processed}/{len(tickers_to_fetch)} tickers processed")
                print(f"  Total earnings: {total_earnings} | With data: {tickers_with_data} | Without: {tickers_without_data}")
            
            # Small delay between batches to avoid overwhelming the API
            if batch_num < num_batches - 1:
                time.sleep(BATCH_DELAY_MS / 1000)
            
        except KeyboardInterrupt:
            print(f"\n\nInterrupted by user at batch {batch_num + 1}/{num_batches}")
            print(f"Progress saved up to batch {batch_num}")
            break
        except Exception as e:
            print(f"  Batch {batch_num + 1} error: {str(e)[:100]}")
            errors += len(batch)
            continue
    
    # Close database connection
    conn.close()
    
    # Print summary
    print("\n" + "=" * 80)
    print("Summary:")
    print(f"  Total tickers in database: {len(all_tickers)}")
    print(f"  Tickers already up-to-date: {tickers_up_to_date}")
    print(f"  Tickers checked for updates: {tickers_with_data + tickers_without_data}")
    print(f"  Tickers with new data fetched: {tickers_with_data}")
    print(f"  Tickers without earnings data: {tickers_without_data}")
    print(f"  Total earnings records saved: {total_earnings}")
    print(f"  Errors: {errors}")
    if tickers_with_data + tickers_without_data > 0:
        print(f"  Fetch success rate: {(tickers_with_data/(tickers_with_data + tickers_without_data)*100):.1f}%")
    print("=" * 80)
    
    # Show some statistics
    cursor = conn.cursor()
    
    # Reconnect for stats
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            COUNT(DISTINCT ticker) as unique_tickers,
            COUNT(*) as total_records,
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            AVG(eps_surprise_percent) as avg_eps_surprise
        FROM earnings
        WHERE actual_eps IS NOT NULL
    """)
    
    stats = cursor.fetchone()
    if stats:
        print(f"\nDatabase Statistics:")
        print(f"  Unique tickers with earnings: {stats[0]}")
        print(f"  Total earnings records: {stats[1]}")
        print(f"  Date range: {stats[2]} to {stats[3]}")
        print(f"  Average EPS surprise: {stats[4]:.2f}%" if stats[4] else "  Average EPS surprise: N/A")
    
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
