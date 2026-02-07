#!/usr/bin/env python3
"""
Fetch detailed ticker information from Massive API for all stocks in the most recent data file.
Makes 10 concurrent requests with 100ms delay between batches.
"""

import requests
import psycopg2
import csv
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import sys

def _load_api_key():
    """Load Massive API key from ~/.massive-api/api_key.txt"""
    try:
        key_file = Path.home() / ".massive-api/api_key.txt"
        if key_file.exists():
            return key_file.read_text().strip()
        else:
            print(f"Warning: API key file not found at {key_file}")
            return "your-api-key-here"
    except Exception as e:
        print(f"Error reading API key file: {e}")
        return "your-api-key-here"

# API Configuration
API_KEY = _load_api_key()
API_BASE_URL = f"https://api.massive.com/v3/reference/tickers"
BATCH_SIZE = 10
BATCH_DELAY_MS = 100  # milliseconds between batches


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
        return None


def get_latest_data_file():
    """Find the most recent CSV file in the day_aggs directory."""
    day_aggs_dir = Path("../day_aggs")
    
    if not day_aggs_dir.exists():
        print(f"Error: Directory {day_aggs_dir} does not exist")
        return None
    
    # Get all CSV files and sort by date in filename (YYYY-MM-DD.csv)
    csv_files = sorted(day_aggs_dir.glob("*.csv"), reverse=True)
    
    # Filter out empty files (non-trading days)
    for csv_file in csv_files:
        if csv_file.stat().st_size > 0:
            return csv_file
    
    print("Error: No valid data files found")
    return None


def get_unique_symbols(csv_file):
    """Extract unique ticker symbols from the CSV file."""
    symbols = set()
    
    try:
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ticker = row.get('ticker', '').strip().upper()
                if ticker:
                    symbols.add(ticker)
        
        return sorted(list(symbols))
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return []


def fetch_ticker_details(ticker):
    """
    Fetch ticker details from Massive API.
    
    Args:
        ticker: Stock ticker symbol
        
    Returns:
        tuple: (ticker, data_dict) or (ticker, None) on error
    """
    url = f"{API_BASE_URL}/{ticker}?apiKey={API_KEY}"
    
    try:
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get('status') == 'OK' and 'results' in data:
                return (ticker, data['results'])
            else:
                print(f"  ⚠ {ticker}: API returned status {data.get('status', 'UNKNOWN')}")
                return (ticker, None)
        else:
            print(f"  ✗ {ticker}: HTTP {response.status_code}")
            return (ticker, None)
            
    except requests.exceptions.Timeout:
        print(f"  ✗ {ticker}: Request timeout")
        return (ticker, None)
    except Exception as e:
        print(f"  ✗ {ticker}: Error - {str(e)[:50]}")
        return (ticker, None)


def upsert_ticker_details(conn, symbol, details):
    """
    Insert or update ticker details in the database.
    
    Args:
        conn: Database connection
        symbol: Stock ticker symbol
        details: Dictionary of ticker details from API
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not details:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Extract address fields
        address = details.get('address', {})
        
        # Prepare the upsert query
        query = """
            INSERT INTO ticker_details (
                symbol, name, description, market_cap, homepage_url, 
                logo_url, icon_url, primary_exchange, locale, market,
                currency_name, active, list_date, sic_code, sic_description,
                total_employees, share_class_shares_outstanding, weighted_shares_outstanding,
                cik, composite_figi, share_class_figi, phone_number, ticker_type,
                round_lot, address_line1, address_city, address_state, 
                address_postal_code, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (symbol) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                market_cap = EXCLUDED.market_cap,
                homepage_url = EXCLUDED.homepage_url,
                logo_url = EXCLUDED.logo_url,
                icon_url = EXCLUDED.icon_url,
                primary_exchange = EXCLUDED.primary_exchange,
                locale = EXCLUDED.locale,
                market = EXCLUDED.market,
                currency_name = EXCLUDED.currency_name,
                active = EXCLUDED.active,
                list_date = EXCLUDED.list_date,
                sic_code = EXCLUDED.sic_code,
                sic_description = EXCLUDED.sic_description,
                total_employees = EXCLUDED.total_employees,
                share_class_shares_outstanding = EXCLUDED.share_class_shares_outstanding,
                weighted_shares_outstanding = EXCLUDED.weighted_shares_outstanding,
                cik = EXCLUDED.cik,
                composite_figi = EXCLUDED.composite_figi,
                share_class_figi = EXCLUDED.share_class_figi,
                phone_number = EXCLUDED.phone_number,
                ticker_type = EXCLUDED.ticker_type,
                round_lot = EXCLUDED.round_lot,
                address_line1 = EXCLUDED.address_line1,
                address_city = EXCLUDED.address_city,
                address_state = EXCLUDED.address_state,
                address_postal_code = EXCLUDED.address_postal_code,
                updated_at = EXCLUDED.updated_at
        """
        
        # Extract branding URLs
        branding = details.get('branding', {})
        
        cursor.execute(query, (
            symbol,
            details.get('name'),
            details.get('description'),
            details.get('market_cap'),
            details.get('homepage_url'),
            branding.get('logo_url'),
            branding.get('icon_url'),
            details.get('primary_exchange'),
            details.get('locale'),
            details.get('market'),
            details.get('currency_name'),
            details.get('active'),
            details.get('list_date'),
            details.get('sic_code'),
            details.get('sic_description'),
            details.get('total_employees'),
            details.get('share_class_shares_outstanding'),
            details.get('weighted_shares_outstanding'),
            details.get('cik'),
            details.get('composite_figi'),
            details.get('share_class_figi'),
            details.get('phone_number'),
            details.get('type'),
            details.get('round_lot'),
            address.get('address1'),
            address.get('city'),
            address.get('state'),
            address.get('postal_code'),
            datetime.now()
        ))
        
        conn.commit()
        cursor.close()
        return True
        
    except Exception as e:
        print(f"  Error saving {symbol} to database: {e}")
        conn.rollback()
        return False


def process_batch(conn, symbols_batch):
    """
    Process a batch of symbols concurrently.
    
    Args:
        conn: Database connection
        symbols_batch: List of symbols to process
        
    Returns:
        dict: Statistics of the batch processing
    """
    stats = {'success': 0, 'failed': 0}
    
    with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
        # Submit all tasks
        futures = {executor.submit(fetch_ticker_details, symbol): symbol 
                   for symbol in symbols_batch}
        
        # Process results as they complete
        for future in as_completed(futures):
            symbol, details = future.result()
            
            if details:
                if upsert_ticker_details(conn, symbol, details):
                    print(f"  ✓ {symbol}")
                    stats['success'] += 1
                else:
                    stats['failed'] += 1
            else:
                stats['failed'] += 1
    
    return stats


def main():
    """Main function to orchestrate the ticker details fetch."""
    print("=" * 80)
    print("Fetching Ticker Details from Massive API")
    print("=" * 80)
    
    # Find the latest data file
    print("\n1. Finding latest data file...")
    latest_file = get_latest_data_file()
    
    if not latest_file:
        sys.exit(1)
    
    print(f"   ✓ Using: {latest_file.name}")
    
    # Get unique symbols
    print("\n2. Extracting unique symbols...")
    symbols = get_unique_symbols(latest_file)
    
    if not symbols:
        print("   Error: No symbols found")
        sys.exit(1)
    
    print(f"   ✓ Found {len(symbols)} unique symbols")
    
    # Connect to database
    print("\n3. Connecting to database...")
    conn = get_db_connection()
    
    if not conn:
        sys.exit(1)
    
    print("   ✓ Database connected")
    
    # Process symbols in batches
    print(f"\n4. Fetching ticker details ({BATCH_SIZE} concurrent requests)...")
    print(f"   Batch delay: {BATCH_DELAY_MS}ms")
    print("-" * 80)
    
    total_stats = {'success': 0, 'failed': 0}
    batches = [symbols[i:i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    
    for i, batch in enumerate(batches, 1):
        print(f"\nBatch {i}/{len(batches)} ({len(batch)} symbols):")
        
        batch_stats = process_batch(conn, batch)
        total_stats['success'] += batch_stats['success']
        total_stats['failed'] += batch_stats['failed']
        
        # Delay between batches (except for the last one)
        if i < len(batches):
            time.sleep(BATCH_DELAY_MS / 1000.0)
    
    # Close database connection
    conn.close()
    
    # Print summary
    print("\n" + "=" * 80)
    print("Summary:")
    print(f"  Total symbols processed: {len(symbols)}")
    print(f"  Successful: {total_stats['success']}")
    print(f"  Failed: {total_stats['failed']}")
    print(f"  Success rate: {(total_stats['success']/len(symbols)*100):.1f}%")
    print("=" * 80)


if __name__ == "__main__":
    main()
