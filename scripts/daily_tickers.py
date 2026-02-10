#!/usr/bin/env python3
"""
Fetch all ticker information from Massive API using the tickers endpoint with pagination.
Iteratively fetches all active stock tickers and stores them in the database.
"""

import requests
import psycopg2
from pathlib import Path
from datetime import datetime
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
API_BASE_URL = "https://api.massive.com/v3/reference/tickers"
REQUEST_DELAY_MS = 200  # milliseconds between requests


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


def fetch_all_tickers():
    """
    Fetch all active stock tickers from Massive API using pagination.
    
    Returns:
        list: List of ticker dictionaries containing ticker information
    """
    all_tickers = []
    next_url = f"{API_BASE_URL}?market=stocks&active=true&limit=1000&apiKey={API_KEY}"
    page_count = 0
    
    print("   Fetching tickers from API...")
    
    while next_url:
        page_count += 1
        
        try:
            response = requests.get(next_url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 'OK' and 'results' in data:
                    results = data['results']
                    all_tickers.extend(results)
                    print(f"   Page {page_count}: Fetched {len(results)} tickers (total: {len(all_tickers)})")
                    
                    # Check for next page
                    next_url = data.get('next_url')
                    if next_url:
                        # Add API key to next_url if not present
                        if 'apiKey=' not in next_url:
                            next_url += f"&apiKey={API_KEY}"
                        # Add delay between requests
                        time.sleep(REQUEST_DELAY_MS / 1000.0)
                else:
                    print(f"   ⚠ API returned status {data.get('status', 'UNKNOWN')}")
                    break
            else:
                print(f"   ✗ HTTP {response.status_code}")
                break
                
        except requests.exceptions.Timeout:
            print(f"   ✗ Request timeout on page {page_count}")
            break
        except Exception as e:
            print(f"   ✗ Error fetching page {page_count}: {str(e)[:100]}")
            break
    
    return all_tickers


def upsert_ticker(conn, ticker_data):
    """
    Insert or update ticker information in the database.
    
    Args:
        conn: Database connection
        ticker_data: Dictionary of ticker data from API
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not ticker_data:
        return False
    
    try:
        cursor = conn.cursor()
        
        symbol = ticker_data.get('ticker')
        
        # Prepare the upsert query with only fields available from the list endpoint
        query = """
            INSERT INTO ticker_details (
                symbol, name, primary_exchange, locale, market,
                currency_name, active, cik, composite_figi, share_class_figi,
                ticker_type, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (symbol) DO UPDATE SET
                name = EXCLUDED.name,
                primary_exchange = EXCLUDED.primary_exchange,
                locale = EXCLUDED.locale,
                market = EXCLUDED.market,
                currency_name = EXCLUDED.currency_name,
                active = EXCLUDED.active,
                cik = EXCLUDED.cik,
                composite_figi = EXCLUDED.composite_figi,
                share_class_figi = EXCLUDED.share_class_figi,
                ticker_type = EXCLUDED.ticker_type,
                updated_at = EXCLUDED.updated_at
        """
        
        cursor.execute(query, (
            symbol,
            ticker_data.get('name'),
            ticker_data.get('primary_exchange'),
            ticker_data.get('locale'),
            ticker_data.get('market'),
            ticker_data.get('currency_name'),
            ticker_data.get('active'),
            ticker_data.get('cik'),
            ticker_data.get('composite_figi'),
            ticker_data.get('share_class_figi'),
            ticker_data.get('type'),
            datetime.now()
        ))
        
        conn.commit()
        cursor.close()
        return True
        
    except Exception as e:
        symbol = ticker_data.get('ticker', 'UNKNOWN')
        print(f"  Error saving {symbol} to database: {e}")
        conn.rollback()
        return False


def main():
    """Main function to orchestrate the ticker fetch and database update."""
    print("=" * 80)
    print("Fetching All Tickers from Massive API")
    print("=" * 80)
    
    # Fetch all tickers from API
    print("\n1. Fetching all active stock tickers from API...")
    tickers = fetch_all_tickers()
    
    if not tickers:
        print("   Error: No tickers fetched from API")
        sys.exit(1)
    
    print(f"   ✓ Fetched {len(tickers)} total tickers")
    
    # Connect to database
    print("\n2. Connecting to database...")
    conn = get_db_connection()
    
    if not conn:
        sys.exit(1)
    
    print("   ✓ Database connected")
    
    # Process tickers and save to database
    print("\n3. Saving tickers to database...")
    print("-" * 80)
    
    stats = {'success': 0, 'failed': 0}
    
    for i, ticker_data in enumerate(tickers, 1):
        symbol = ticker_data.get('ticker', 'UNKNOWN')
        
        if upsert_ticker(conn, ticker_data):
            stats['success'] += 1
            if i % 100 == 0:  # Print progress every 100 tickers
                print(f"   Progress: {i}/{len(tickers)} processed ({stats['success']} successful)")
        else:
            stats['failed'] += 1
            print(f"   ✗ Failed to save: {symbol}")
    
    # Close database connection
    conn.close()
    
    # Print summary
    print("\n" + "=" * 80)
    print("Summary:")
    print(f"  Total tickers fetched: {len(tickers)}")
    print(f"  Successfully saved: {stats['success']}")
    print(f"  Failed: {stats['failed']}")
    print(f"  Success rate: {(stats['success']/len(tickers)*100):.1f}%")
    print("=" * 80)


if __name__ == "__main__":
    main()
