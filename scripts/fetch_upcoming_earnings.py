#!/usr/bin/env python3
"""
Fetch upcoming earnings announcements from Benzinga API

This script focuses on upcoming earnings (next 60 days) and should be run
daily or weekly to keep earnings calendars current. Helps avoid buying
stocks right before earnings announcements.

Run this frequently (daily) to stay updated on:
- Upcoming earnings dates
- Analyst estimates for next quarters
- Earnings calendar for risk management
"""

import requests
import psycopg2
from datetime import datetime, timedelta
from pathlib import Path
import sys

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
            user='postgres',
            password='adamesk'
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error: Could not connect to PostgreSQL: {e}")
        sys.exit(1)


def fetch_upcoming_earnings(days_ahead=60):
    """
    Fetch upcoming earnings for all stocks.
    
    Args:
        days_ahead: Number of days to look ahead (default 60)
        
    Returns:
        list: Upcoming earnings records
    """
    today = datetime.now().strftime("%Y-%m-%d")
    end_date = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
    
    params = {
        'date.gte': today,
        'date.lte': end_date,
        'sort': 'date.asc',
        'limit': 1000,  # Get many results
        'apiKey': API_KEY
    }
    
    all_results = []
    url = API_BASE_URL
    
    try:
        while url:
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 'OK':
                    results = data.get('results', [])
                    all_results.extend(results)
                    
                    # Check for pagination
                    next_url = data.get('next_url')
                    if next_url:
                        url = next_url
                        params = {'apiKey': API_KEY}  # Reset params for next page
                    else:
                        break
                else:
                    print(f"API returned status: {data.get('status', 'UNKNOWN')}")
                    break
            else:
                print(f"HTTP {response.status_code}: {response.text[:200]}")
                break
                
    except Exception as e:
        print(f"Error fetching upcoming earnings: {e}")
        return []
    
    return all_results


def upsert_earnings_record(conn, record):
    """Insert or update an earnings record."""
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
            ON CONFLICT (ticker, date, fiscal_period, fiscal_year)
            DO UPDATE SET
                time = EXCLUDED.time,
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
                benzinga_id = EXCLUDED.benzinga_id,
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
        print(f"Error saving earnings: {e}")
        conn.rollback()
        cursor.close()
        return False


def main():
    """Main function."""
    print("=" * 80)
    print("Upcoming Earnings Fetcher (Next 60 Days)")
    print("=" * 80)
    
    # Connect to database
    conn = get_db_connection()
    print("✓ Database connected")
    
    # Fetch upcoming earnings
    print("\nFetching upcoming earnings...")
    earnings = fetch_upcoming_earnings(days_ahead=60)
    print(f"✓ Found {len(earnings)} upcoming earnings announcements")
    
    if not earnings:
        print("\n⚠ No upcoming earnings found")
        conn.close()
        return
    
    # Save to database
    print("\nSaving to database...")
    saved = 0
    for record in earnings:
        if upsert_earnings_record(conn, record):
            saved += 1
    
    print(f"✓ Saved {saved} earnings records")
    
    # Show upcoming earnings by date
    cursor = conn.cursor()
    cursor.execute("""
        SELECT date, COUNT(*) as count
        FROM earnings
        WHERE date >= CURRENT_DATE
        AND date <= CURRENT_DATE + INTERVAL '60 days'
        GROUP BY date
        ORDER BY date
        LIMIT 20
    """)
    
    print("\n" + "=" * 80)
    print("Upcoming Earnings Calendar (Next 20 Days):")
    print("=" * 80)
    for row in cursor.fetchall():
        date, count = row
        print(f"  {date}: {count} companies")
    
    # Show stocks in our portfolio with upcoming earnings
    cursor.execute("""
        SELECT e.ticker, e.date, e.time, e.estimated_eps, e.date_status
        FROM earnings e
        WHERE e.date >= CURRENT_DATE
        AND e.date <= CURRENT_DATE + INTERVAL '14 days'
        AND EXISTS (
            SELECT 1 FROM minervini_metrics mm 
            WHERE mm.symbol = e.ticker 
            AND mm.passes_minervini = true
            ORDER BY mm.date DESC 
            LIMIT 1
        )
        ORDER BY e.date
    """)
    
    upcoming = cursor.fetchall()
    if upcoming:
        print("\n" + "=" * 80)
        print("⚠️  WARNING: Stocks Passing Minervini with Earnings in Next 14 Days:")
        print("=" * 80)
        for ticker, date, time, est_eps, status in upcoming:
            time_str = time.strftime("%H:%M") if time else "TBD"
            print(f"  {ticker}: {date} at {time_str} ({status}) - Est EPS: ${est_eps if est_eps else 'N/A'}")
        print("\n  → Consider exiting or tightening stops before earnings!")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 80)
    print("✓ Update complete")
    print("=" * 80)


if __name__ == "__main__":
    main()
