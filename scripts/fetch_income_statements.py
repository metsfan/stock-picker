#!/usr/bin/env python3
"""
Fetch income statement data from Massive/Polygon Financials API

Retrieves quarterly and annual financial statements from SEC filings,
including revenue, EPS, expenses, and profitability metrics.

This data enables Minervini's fundamental analysis:
- EPS growth rates (QoQ and YoY)
- Revenue growth rates
- Earnings acceleration analysis
- Profit margin trends
- Operational efficiency metrics

API Documentation: https://massive.com/docs/rest/stocks/fundamentals/income-statements
"""

import requests
import psycopg2
from datetime import datetime, timedelta
from pathlib import Path
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
            print(f"Error: API key file not found at {key_file}")
            sys.exit(1)
    except Exception as e:
        print(f"Error reading API key file: {e}")
        sys.exit(1)

# API Configuration
API_KEY = _load_api_key()
API_BASE_URL = "https://api.massive.com/stocks/financials/v1/income-statements"
BATCH_SIZE = 10
BATCH_DELAY_MS = 100  # milliseconds between batches

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


def get_all_tickers_with_cik(conn):
    """
    Get all unique tickers with their CIK numbers from ticker_details.
    CIK is required for the income statements API.
    """
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT DISTINCT symbol, cik 
        FROM ticker_details 
        WHERE cik IS NOT NULL AND cik != ''
        ORDER BY symbol
    """)
    
    tickers = [(row[0], row[1]) for row in cursor.fetchall()]
    cursor.close()
    
    return tickers


def get_tickers_needing_update(conn, all_tickers):
    """
    Filter tickers to only those that need income statement updates.
    
    Returns tickers that either:
    - Have no income statement data at all, OR
    - Have data but the most recent period is older than 90 days
    
    Args:
        conn: Database connection
        all_tickers: List of (ticker, cik) tuples
        
    Returns:
        tuple: (tickers_to_fetch, tickers_with_data_count)
    """
    cursor = conn.cursor()
    
    # Get the most recent period_end for each CIK that has data
    cursor.execute("""
        SELECT cik, MAX(period_end) as latest_period
        FROM income_statements
        WHERE timeframe = 'quarterly'
        GROUP BY cik
    """)
    
    latest_periods = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.close()
    
    tickers_to_fetch = []
    tickers_with_data_count = 0
    current_date = datetime.now().date()
    
    for ticker, cik in all_tickers:
        latest_period = latest_periods.get(cik)
        
        if latest_period is None:
            # No data at all - definitely fetch
            tickers_to_fetch.append((ticker, cik, None))
        else:
            # Calculate how old the data is
            days_old = (current_date - latest_period).days
            
            # Fetch if data is older than 90 days (roughly 1 quarter)
            if days_old > 90:
                tickers_to_fetch.append((ticker, cik, latest_period))
            else:
                tickers_with_data_count += 1
    
    return tickers_to_fetch, tickers_with_data_count


def fetch_income_statements_for_ticker(ticker, cik, start_date=None, limit=20):
    """
    Fetch income statements for a specific ticker/CIK.
    
    Args:
        ticker: Stock symbol (for display)
        cik: Central Index Key (required by API)
        start_date: Start date (YYYY-MM-DD) for historical data
        limit: Maximum number of records (default 20 = ~5 years quarterly)
        
    Returns:
        list: Income statement records
    """
    params = {
        'cik': cik,
        'timeframe': 'quarterly',  # quarterly data for growth calculations
        'sort': 'period_end.desc',
        'limit': limit,
        'apiKey': API_KEY
    }
    
    if start_date:
        params['period_end.gte'] = start_date
    
    try:
        response = requests.get(API_BASE_URL, params=params, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get('status') == 'OK':
                return data.get('results', [])
            else:
                print(f"  ⚠ {ticker}: API returned status {data.get('status', 'UNKNOWN')}")
                return []
        elif response.status_code == 404:
            # No financial data for this CIK
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


def process_ticker_statements(ticker_cik_tuple, conn, start_date):
    """
    Process income statements for a single ticker.
    
    Args:
        ticker_cik_tuple: Tuple of (ticker, cik, latest_period) where latest_period is the 
                         most recent period_end we have in the DB (or None if no data)
        conn: Database connection
        start_date: Default start date for fetching (used if no existing data)
        
    Returns:
        dict: Results with ticker, saved_count, growth info, etc.
    """
    ticker, cik, latest_period = ticker_cik_tuple
    
    # If we have existing data, fetch only data after that date
    # Otherwise use the default start_date
    fetch_start_date = start_date
    if latest_period:
        # Add 1 day to avoid re-fetching the same period
        fetch_start_date = (latest_period + timedelta(days=1)).strftime("%Y-%m-%d")
    
    try:
        # Fetch income statements for this ticker
        statements = fetch_income_statements_for_ticker(ticker, cik, fetch_start_date, limit=20)
        
        if statements:
            # Save each statement
            saved_count = 0
            for record in statements:
                if upsert_income_statement(conn, record, ticker):
                    saved_count += 1
            
            # Calculate growth info if we have enough data
            growth_str = None
            if len(statements) >= 4:  # At least 4 quarters
                latest = statements[0]
                year_ago = statements[3] if len(statements) > 3 else None
                
                if latest.get('basic_earnings_per_share') and year_ago and year_ago.get('basic_earnings_per_share'):
                    latest_eps = float(latest['basic_earnings_per_share'])
                    year_ago_eps = float(year_ago['basic_earnings_per_share'])
                    
                    if year_ago_eps > 0:
                        growth = ((latest_eps - year_ago_eps) / year_ago_eps) * 100
                        growth_str = f"{growth:+.1f}%"
            
            return {
                'ticker': ticker,
                'success': True,
                'saved_count': saved_count,
                'growth': growth_str
            }
        else:
            return {
                'ticker': ticker,
                'success': False,
                'saved_count': 0,
                'growth': None
            }
            
    except Exception as e:
        return {
            'ticker': ticker,
            'success': False,
            'error': str(e)[:100],
            'saved_count': 0,
            'growth': None
        }


def process_batch(conn, tickers_batch, start_date):
    """
    Process a batch of tickers concurrently.
    
    Args:
        conn: Database connection
        tickers_batch: List of (ticker, cik) tuples
        start_date: Start date for fetching
        
    Returns:
        dict: Statistics of the batch processing
    """
    stats = {'success': 0, 'failed': 0, 'total_statements': 0}
    
    with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
        # Submit all tasks
        futures = {
            executor.submit(process_ticker_statements, ticker_cik, conn, start_date): ticker_cik[0]
            for ticker_cik in tickers_batch
        }
        
        # Process results as they complete
        for future in as_completed(futures):
            result = future.result()
            
            if result['success']:
                stats['success'] += 1
                stats['total_statements'] += result['saved_count']
                
                growth_info = f" | YoY EPS Growth: {result['growth']}" if result['growth'] else ""
                print(f"  ✓ {result['ticker']}: {result['saved_count']} quarters{growth_info}")
            else:
                stats['failed'] += 1
                if 'error' in result:
                    print(f"  ✗ {result['ticker']}: {result['error']}")
    
    return stats


def upsert_income_statement(conn, record, ticker=None):
    """
    Insert or update an income statement record in the database.
    
    Args:
        conn: Database connection
        record: Income statement record from API
        ticker: Primary ticker symbol (extracted from tickers array)
        
    Returns:
        bool: True if successful, False otherwise
    """
    cursor = conn.cursor()
    
    # Extract primary ticker from tickers array if not provided
    if not ticker and record.get('tickers'):
        ticker = record['tickers'][0] if isinstance(record['tickers'], list) else None
    
    try:
        cursor.execute("""
            INSERT INTO income_statements (
                cik, tickers, ticker, period_end, filing_date, fiscal_year, fiscal_quarter, timeframe,
                revenue, cost_of_revenue, gross_profit, operating_income,
                net_income_loss_attributable_common_shareholders, consolidated_net_income_loss,
                basic_earnings_per_share, diluted_earnings_per_share,
                basic_shares_outstanding, diluted_shares_outstanding,
                total_operating_expenses, selling_general_administrative,
                research_development, depreciation_depletion_amortization, other_operating_expenses,
                total_other_income_expense, interest_income, interest_expense, other_income_expense,
                income_before_income_taxes, income_taxes, discontinued_operations, extraordinary_items,
                ebitda, equity_in_affiliates, noncontrolling_interest,
                preferred_stock_dividends_declared, fetched_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            ON CONFLICT (cik, period_end, timeframe)
            DO UPDATE SET
                tickers = EXCLUDED.tickers,
                ticker = EXCLUDED.ticker,
                filing_date = EXCLUDED.filing_date,
                fiscal_year = EXCLUDED.fiscal_year,
                fiscal_quarter = EXCLUDED.fiscal_quarter,
                revenue = EXCLUDED.revenue,
                cost_of_revenue = EXCLUDED.cost_of_revenue,
                gross_profit = EXCLUDED.gross_profit,
                operating_income = EXCLUDED.operating_income,
                net_income_loss_attributable_common_shareholders = EXCLUDED.net_income_loss_attributable_common_shareholders,
                consolidated_net_income_loss = EXCLUDED.consolidated_net_income_loss,
                basic_earnings_per_share = EXCLUDED.basic_earnings_per_share,
                diluted_earnings_per_share = EXCLUDED.diluted_earnings_per_share,
                basic_shares_outstanding = EXCLUDED.basic_shares_outstanding,
                diluted_shares_outstanding = EXCLUDED.diluted_shares_outstanding,
                total_operating_expenses = EXCLUDED.total_operating_expenses,
                selling_general_administrative = EXCLUDED.selling_general_administrative,
                research_development = EXCLUDED.research_development,
                depreciation_depletion_amortization = EXCLUDED.depreciation_depletion_amortization,
                other_operating_expenses = EXCLUDED.other_operating_expenses,
                total_other_income_expense = EXCLUDED.total_other_income_expense,
                interest_income = EXCLUDED.interest_income,
                interest_expense = EXCLUDED.interest_expense,
                other_income_expense = EXCLUDED.other_income_expense,
                income_before_income_taxes = EXCLUDED.income_before_income_taxes,
                income_taxes = EXCLUDED.income_taxes,
                discontinued_operations = EXCLUDED.discontinued_operations,
                extraordinary_items = EXCLUDED.extraordinary_items,
                ebitda = EXCLUDED.ebitda,
                equity_in_affiliates = EXCLUDED.equity_in_affiliates,
                noncontrolling_interest = EXCLUDED.noncontrolling_interest,
                preferred_stock_dividends_declared = EXCLUDED.preferred_stock_dividends_declared,
                fetched_at = EXCLUDED.fetched_at
        """, (
            record.get('cik'),
            record.get('tickers'),
            ticker,
            record.get('period_end'),
            record.get('filing_date'),
            record.get('fiscal_year'),
            record.get('fiscal_quarter'),
            record.get('timeframe'),
            record.get('revenue'),
            record.get('cost_of_revenue'),
            record.get('gross_profit'),
            record.get('operating_income'),
            record.get('net_income_loss_attributable_common_shareholders'),
            record.get('consolidated_net_income_loss'),
            record.get('basic_earnings_per_share'),
            record.get('diluted_earnings_per_share'),
            record.get('basic_shares_outstanding'),
            record.get('diluted_shares_outstanding'),
            record.get('total_operating_expenses'),
            record.get('selling_general_administrative'),
            record.get('research_development'),
            record.get('depreciation_depletion_amortization'),
            record.get('other_operating_expenses'),
            record.get('total_other_income_expense'),
            record.get('interest_income'),
            record.get('interest_expense'),
            record.get('other_income_expense'),
            record.get('income_before_income_taxes'),
            record.get('income_taxes'),
            record.get('discontinued_operations'),
            record.get('extraordinary_items'),
            record.get('ebitda'),
            record.get('equity_in_affiliates'),
            record.get('noncontrolling_interest'),
            record.get('preferred_stock_dividends_declared'),
            datetime.now()
        ))
        
        conn.commit()
        cursor.close()
        return True
        
    except Exception as e:
        print(f"  Error saving income statement: {e}")
        conn.rollback()
        cursor.close()
        return False


def main():
    """Main function to fetch and store income statement data."""
    print("=" * 80)
    print("Income Statements Fetcher (SEC Filings)")
    print("=" * 80)
    
    # Connect to database
    print("\n1. Connecting to database...")
    conn = get_db_connection()
    print("   ✓ Database connected")
    
    # Get all tickers with CIK numbers
    print("\n2. Fetching tickers with CIK numbers...")
    all_tickers_with_cik = get_all_tickers_with_cik(conn)
    
    if not all_tickers_with_cik:
        print("   ✗ No tickers with CIK found!")
        print("   → Run fetch_ticker_details.py first to populate CIK numbers")
        conn.close()
        sys.exit(1)
    
    print(f"   ✓ Found {len(all_tickers_with_cik)} tickers with CIK")
    
    # Filter to only tickers needing updates
    print("\n3. Checking for existing income statement data...")
    tickers_to_fetch, tickers_up_to_date = get_tickers_needing_update(conn, all_tickers_with_cik)
    
    print(f"   ✓ {len(tickers_to_fetch)} tickers need updates")
    print(f"   ✓ {tickers_up_to_date} tickers already have recent data (< 90 days old)")
    
    if tickers_to_fetch and len(tickers_to_fetch) <= 10:
        # Show which tickers are being fetched if there are only a few
        ticker_names = [t[0] for t in tickers_to_fetch[:10]]
        print(f"   → Fetching: {', '.join(ticker_names)}")
    
    if not tickers_to_fetch:
        print("\n   All tickers have recent income statement data!")
        print("   Nothing to fetch.")
        conn.close()
        return
    
    # Date range for historical data (last 5 years = 20 quarters)
    start_date = (datetime.now() - timedelta(days=5*365)).strftime("%Y-%m-%d")
    
    print(f"\n4. Fetching income statements (default since {start_date})...")
    print(f"   Fetching last 20 quarters (~5 years) per ticker")
    print(f"   For tickers with existing data, only fetching new periods")
    print(f"   Processing {BATCH_SIZE} tickers concurrently per batch")
    print("-" * 80)
    
    total_statements = 0
    tickers_with_data = 0
    tickers_without_data = 0
    
    # Split tickers into batches
    batches = [tickers_to_fetch[i:i + BATCH_SIZE] for i in range(0, len(tickers_to_fetch), BATCH_SIZE)]
    
    for batch_num, batch in enumerate(batches, 1):
        try:
            print(f"\nBatch {batch_num}/{len(batches)} ({len(batch)} tickers):")
            
            batch_stats = process_batch(conn, batch, start_date)
            total_statements += batch_stats['total_statements']
            tickers_with_data += batch_stats['success']
            tickers_without_data += batch_stats['failed']
            
            # Progress checkpoint every 10 batches
            if batch_num % 10 == 0:
                processed = batch_num * BATCH_SIZE
                print(f"\n  Progress checkpoint: ~{processed}/{len(tickers_to_fetch)} tickers processed")
                print(f"  Total statements: {total_statements} | With data: {tickers_with_data} | Without: {tickers_without_data}\n")
            
            # Delay between batches (except for the last one)
            if batch_num < len(batches):
                time.sleep(BATCH_DELAY_MS / 1000.0)
            
        except KeyboardInterrupt:
            print(f"\n\nInterrupted by user at batch {batch_num}/{len(batches)}")
            print(f"Progress saved. Processed ~{batch_num * BATCH_SIZE} tickers")
            break
        except Exception as e:
            print(f"  Batch {batch_num} error: {str(e)[:100]}")
            continue
    
    # Close database connection
    conn.close()
    
    # Print summary
    print("\n" + "=" * 80)
    print("Summary:")
    print(f"  Total tickers in database: {len(all_tickers_with_cik)}")
    print(f"  Tickers already up-to-date: {tickers_up_to_date}")
    print(f"  Tickers checked for updates: {tickers_with_data + tickers_without_data}")
    print(f"  Tickers with new data fetched: {tickers_with_data}")
    print(f"  Tickers without financial data: {tickers_without_data}")
    print(f"  Total income statements saved: {total_statements}")
    if tickers_with_data + tickers_without_data > 0:
        print(f"  Fetch success rate: {(tickers_with_data/(tickers_with_data + tickers_without_data)*100):.1f}%")
    print("=" * 80)
    
    # Show some statistics
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get growth statistics
    cursor.execute("""
        WITH quarterly_data AS (
            SELECT 
                ticker,
                period_end,
                basic_earnings_per_share as eps,
                revenue,
                LAG(basic_earnings_per_share, 4) OVER (PARTITION BY ticker ORDER BY period_end) as eps_year_ago,
                LAG(revenue, 4) OVER (PARTITION BY ticker ORDER BY period_end) as revenue_year_ago
            FROM income_statements
            WHERE timeframe = 'quarterly'
            AND basic_earnings_per_share IS NOT NULL
            AND revenue IS NOT NULL
        )
        SELECT 
            COUNT(*) as stocks_with_growth_data,
            AVG(CASE 
                WHEN eps_year_ago > 0 
                THEN ((eps - eps_year_ago) / eps_year_ago * 100)
                ELSE NULL 
            END) as avg_eps_growth,
            AVG(CASE 
                WHEN revenue_year_ago > 0 
                THEN ((revenue - revenue_year_ago) / revenue_year_ago::numeric * 100)
                ELSE NULL 
            END) as avg_revenue_growth,
            COUNT(CASE 
                WHEN eps_year_ago > 0 
                AND ((eps - eps_year_ago) / eps_year_ago) >= 0.25 
                THEN 1 
            END) as stocks_with_25pct_growth
        FROM quarterly_data
        WHERE eps_year_ago IS NOT NULL
    """)
    
    stats = cursor.fetchone()
    if stats and stats[0]:
        print(f"\nGrowth Statistics:")
        print(f"  Stocks with YoY data: {stats[0]}")
        print(f"  Average EPS growth: {stats[1]:.1f}%" if stats[1] else "  Average EPS growth: N/A")
        print(f"  Average revenue growth: {stats[2]:.1f}%" if stats[2] else "  Average revenue growth: N/A")
        print(f"  Stocks with 25%+ EPS growth: {stats[3]}")
    
    # Show top performers
    cursor.execute("""
        WITH quarterly_data AS (
            SELECT 
                ticker,
                period_end,
                basic_earnings_per_share as eps,
                LAG(basic_earnings_per_share, 4) OVER (PARTITION BY ticker ORDER BY period_end) as eps_year_ago,
                ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_end DESC) as rn
            FROM income_statements
            WHERE timeframe = 'quarterly'
            AND basic_earnings_per_share IS NOT NULL
        )
        SELECT 
            ticker,
            eps,
            eps_year_ago,
            ((eps - eps_year_ago) / eps_year_ago * 100) as growth_pct
        FROM quarterly_data
        WHERE rn = 1 
        AND eps_year_ago > 0
        AND ((eps - eps_year_ago) / eps_year_ago) >= 0.25
        ORDER BY growth_pct DESC
        LIMIT 10
    """)
    
    top_growth = cursor.fetchall()
    if top_growth:
        print(f"\nTop 10 EPS Growth Stocks (YoY):")
        print(f"  {'Ticker':<8} {'Latest EPS':>12} {'Year Ago':>12} {'Growth':>10}")
        print("  " + "-" * 50)
        for ticker, eps, eps_ago, growth in top_growth:
            print(f"  {ticker:<8} ${eps:>11.2f} ${eps_ago:>11.2f} {growth:>9.1f}%")
    
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
