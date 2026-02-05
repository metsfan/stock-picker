#!/usr/bin/env python3
"""
Quick query tool for the stock database.
Provides common queries and utilities.
"""

import psycopg2
from datetime import datetime, timedelta
from pathlib import Path
import sys


def connect_db():
    """Connect to the stocks database."""
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
        print(f"Error: Could not connect to PostgreSQL database")
        print(f"Details: {e}")
        print("Make sure PostgreSQL is running and the database exists.")
        sys.exit(1)


def get_latest_date(conn):
    """Get the most recent date in the database."""
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(date) FROM minervini_metrics")
    result = cursor.fetchone()
    return result[0] if result[0] else None


def show_passing_stocks(conn, date=None, limit=50):
    """Show stocks passing Minervini criteria."""
    if date is None:
        date = get_latest_date(conn)
    
    if not date:
        print("No data available in database.")
        return
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT symbol, close_price, ma_50, ma_150, ma_200,
               percent_from_52w_high, relative_strength, stage, criteria_passed
        FROM minervini_metrics
        WHERE date = %s AND passes_minervini = true
        ORDER BY relative_strength DESC
        LIMIT %s
    """, (date, limit))
    
    results = cursor.fetchall()
    
    print(f"\n{'='*90}")
    print(f"STOCKS PASSING MINERVINI CRITERIA - {date}")
    print(f"{'='*90}\n")
    
    if not results:
        print("No stocks passed all criteria on this date.")
        return
    
    print(f"{'Symbol':<8} {'Price':<10} {'MA50':<10} {'MA150':<10} "
          f"{'MA200':<10} {'%52wH':<10} {'RS':<6} {'Stage':<6} {'Score':<6}")
    print("-" * 96)
    
    for stock in results:
        symbol, price, ma50, ma150, ma200, pct_high, rs, stage, score = stock
        print(f"{symbol:<8} ${price:<9.2f} ${ma50:<9.2f} ${ma150:<9.2f} "
              f"${ma200:<9.2f} {pct_high:<9.1f}% {rs:<5.0f} {stage:<6} {score}/9")
    
    print(f"\nTotal: {len(results)} stocks")


def show_stock_history(conn, symbol, days=30):
    """Show historical data for a specific stock."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT date, close_price, ma_50, ma_150, ma_200,
               relative_strength, stage, passes_minervini, criteria_passed
        FROM minervini_metrics
        WHERE symbol = %s
        ORDER BY date DESC
        LIMIT %s
    """, (symbol.upper(), days))
    
    results = cursor.fetchall()
    
    if not results:
        print(f"\nNo data found for {symbol}")
        return
    
    print(f"\n{'='*100}")
    print(f"HISTORY FOR {symbol.upper()}")
    print(f"{'='*100}\n")
    
    print(f"{'Date':<12} {'Price':<10} {'MA50':<10} {'MA150':<10} "
          f"{'MA200':<10} {'RS':<6} {'Stage':<6} {'Pass':<6} {'Score':<6}")
    print("-" * 106)
    
    for row in results:
        date, price, ma50, ma150, ma200, rs, stage, passed, score = row
        pass_str = "✓" if passed else "✗"
        print(f"{date:<12} ${price:<9.2f} ${ma50:<9.2f} ${ma150:<9.2f} "
              f"${ma200:<9.2f} {rs or 0:<5.0f} {stage:<6} {pass_str:<6} {score}/9")


def show_statistics(conn, date=None):
    """Show overall statistics."""
    if date is None:
        date = get_latest_date(conn)
    
    if not date:
        print("No data available in database.")
        return
    
    cursor = conn.cursor()
    
    # Total stocks analyzed
    cursor.execute("SELECT COUNT(*) FROM minervini_metrics WHERE date = %s", (date,))
    total = cursor.fetchone()[0]
    
    # Stocks passing
    cursor.execute("SELECT COUNT(*) FROM minervini_metrics WHERE date = %s AND passes_minervini = true", (date,))
    passed = cursor.fetchone()[0]
    
    # Average RS
    cursor.execute("SELECT AVG(relative_strength) FROM minervini_metrics WHERE date = %s AND relative_strength IS NOT NULL", (date,))
    avg_rs = cursor.fetchone()[0]
    
    # Stage distribution
    cursor.execute("""
        SELECT stage, COUNT(*) 
        FROM minervini_metrics 
        WHERE date = %s
        GROUP BY stage
        ORDER BY stage
    """, (date,))
    stage_dist = cursor.fetchall()
    
    # Criteria distribution
    cursor.execute("""
        SELECT criteria_passed, COUNT(*) 
        FROM minervini_metrics 
        WHERE date = %s
        GROUP BY criteria_passed
        ORDER BY criteria_passed DESC
    """, (date,))
    criteria_dist = cursor.fetchall()
    
    print(f"\n{'='*60}")
    print(f"STATISTICS FOR {date}")
    print(f"{'='*60}\n")
    
    print(f"Total stocks analyzed: {total}")
    print(f"Passed all criteria: {passed} ({(passed/total*100) if total else 0:.1f}%)")
    print(f"Average Relative Strength: {avg_rs:.1f}" if avg_rs else "Average Relative Strength: N/A")
    
    print(f"\nStage Distribution:")
    stage_names = {1: "Basing", 2: "Advancing", 3: "Topping", 4: "Declining"}
    for stage, count in stage_dist:
        pct = (count/total*100) if total else 0
        bar = "█" * int(count / total * 40) if total else ""
        stage_name = stage_names.get(stage, "Unknown")
        print(f"  Stage {stage} ({stage_name:<9}): {count:4} ({pct:5.1f}%) {bar}")
    
    print(f"\nCriteria Score Distribution:")
    for score, count in criteria_dist:
        bar = "█" * int(count / total * 50) if total else ""
        print(f"  {score}/9: {count:4} {bar}")


def show_stocks_by_stage(conn, stage, date=None, limit=50):
    """Show stocks in a specific stage."""
    if date is None:
        date = get_latest_date(conn)
    
    if not date:
        print("No data available in database.")
        return
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT symbol, close_price, ma_50, ma_150, ma_200,
               percent_from_52w_high, relative_strength, criteria_passed
        FROM minervini_metrics
        WHERE date = %s AND stage = %s
        ORDER BY relative_strength DESC
        LIMIT %s
    """, (date, stage, limit))
    
    results = cursor.fetchall()
    
    stage_names = {1: "Basing/Accumulation", 2: "Advancing/Markup", 
                   3: "Topping/Distribution", 4: "Declining/Markdown"}
    
    print(f"\n{'='*90}")
    print(f"STAGE {stage} STOCKS ({stage_names.get(stage, 'Unknown')}) - {date}")
    print(f"{'='*90}\n")
    
    if not results:
        print(f"No stocks in stage {stage} on this date.")
        return
    
    print(f"{'Symbol':<8} {'Price':<10} {'MA50':<10} {'MA150':<10} "
          f"{'MA200':<10} {'%52wH':<10} {'RS':<6} {'Score':<6}")
    print("-" * 90)
    
    for stock in results:
        symbol, price, ma50, ma150, ma200, pct_high, rs, score = stock
        print(f"{symbol:<8} ${price:<9.2f} ${ma50:<9.2f} ${ma150:<9.2f} "
              f"${ma200:<9.2f} {pct_high:<9.1f}% {rs or 0:<5.0f} {score}/9")
    
    print(f"\nTotal: {len(results)} stocks")


def show_vcp_stocks(conn, date=None, limit=50, min_score=50):
    """Show stocks with VCP (Volatility Contraction Pattern) detected."""
    if date is None:
        date = get_latest_date(conn)
    
    if not date:
        print("No data available in database.")
        return
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT symbol, close_price, relative_strength, stage,
               vcp_score, contraction_count, latest_contraction_pct, 
               volume_contraction, pivot_price, passes_minervini
        FROM minervini_metrics
        WHERE date = %s AND vcp_detected = true AND vcp_score >= %s
        ORDER BY vcp_score DESC, relative_strength DESC
        LIMIT %s
    """, (date, min_score, limit))
    
    results = cursor.fetchall()
    
    print(f"\n{'='*110}")
    print(f"VCP (VOLATILITY CONTRACTION PATTERN) STOCKS - {date}")
    print(f"{'='*110}\n")
    
    if not results:
        print(f"No stocks with VCP detected (min score: {min_score}).")
        return
    
    print(f"{'Symbol':<8} {'Price':<10} {'RS':<6} {'Stage':<6} {'VCP':<6} "
          f"{'Cont':<5} {'Tight%':<8} {'VolDry':<7} {'Pivot':<10} {'Pass':<5}")
    print("-" * 110)
    
    for stock in results:
        symbol, price, rs, stage, vcp_score, cont_count, tight_pct, vol_dry, pivot, passes = stock
        vol_str = "Yes" if vol_dry else "No"
        pass_str = "✓" if passes else "-"
        tight_str = f"{tight_pct:.1f}%" if tight_pct else "-"
        print(f"{symbol:<8} ${price:<9.2f} {rs or 0:<5.0f} {stage:<6} {vcp_score:<5.0f} "
              f"{cont_count:<5} {tight_str:<8} {vol_str:<7} ${pivot:<9.2f} {pass_str:<5}")
    
    print(f"\nTotal: {len(results)} stocks with VCP pattern")
    print("\nLegend:")
    print("  Cont = Number of contractions")
    print("  Tight% = Latest contraction tightness (lower = better)")
    print("  VolDry = Volume dried up during consolidation")
    print("  Pivot = Potential breakout price")


def main():
    """Main menu."""
    if len(sys.argv) < 2:
        print("\nStock Database Query Tool")
        print("=" * 50)
        print("\nUsage:")
        print("  python query_stocks.py passing [limit]     - Show stocks passing Minervini")
        print("  python query_stocks.py stage STAGE [limit] - Show stocks in stage (1-4)")
        print("  python query_stocks.py vcp [limit] [min_score] - Show VCP pattern stocks")
        print("  python query_stocks.py stock SYMBOL [days] - Show history for a stock")
        print("  python query_stocks.py stats               - Show statistics")
        print("\nStages:")
        print("  1 = Basing/Accumulation")
        print("  2 = Advancing/Markup (ideal buy zone)")
        print("  3 = Topping/Distribution")
        print("  4 = Declining/Markdown")
        print("\nVCP (Volatility Contraction Pattern):")
        print("  Stocks consolidating with tightening price ranges")
        print("  Higher score = better setup for potential breakout")
        print("\nExamples:")
        print("  python query_stocks.py passing 20")
        print("  python query_stocks.py stage 2 50")
        print("  python query_stocks.py vcp 30 60              # Top 30 VCPs with score >= 60")
        print("  python query_stocks.py stock AAPL 60")
        print("  python query_stocks.py stats")
        sys.exit(0)
    
    conn = connect_db()
    command = sys.argv[1].lower()
    
    try:
        if command == "passing":
            limit = int(sys.argv[2]) if len(sys.argv) > 2 else 50
            show_passing_stocks(conn, limit=limit)
        
        elif command == "stage":
            if len(sys.argv) < 3:
                print("Error: Please specify a stage (1-4)")
                sys.exit(1)
            stage = int(sys.argv[2])
            if stage not in [1, 2, 3, 4]:
                print("Error: Stage must be 1, 2, 3, or 4")
                sys.exit(1)
            limit = int(sys.argv[3]) if len(sys.argv) > 3 else 50
            show_stocks_by_stage(conn, stage, limit=limit)
        
        elif command == "stock":
            if len(sys.argv) < 3:
                print("Error: Please specify a stock symbol")
                sys.exit(1)
            symbol = sys.argv[2]
            days = int(sys.argv[3]) if len(sys.argv) > 3 else 30
            show_stock_history(conn, symbol, days)
        
        elif command == "vcp":
            limit = int(sys.argv[2]) if len(sys.argv) > 2 else 50
            min_score = int(sys.argv[3]) if len(sys.argv) > 3 else 50
            show_vcp_stocks(conn, limit=limit, min_score=min_score)
        
        elif command == "stats":
            show_statistics(conn)
        
        else:
            print(f"Unknown command: {command}")
            print("Use: passing, stage, vcp, stock, or stats")
            sys.exit(1)
    
    finally:
        conn.close()


if __name__ == "__main__":
    main()
