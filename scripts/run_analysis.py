#!/usr/bin/env python3
"""
Convenience script to run analysis for specific dates or date ranges.
Useful for backfilling historical data or testing.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path
import importlib.util

# Import the MinerviniAnalyzer class
spec = importlib.util.spec_from_file_location("minervini", "./minervini.py")
minervini = importlib.util.module_from_spec(spec)
spec.loader.exec_module(minervini)


def analyze_date(date_str, recreate_db=False):
    """Analyze a specific date."""
    print(f"\n{'='*80}")
    print(f"Analyzing {date_str}")
    print(f"{'='*80}")
    
    analyzer = minervini.MinerviniAnalyzer(recreate_db=recreate_db)
    
    try:
        # Check if data exists in database
        if not analyzer.check_data_exists(date_str):
            print(f"⊙ Skipping {date_str} (no data in database)")
            return False
        
        # Analyze
        analyzed, passed = analyzer.analyze_date(date_str)
        
        print(f"\n✓ {date_str}: {passed}/{analyzed} stocks passed Minervini criteria")
        
        return True
        
    finally:
        analyzer.close()


def analyze_date_range(start_date_str, end_date_str):
    """Analyze a range of dates."""
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    print(f"\nAnalyzing date range: {start_date_str} to {end_date_str}")
    print(f"Total days: {(end_date - start_date).days + 1}\n")
    
    current_date = start_date
    successful = 0
    skipped = 0
    failed = 0
    first_date = True
    
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        
        try:
            # Only recreate DB on the first date
            result = analyze_date(date_str, recreate_db=first_date)
            first_date = False
            
            if result:
                successful += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"✗ Error analyzing {date_str}: {e}")
            failed += 1
        
        current_date += timedelta(days=1)
    
    print(f"\n{'='*80}")
    print(f"BATCH ANALYSIS SUMMARY")
    print(f"{'='*80}")
    print(f"Successful: {successful}")
    print(f"Skipped: {skipped}")
    print(f"Failed: {failed}")
    print(f"Total: {successful + skipped + failed}")


def analyze_last_n_days(n):
    """Analyze the last N days."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=n-1)
    
    analyze_date_range(
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d")
    )


def main():
    """Main execution."""
    if len(sys.argv) < 2:
        print("\nRun Analysis Tool")
        print("=" * 50)
        print("\nUsage:")
        print("  python run_analysis.py today             # Latest available (today minus 2 days)")
        print("  python run_analysis.py YYYY-MM-DD        # Specific date")
        print("  python run_analysis.py YYYY-MM-DD YYYY-MM-DD  # Date range")
        print("  python run_analysis.py last N            # Last N days")
        print("\nExamples:")
        print("  python run_analysis.py today")
        print("  python run_analysis.py 2026-02-01")
        print("  python run_analysis.py 2026-01-01 2026-01-31")
        print("  python run_analysis.py last 7")
        print("\nNote: Source data has a 24+ hour delay")
        sys.exit(0)
    
    if sys.argv[1].lower() == "today":
        # Use latest available date (2 days ago due to source data delay)
        latest_available = datetime.now() - timedelta(days=2)
        date_str = latest_available.strftime("%Y-%m-%d")
        print(f"Using latest available date: {date_str} (today minus 2 days)")
        analyze_date(date_str, recreate_db=True)
    
    elif sys.argv[1].lower() == "last":
        if len(sys.argv) < 3:
            print("Error: Specify number of days (e.g., 'last 7')")
            sys.exit(1)
        n_days = int(sys.argv[2])
        analyze_last_n_days(n_days)
    
    elif len(sys.argv) == 2:
        # Single date
        analyze_date(sys.argv[1], recreate_db=True)
    
    elif len(sys.argv) == 3:
        # Date range
        analyze_date_range(sys.argv[1], sys.argv[2])
    
    else:
        print("Invalid arguments. Use --help for usage.")
        sys.exit(1)


if __name__ == "__main__":
    main()
