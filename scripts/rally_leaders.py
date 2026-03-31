#!/usr/bin/env python3
"""
Post-Correction Rally Leaders -- Standalone CLI

Run AFTER the daily pipeline (fetch_stock_prices + minervini + fetch_macd)
to find the stocks Mark Minervini would target on a big rally day following
a market downturn.

Usage:
    python rally_leaders.py                    # defaults: latest date, 30-day lookback
    python rally_leaders.py --date 2026-03-31  # specific date
    python rally_leaders.py --lookback 45      # 45-day correction window
    python rally_leaders.py --top 30           # show top 30 (default 50)
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from minervini.rally_screener import RallyDayScreener, print_rally_leaders


def main():
    parser = argparse.ArgumentParser(
        description="Find post-correction rally leaders (Minervini methodology)"
    )
    parser.add_argument(
        '--date', type=str, default=None,
        help='Analysis date (YYYY-MM-DD). Defaults to latest in stock_prices.'
    )
    parser.add_argument(
        '--lookback', type=int, default=30,
        help='Correction lookback in calendar days for drawdown comparison (default: 30)'
    )
    parser.add_argument(
        '--top', type=int, default=50,
        help='Number of top results to show (default: 50)'
    )
    args = parser.parse_args()

    screener = RallyDayScreener()

    try:
        candidates = screener.screen(
            date=args.date,
            correction_lookback_days=args.lookback,
            limit=args.top,
        )

        spy_change = candidates[0]['spy_change'] if candidates else None
        print_rally_leaders(candidates, spy_change=spy_change)

        print(f"\nFound {len(candidates)} rally leader candidate(s)")
        print(f"Database: PostgreSQL @ localhost:5432/stocks")

    finally:
        screener.close()


if __name__ == '__main__':
    main()
