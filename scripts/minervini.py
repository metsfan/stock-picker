#!/usr/bin/env python3
"""
Minervini Stock Analyzer (Enhanced with Company Fundamentals)

Analyzes stocks based on Mark Minervini's Trend Template Criteria:
(Reference: "Trade Like a Stock Market Wizard" by Mark Minervini)

Book Criterion #1: Current price above both 150-day and 200-day MA
Book Criterion #2: 150-day MA above 200-day MA
Book Criterion #3: 200-day MA trending upward for at least 1 month
Book Criterion #4: 50-day MA above both 150-day and 200-day MA
Book Criterion #5: Current price above 50-day MA
Book Criterion #6: Price at least 30% above 52-week low
Book Criterion #7: Price within 25% of 52-week high
Book Criterion #8: RS Rating >= 70 (IBD-style percentile ranking)

These 8 book criteria expand to 9 discrete checks + RS filter (since #1 and #4
each test two MA relationships).

ENHANCED FEATURES (using ticker_details table):
- Market Cap Filtering: Automatically excludes stocks < $100M (too illiquid)
- Liquidity Scoring: Classifies stocks by market cap tier (Micro/Small/Mid/Large/Mega)
- Institutional Quality: Flags stocks with sufficient size for institutional investment
- Active Stock Filter: Excludes delisted/inactive stocks
- Quality Adjustment: Applies stricter RS requirements for lower quality stocks

Minervini Market Cap Philosophy:
- Avoids micro caps (<$300M) due to liquidity and manipulation concerns
- Sweet spot: $300M - $50B (growth potential + liquidity)
- Prefers stocks with institutional sponsorship

Stage Analysis:
Each stock is classified into one of four market stages based on Minervini's methodology:

Stage 1 - Basing/Accumulation:
    - Consolidating after decline, preparing for potential advance
    - MAs converging, price stabilizing
    - Default for stocks that don't clearly fit other stages

Stage 2 - Advancing/Markup (IDEAL BUY ZONE):
    - Strong uptrend with proper technical setup
    - Price above all MAs, MAs in correct order (50>150>200)
    - 200-day MA trending up, price within 25% of 52-week high
    - Strong relative strength (RS >= 60)

Stage 3 - Topping/Distribution:
    - Peaked and losing momentum
    - Price may be elevated but technical deterioration visible
    - MAs flattening or starting to turn down
    - Weakening relative strength

Stage 4 - Declining/Markdown:
    - Clear downtrend
    - Price below 200-day MA, MAs in wrong order
    - 200-day MA trending down significantly
    - Far from 52-week high, weak relative strength

This script processes daily stock data and stores metrics in PostgreSQL for trend analysis.
"""

import psycopg2
from datetime import datetime, timedelta, date
from pathlib import Path
import sys


class MinerviniAnalyzer:
    def __init__(self, recreate_db=True):
        """Initialize the analyzer with database connection."""
        # PostgreSQL connection parameters
        self.conn_params = {
            'host': 'localhost',
            'port': 5432,
            'database': 'stocks',
            'user': 'stocks',
            'password': 'stocks'
        }
        
        # Market benchmark symbols and their weights for relative strength calculation
        # Weights reflect the importance/relevance of each index
        self.market_benchmarks = {
            'SPY': 0.35,   # S&P 500 - Broad US large cap (most important)
            'DIA': 0.15,   # Dow Jones Industrial Average - Blue chip stocks
            'QQQ': 0.25,   # Nasdaq-100 - Tech-heavy index
            'VTI': 0.20,   # Total US Stock Market - Comprehensive US coverage
            'VT': 0.05     # Total World Stock - Global diversification
        }
        # Total weights should sum to 1.0
        
        # Cache for market performance by date
        self.market_performance_cache = {}
        
        # Connect to PostgreSQL
        self.conn = psycopg2.connect(**self.conn_params)
        self.conn.autocommit = False
        
        # Clear analysis tables if requested
        if recreate_db:
            self.truncate_metrics_table()
    
    def truncate_metrics_table(self):
        """Clear all data from minervini_metrics table."""
        cursor = self.conn.cursor()
        try:
            cursor.execute("TRUNCATE TABLE minervini_metrics CASCADE")
            self.conn.commit()
            print("✓ Cleared previous analysis results")
        except Exception as e:
            print(f"Error truncating table: {e}")
            print("Make sure to run setup_database.sql first to create tables")
            self.conn.rollback()
            raise
        finally:
            cursor.close()
    
    def check_data_exists(self, date):
        """Check if data exists in the database for a given date."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM stock_prices WHERE date = %s
        """, (date,))
        count = cursor.fetchone()[0]
        cursor.close()
        return count > 0
    
    def get_ticker_details(self, symbol):
        """
        Get ticker details from ticker_details table.
        Returns dict with company fundamentals or None if not found.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT symbol, name, market_cap, active, total_employees,
                   primary_exchange, sic_description, list_date
            FROM ticker_details
            WHERE symbol = %s
        """, (symbol,))
        
        result = cursor.fetchone()
        cursor.close()
        
        if not result:
            return None
        
        return {
            'symbol': result[0],
            'name': result[1],
            'market_cap': result[2],
            'active': result[3],
            'total_employees': result[4],
            'primary_exchange': result[5],
            'industry': result[6],
            'list_date': result[7]
        }
    
    def classify_market_cap_tier(self, market_cap):
        """
        Classify stock by market cap tier following Minervini principles.
        
        Returns:
            tuple: (tier_name, liquidity_score, institutional_quality)
            
        Minervini typically focuses on stocks with sufficient liquidity:
        - Micro caps (<$300M): Too risky, prone to manipulation
        - Small caps ($300M-$2B): Good for aggressive growth
        - Mid caps ($2B-$10B): Sweet spot - growth potential + liquidity
        - Large caps ($10B-$200B): Stable, institutional favorites
        - Mega caps (>$200B): Slower growth, but very stable
        """
        if market_cap is None:
            return ('Unknown', 0, False)
        
        if market_cap < 300_000_000:  # < $300M
            return ('Micro Cap', 2, False)
        elif market_cap < 2_000_000_000:  # $300M - $2B
            return ('Small Cap', 6, True)
        elif market_cap < 10_000_000_000:  # $2B - $10B
            return ('Mid Cap', 9, True)
        elif market_cap < 200_000_000_000:  # $10B - $200B
            return ('Large Cap', 8, True)
        else:  # > $200B
            return ('Mega Cap', 7, True)
    
    def calculate_moving_average(self, symbol, end_date, days):
        """Calculate moving average for a symbol over specified days."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT close FROM stock_prices
            WHERE symbol = %s AND date <= %s
            ORDER BY date DESC
            LIMIT %s
        """, (symbol, end_date, days))
        
        prices = [float(row[0]) for row in cursor.fetchall()]
        cursor.close()
        
        if len(prices) < days:
            return None
        
        return sum(prices) / len(prices)
    
    def get_52_week_high_low(self, symbol, end_date):
        """Get 52-week high and low for a symbol."""
        cursor = self.conn.cursor()
        
        # Calculate date 52 weeks ago
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        start_date = (end_date_obj - timedelta(weeks=52)).strftime("%Y-%m-%d")
        
        cursor.execute("""
            SELECT MAX(high), MIN(low) FROM stock_prices
            WHERE symbol = %s AND date BETWEEN %s AND %s
        """, (symbol, start_date, end_date))
        
        result = cursor.fetchone()
        cursor.close()
        return (float(result[0]) if result[0] is not None else None,
                float(result[1]) if result[1] is not None else None)
    
    def calculate_ma_trend(self, symbol, end_date, ma_period, trend_days=20):
        """
        Calculate the trend (slope) of a moving average over specified days.
        
        Args:
            symbol: Stock symbol
            end_date: Date to calculate trend for
            ma_period: Moving average period (e.g., 150, 200)
            trend_days: Days to measure trend over (default 20 ≈ 1 month)
            
        Returns:
            float: Percentage change in MA over the period, or None
        """
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        start_trend_date = (end_date_obj - timedelta(days=trend_days)).strftime("%Y-%m-%d")
        
        ma_today = self.calculate_moving_average(symbol, end_date, ma_period)
        ma_trend_start = self.calculate_moving_average(symbol, start_trend_date, ma_period)
        
        if ma_today is None or ma_trend_start is None:
            return None
        
        return ((ma_today - ma_trend_start) / ma_trend_start) * 100
    
    def calculate_ema(self, symbol, end_date, period):
        """
        Calculate Exponential Moving Average.
        
        EMA gives more weight to recent prices, making it more responsive
        than SMA. Critical for Minervini's pullback entries and trailing stops.
        
        Key levels:
        - 10-day EMA: First support in a strong uptrend (tight trailing stop)
        - 21-day EMA: Intermediate trend support (Minervini's key pullback level)
        
        Args:
            symbol: Stock symbol
            end_date: Date to calculate EMA for
            period: EMA period (e.g., 10, 21)
            
        Returns:
            float: EMA value or None if insufficient data
        """
        cursor = self.conn.cursor()
        # Need extra data for EMA warm-up period
        cursor.execute("""
            SELECT close FROM stock_prices
            WHERE symbol = %s AND date <= %s
            ORDER BY date DESC
            LIMIT %s
        """, (symbol, end_date, period * 3))
        
        prices = [float(row[0]) for row in cursor.fetchall()]
        cursor.close()
        
        if len(prices) < period:
            return None
        
        prices.reverse()  # Oldest first for EMA calculation
        multiplier = 2 / (period + 1)
        ema = sum(prices[:period]) / period  # Seed with SMA
        
        for price in prices[period:]:
            ema = (price - ema) * multiplier + ema
        
        return round(ema, 2)
    
    def calculate_ma_200_trend(self, symbol, end_date):
        """
        Calculate the trend of 200-day MA over the last 20 days.
        Returns the change in MA over this period.
        
        Minervini requires 200-day MA trending up for at least 1 month.
        """
        return self.calculate_ma_trend(symbol, end_date, 200, 20)
    
    def calculate_ma_150_trend(self, symbol, end_date):
        """
        Calculate the trend of 150-day MA over the last 20 days.
        Returns the change in MA over this period.
        
        Minervini requires 150-day MA trending upward.
        """
        return self.calculate_ma_trend(symbol, end_date, 150, 20)
    
    def get_market_performance(self, end_date):
        """
        Calculate weighted market performance using benchmark indices.
        Results are cached per date for efficiency.
        
        Uses weighted average based on index importance:
        - SPY (35%): S&P 500 - Broad US large cap
        - DIA (15%): Dow Jones - Blue chip stocks
        - QQQ (25%): Nasdaq-100 - Tech-heavy
        - VTI (20%): Total US Market - Comprehensive coverage
        - VT (5%): Total World - Global diversification
        
        Args:
            end_date: Date string in YYYY-MM-DD format
            
        Returns:
            float: Weighted market performance percentage over 90 days
        """
        # Check cache first
        if end_date in self.market_performance_cache:
            return self.market_performance_cache[end_date]
        
        cursor = self.conn.cursor()
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        start_date = (end_date_obj - timedelta(days=90)).strftime("%Y-%m-%d")
        
        weighted_performance = 0
        total_weight = 0
        
        for symbol, weight in self.market_benchmarks.items():
            cursor.execute("""
                SELECT close FROM stock_prices
                WHERE symbol = %s AND date BETWEEN %s AND %s
                ORDER BY date ASC
            """, (symbol, start_date, end_date))
            
            prices = [float(row[0]) for row in cursor.fetchall()]
            if len(prices) >= 2:
                perf = ((prices[-1] - prices[0]) / prices[0]) * 100
                weighted_performance += perf * weight
                total_weight += weight
        
        # Calculate weighted average (normalize by total weight in case some indices missing)
        if total_weight > 0:
            market_avg = weighted_performance / total_weight
        else:
            market_avg = 0  # Fallback if no market data
        
        # Cache the result
        self.market_performance_cache[end_date] = market_avg
        
        cursor.close()
        return market_avg
    
    def calculate_all_stock_performances(self, end_date):
        """
        Calculate weighted performance for all stocks.
        
        IBD-style RS uses weighted 12-month performance:
        - 40% weight on most recent quarter (63 days)
        - 20% weight on Q2 (63-126 days)
        - 20% weight on Q3 (126-189 days)
        - 20% weight on Q4 (189-252 days)
        
        Args:
            end_date: Date to calculate performances for
            
        Returns:
            dict: {symbol: weighted_performance} for all stocks
        """
        # Check cache first
        cache_key = f"all_perf_{end_date}"
        if hasattr(self, '_perf_cache') and cache_key in self._perf_cache:
            return self._perf_cache[cache_key]
        
        if not hasattr(self, '_perf_cache'):
            self._perf_cache = {}
        
        cursor = self.conn.cursor()
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Define quarters for weighted calculation
        quarters = [
            ((end_date_obj - timedelta(days=63)).strftime("%Y-%m-%d"), end_date, 0.40),   # Q1: most recent
            ((end_date_obj - timedelta(days=126)).strftime("%Y-%m-%d"), 
             (end_date_obj - timedelta(days=63)).strftime("%Y-%m-%d"), 0.20),              # Q2
            ((end_date_obj - timedelta(days=189)).strftime("%Y-%m-%d"), 
             (end_date_obj - timedelta(days=126)).strftime("%Y-%m-%d"), 0.20),             # Q3
            ((end_date_obj - timedelta(days=252)).strftime("%Y-%m-%d"), 
             (end_date_obj - timedelta(days=189)).strftime("%Y-%m-%d"), 0.20),             # Q4
        ]
        
        # Get all symbols with sufficient data
        cursor.execute("""
            SELECT DISTINCT symbol FROM stock_prices
            WHERE date = %s
        """, (end_date,))
        all_symbols = [row[0] for row in cursor.fetchall()]
        
        performances = {}
        
        for symbol in all_symbols:
            weighted_perf = 0
            total_weight = 0
            
            for start_q, end_q, weight in quarters:
                cursor.execute("""
                    SELECT close FROM stock_prices
                    WHERE symbol = %s AND date >= %s AND date <= %s
                    ORDER BY date ASC
                """, (symbol, start_q, end_q))
                
                prices = [float(row[0]) for row in cursor.fetchall()]
                
                if len(prices) >= 2 and prices[0] != 0:
                    qtr_perf = ((prices[-1] - prices[0]) / prices[0]) * 100
                    weighted_perf += qtr_perf * weight
                    total_weight += weight
            
            if total_weight > 0:
                # Normalize by actual weight used (in case some quarters missing)
                performances[symbol] = weighted_perf / total_weight * (0.40 + 0.20 + 0.20 + 0.20)
        
        cursor.close()
        
        # Cache the result
        self._perf_cache[cache_key] = performances
        return performances
    
    def calculate_rs_percentile_ranking(self, end_date):
        """
        Calculate IBD-style percentile RS ranking for all stocks.
        
        RS Rating is a percentile ranking (1-99) comparing each stock's
        12-month weighted performance against all other stocks.
        
        Args:
            end_date: Date to calculate rankings for
            
        Returns:
            dict: {symbol: rs_percentile} for all stocks (1-99 scale)
        """
        # Check cache first
        cache_key = f"rs_rank_{end_date}"
        if hasattr(self, '_rs_cache') and cache_key in self._rs_cache:
            return self._rs_cache[cache_key]
        
        if not hasattr(self, '_rs_cache'):
            self._rs_cache = {}
        
        # Get all stock performances
        performances = self.calculate_all_stock_performances(end_date)
        
        if not performances:
            return {}
        
        # Sort by performance
        sorted_symbols = sorted(performances.keys(), key=lambda s: performances[s])
        total_stocks = len(sorted_symbols)
        
        # Calculate percentile rank for each stock
        rs_rankings = {}
        for rank, symbol in enumerate(sorted_symbols, 1):
            # Percentile: what percentage of stocks this stock outperforms
            percentile = int((rank / total_stocks) * 99)
            rs_rankings[symbol] = max(1, min(99, percentile))
        
        # Cache the result
        self._rs_cache[cache_key] = rs_rankings
        return rs_rankings
    
    def calculate_relative_strength(self, symbol, end_date):
        """
        Calculate IBD-style Relative Strength rating for a stock.
        
        This is a PERCENTILE RANKING (1-99) comparing the stock's weighted
        12-month performance against ALL other stocks in the database.
        
        - RS 99 = Top 1% performer (outperforms 99% of stocks)
        - RS 70 = Outperforms 70% of stocks (Minervini's minimum)
        - RS 50 = Average performer
        - RS 1 = Bottom 1% performer
        
        IBD-style weighting:
        - 40% weight on most recent quarter
        - 20% weight each on prior 3 quarters
        
        Args:
            symbol: Stock symbol
            end_date: Date to calculate RS for
            
        Returns:
            int: RS rating 1-99, or None if insufficient data
        """
        rs_rankings = self.calculate_rs_percentile_ranking(end_date)
        return rs_rankings.get(symbol)
    
    def calculate_avg_dollar_volume(self, symbol, end_date, days=50):
        """
        Calculate average daily dollar volume over specified days.
        Dollar volume = close price * volume
        
        Minervini typically looks for $20M+ daily dollar volume for institutional tradability.
        
        Args:
            symbol: Stock symbol
            end_date: Date string
            days: Number of days to average (default 50)
            
        Returns:
            int: Average dollar volume or None if insufficient data
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT close, volume FROM stock_prices
            WHERE symbol = %s AND date <= %s
            ORDER BY date DESC
            LIMIT %s
        """, (symbol, end_date, days))
        
        rows = cursor.fetchall()
        cursor.close()
        
        if len(rows) < days // 2:  # Need at least half the days
            return None
        
        dollar_volumes = [float(row[0]) * int(row[1]) for row in rows]
        return int(sum(dollar_volumes) / len(dollar_volumes))
    
    def calculate_volume_ratio(self, symbol, date, avg_days=50):
        """
        Calculate today's volume relative to average volume.
        
        A ratio > 1.5 indicates unusually high volume (potential breakout).
        A ratio < 0.5 indicates unusually low volume (consolidation).
        
        Args:
            symbol: Stock symbol
            date: Date to analyze
            avg_days: Days for average calculation
            
        Returns:
            float: Volume ratio or None if insufficient data
        """
        cursor = self.conn.cursor()
        
        # Get today's volume
        cursor.execute("""
            SELECT volume FROM stock_prices
            WHERE symbol = %s AND date = %s
        """, (symbol, date))
        
        result = cursor.fetchone()
        if not result or result[0] is None:
            cursor.close()
            return None
        
        today_volume = int(result[0])
        
        # Get average volume (excluding today)
        cursor.execute("""
            SELECT AVG(volume) FROM (
                SELECT volume FROM stock_prices
                WHERE symbol = %s AND date < %s
                ORDER BY date DESC
                LIMIT %s
            ) subq
        """, (symbol, date, avg_days))
        
        result = cursor.fetchone()
        cursor.close()
        
        if not result or result[0] is None or float(result[0]) == 0:
            return None
        
        avg_volume = float(result[0])
        return round(today_volume / avg_volume, 2)
    
    def calculate_returns(self, symbol, end_date):
        """
        Calculate multi-timeframe returns (1m, 3m, 6m, 12m).
        
        IBD-style momentum analysis uses weighted returns across timeframes.
        Strong stocks show positive returns across all timeframes.
        
        Args:
            symbol: Stock symbol
            end_date: Date to calculate returns from
            
        Returns:
            dict: Returns for each timeframe or None values if insufficient data
        """
        cursor = self.conn.cursor()
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Get current price
        cursor.execute("""
            SELECT close FROM stock_prices
            WHERE symbol = %s AND date = %s
        """, (symbol, end_date))
        
        result = cursor.fetchone()
        if not result:
            cursor.close()
            return {'return_1m': None, 'return_3m': None, 'return_6m': None, 'return_12m': None}
        
        current_price = float(result[0])
        
        returns = {}
        timeframes = {
            'return_1m': 21,    # ~1 month of trading days
            'return_3m': 63,    # ~3 months
            'return_6m': 126,   # ~6 months
            'return_12m': 252   # ~12 months
        }
        
        for key, days in timeframes.items():
            start_date = (end_date_obj - timedelta(days=int(days * 1.5))).strftime("%Y-%m-%d")
            
            cursor.execute("""
                SELECT close FROM stock_prices
                WHERE symbol = %s AND date >= %s AND date < %s
                ORDER BY date ASC
                LIMIT 1
            """, (symbol, start_date, end_date))
            
            result = cursor.fetchone()
            if result and result[0]:
                past_price = float(result[0])
                returns[key] = round(((current_price - past_price) / past_price) * 100, 2)
            else:
                returns[key] = None
        
        cursor.close()
        return returns
    
    def calculate_atr(self, symbol, end_date, period=14):
        """
        Calculate Average True Range (ATR) - a volatility indicator.
        
        True Range = max(high-low, abs(high-prev_close), abs(low-prev_close))
        ATR = Average of True Range over period
        
        Used for:
        - Position sizing (smaller positions for volatile stocks)
        - Stop-loss placement (typically 1.5-2x ATR below entry)
        
        Args:
            symbol: Stock symbol
            end_date: Date to calculate ATR for
            period: ATR period (default 14 days)
            
        Returns:
            tuple: (atr_value, atr_percent) or (None, None)
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT high, low, close FROM stock_prices
            WHERE symbol = %s AND date <= %s
            ORDER BY date DESC
            LIMIT %s
        """, (symbol, end_date, period + 1))
        
        rows = cursor.fetchall()
        cursor.close()
        
        if len(rows) < period + 1:
            return None, None
        
        # Rows are in descending order, reverse for calculation
        rows = list(reversed(rows))
        
        true_ranges = []
        for i in range(1, len(rows)):
            high = float(rows[i][0])
            low = float(rows[i][1])
            prev_close = float(rows[i-1][2])
            
            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close)
            )
            true_ranges.append(tr)
        
        if not true_ranges:
            return None, None
        
        atr = sum(true_ranges) / len(true_ranges)
        current_price = float(rows[-1][2])
        atr_percent = (atr / current_price) * 100
        
        return round(atr, 2), round(atr_percent, 2)
    
    def calculate_new_high_metrics(self, symbol, end_date):
        """
        Calculate metrics related to 52-week highs.
        
        Stocks making new highs tend to continue making new highs.
        Days since high helps identify stocks breaking out vs extended.
        
        Args:
            symbol: Stock symbol
            end_date: Date to analyze
            
        Returns:
            tuple: (is_52w_high, days_since_52w_high)
        """
        cursor = self.conn.cursor()
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        start_date = (end_date_obj - timedelta(weeks=52)).strftime("%Y-%m-%d")
        
        # Get today's high and 52-week high with its date
        cursor.execute("""
            SELECT date, high FROM stock_prices
            WHERE symbol = %s AND date BETWEEN %s AND %s
            ORDER BY high DESC
            LIMIT 1
        """, (symbol, start_date, end_date))
        
        high_result = cursor.fetchone()
        
        cursor.execute("""
            SELECT high FROM stock_prices
            WHERE symbol = %s AND date = %s
        """, (symbol, end_date))
        
        today_result = cursor.fetchone()
        cursor.close()
        
        if not high_result or not today_result:
            return False, None
        
        high_date = high_result[0]
        week_52_high = float(high_result[1])
        today_high = float(today_result[0])
        
        # Is today a new 52-week high? (within 0.5% tolerance)
        is_52w_high = today_high >= week_52_high * 0.995
        
        # Days since 52-week high
        if isinstance(high_date, str):
            high_date_obj = datetime.strptime(high_date, "%Y-%m-%d")
        else:
            high_date_obj = datetime.combine(high_date, datetime.min.time())
        
        days_since = (end_date_obj - high_date_obj).days
        
        return is_52w_high, days_since
    
    def find_recent_swing_low(self, symbol, end_date, lookback=30):
        """
        Find the most recent swing low in the price action.
        
        A swing low is a day whose low is lower than the 2 days
        before AND after it (local minimum). Used for pattern-based
        stop loss placement per Minervini's methodology.
        
        Args:
            symbol: Stock symbol
            end_date: Date to search from
            lookback: Number of days to look back (default 30)
            
        Returns:
            float: Most recent swing low price, or None
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT date, low FROM stock_prices
            WHERE symbol = %s AND date <= %s
            ORDER BY date DESC
            LIMIT %s
        """, (symbol, end_date, lookback))
        
        rows = list(reversed(cursor.fetchall()))  # Oldest first
        cursor.close()
        
        if len(rows) < 5:
            return None
        
        # Walk backward from recent to find the most recent swing low
        for i in range(len(rows) - 3, 1, -1):
            low_i = float(rows[i][1])
            if (low_i < float(rows[i-1][1]) and
                low_i < float(rows[i-2][1]) and
                low_i < float(rows[i+1][1]) and
                low_i < float(rows[i+2][1])):
                return low_i
        
        return None
    
    def calculate_sector_performance(self, date):
        """
        Calculate and store sector performance for all SIC codes.
        This creates the sector_performance table data for the given date.
        
        Should be called once per date before analyzing individual stocks.
        
        Args:
            date: Date to calculate sector performance for
        """
        cursor = self.conn.cursor()
        
        # Get market performance for comparison
        market_perf = self.get_market_performance(date)
        
        end_date_obj = datetime.strptime(date, "%Y-%m-%d")
        start_date = (end_date_obj - timedelta(days=90)).strftime("%Y-%m-%d")
        
        # Get all unique SIC codes with their descriptions
        cursor.execute("""
            SELECT DISTINCT td.sic_code, td.sic_description
            FROM ticker_details td
            WHERE td.sic_code IS NOT NULL AND td.sic_code != ''
        """)
        
        sic_codes = cursor.fetchall()
        
        for sic_code, sic_description in sic_codes:
            # Calculate average 90-day return for stocks in this sector
            # Fixed: Use DISTINCT ON to avoid counting each symbol multiple times
            cursor.execute("""
                WITH sector_stocks AS (
                    SELECT DISTINCT ON (sp.symbol) 
                           sp.symbol,
                           FIRST_VALUE(sp.close) OVER (PARTITION BY sp.symbol ORDER BY sp.date ASC) as start_price,
                           LAST_VALUE(sp.close) OVER (PARTITION BY sp.symbol ORDER BY sp.date ASC 
                               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as end_price
                    FROM stock_prices sp
                    JOIN ticker_details td ON sp.symbol = td.symbol
                    WHERE td.sic_code = %s
                    AND sp.date BETWEEN %s AND %s
                ),
                sector_returns AS (
                    SELECT symbol,
                           ((end_price - start_price) / NULLIF(start_price, 0) * 100) as return_pct
                    FROM sector_stocks
                    WHERE start_price > 0
                )
                SELECT AVG(return_pct) as avg_return,
                       COUNT(*) as stock_count
                FROM sector_returns
            """, (sic_code, start_date, date))
            
            result = cursor.fetchone()
            
            if result and result[0] is not None:
                sector_return = float(result[0])
                stock_count = result[1]
                
                # Calculate sector RS vs market (0-100 scale, 50 = market average)
                if sector_return > market_perf:
                    sector_rs = 50 + min(50, (sector_return - market_perf) * 2)
                else:
                    sector_rs = 50 - min(50, (market_perf - sector_return) * 2)
                
                sector_rs = max(0, min(100, sector_rs))
                
                # Upsert sector performance
                cursor.execute("""
                    INSERT INTO sector_performance (date, sic_code, sic_description, sector_return_90d, sector_rs, stock_count)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date, sic_code) DO UPDATE SET
                        sic_description = EXCLUDED.sic_description,
                        sector_return_90d = EXCLUDED.sector_return_90d,
                        sector_rs = EXCLUDED.sector_rs,
                        stock_count = EXCLUDED.stock_count
                """, (date, sic_code, sic_description, round(sector_return, 2), round(sector_rs, 2), stock_count))
        
        self.conn.commit()
        cursor.close()
    
    def get_industry_rs(self, symbol, date):
        """
        Get the industry/sector relative strength for a stock.
        
        Args:
            symbol: Stock symbol
            date: Date to get sector RS for
            
        Returns:
            float: Sector RS score (0-100) or None
        """
        cursor = self.conn.cursor()
        
        # Get the stock's SIC code
        cursor.execute("""
            SELECT td.sic_code FROM ticker_details td
            WHERE td.symbol = %s
        """, (symbol,))
        
        result = cursor.fetchone()
        if not result or not result[0]:
            cursor.close()
            return None
        
        sic_code = result[0]
        
        # Get sector RS
        cursor.execute("""
            SELECT sector_rs FROM sector_performance
            WHERE date = %s AND sic_code = %s
        """, (date, sic_code))
        
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            return float(result[0])
        return None
    
    def get_earnings_growth(self, symbol):
        """
        Calculate EPS and revenue growth rates from income statements.
        
        Minervini requires:
        - 25%+ quarterly EPS growth (Year-over-Year comparison)
        - Consistent YoY growth
        - Earnings acceleration (YoY growth rate improving each quarter)
        
        Args:
            symbol: Stock symbol
            
        Returns:
            dict: Earnings growth metrics or None
        """
        cursor = self.conn.cursor()
        
        # Get last 8 quarters of data for YoY acceleration calculations
        # We need 8 quarters to calculate YoY growth for 4 consecutive quarters
        cursor.execute("""
            SELECT 
                period_end,
                fiscal_year,
                fiscal_quarter,
                basic_earnings_per_share,
                revenue
            FROM income_statements
            WHERE ticker = %s
            AND timeframe = 'quarterly'
            AND basic_earnings_per_share IS NOT NULL
            ORDER BY period_end DESC
            LIMIT 8
        """, (symbol,))
        
        quarters = cursor.fetchall()
        cursor.close()
        
        if len(quarters) < 2:
            return None
        
        # Latest quarter
        latest = quarters[0]
        latest_eps = float(latest[3]) if latest[3] else None
        latest_revenue = float(latest[4]) if latest[4] else None
        
        # Quarter-over-quarter (QoQ) growth (sequential)
        qoq_eps_growth = None
        if len(quarters) >= 2 and latest_eps and quarters[1][3]:
            prev_quarter_eps = float(quarters[1][3])
            if prev_quarter_eps > 0:
                qoq_eps_growth = ((latest_eps - prev_quarter_eps) / prev_quarter_eps) * 100
        
        # Year-over-year (YoY) growth for latest quarter
        yoy_eps_growth = None
        yoy_revenue_growth = None
        if len(quarters) >= 5:
            year_ago = quarters[4]  # 4 quarters back = same quarter last year
            year_ago_eps = float(year_ago[3]) if year_ago[3] else None
            year_ago_revenue = float(year_ago[4]) if year_ago[4] else None
            
            if latest_eps and year_ago_eps and year_ago_eps > 0:
                yoy_eps_growth = ((latest_eps - year_ago_eps) / year_ago_eps) * 100
            
            if latest_revenue and year_ago_revenue and year_ago_revenue > 0:
                yoy_revenue_growth = ((latest_revenue - year_ago_revenue) / year_ago_revenue) * 100
        
        # Check for earnings acceleration (YoY growth rate improving each quarter)
        # This is the TRUE Minervini acceleration check:
        # - Q0 YoY growth > Q1 YoY growth > Q2 YoY growth
        # (Each quarter's YoY growth is higher than the previous quarter's YoY growth)
        acceleration = False
        yoy_growth_rates = []
        
        if len(quarters) >= 8:
            # Calculate YoY growth for each of the last 4 quarters
            # quarters[0] vs quarters[4] = most recent YoY
            # quarters[1] vs quarters[5] = previous quarter's YoY
            # quarters[2] vs quarters[6] = 2 quarters ago YoY
            # quarters[3] vs quarters[7] = 3 quarters ago YoY
            for i in range(4):
                current_q = quarters[i]
                year_ago_q = quarters[i + 4]
                
                current_eps = float(current_q[3]) if current_q[3] else None
                year_ago_eps = float(year_ago_q[3]) if year_ago_q[3] else None
                
                if current_eps and year_ago_eps and year_ago_eps > 0:
                    yoy_growth = ((current_eps - year_ago_eps) / year_ago_eps) * 100
                    yoy_growth_rates.append(yoy_growth)
            
            # Acceleration = YoY growth rate is improving each quarter
            # yoy_growth_rates[0] is most recent, should be >= yoy_growth_rates[1], etc.
            if len(yoy_growth_rates) >= 3:
                acceleration = all(
                    yoy_growth_rates[i] >= yoy_growth_rates[i+1] 
                    for i in range(len(yoy_growth_rates)-1)
                )
        
        return {
            'latest_eps': latest_eps,
            'latest_revenue': latest_revenue,
            'qoq_eps_growth': qoq_eps_growth,
            'yoy_eps_growth': yoy_eps_growth,
            'yoy_revenue_growth': yoy_revenue_growth,
            'earnings_acceleration': acceleration,
            'quarters_available': len(quarters),
            'yoy_growth_rates': yoy_growth_rates if yoy_growth_rates else None  # For debugging
        }
    
    def get_earnings_surprises(self, symbol, num_quarters=4):
        """
        Get recent earnings surprise history (beat/miss patterns).
        
        Minervini prefers stocks that consistently beat estimates.
        
        Args:
            symbol: Stock symbol
            num_quarters: Number of recent quarters to check
            
        Returns:
            dict: Surprise statistics or None
        """
        cursor = self.conn.cursor()
        
        # Get last N earnings with surprises
        cursor.execute("""
            SELECT 
                date,
                actual_eps,
                estimated_eps,
                eps_surprise_percent,
                actual_revenue,
                estimated_revenue,
                revenue_surprise_percent
            FROM earnings
            WHERE ticker = %s
            AND actual_eps IS NOT NULL
            AND estimated_eps IS NOT NULL
            ORDER BY date DESC
            LIMIT %s
        """, (symbol, num_quarters))
        
        results = cursor.fetchall()
        cursor.close()
        
        if not results:
            return None
        
        eps_surprises = []
        revenue_surprises = []
        beats = 0
        misses = 0
        
        for row in results:
            if row[3] is not None:  # eps_surprise_percent
                eps_surprises.append(float(row[3]))
                if float(row[3]) > 0:
                    beats += 1
                else:
                    misses += 1
            
            if row[6] is not None:  # revenue_surprise_percent
                revenue_surprises.append(float(row[6]))
        
        avg_eps_surprise = sum(eps_surprises) / len(eps_surprises) if eps_surprises else None
        avg_revenue_surprise = sum(revenue_surprises) / len(revenue_surprises) if revenue_surprises else None
        
        # Consistency: All beats = very strong
        all_beats = beats == len(results) and misses == 0
        mostly_beats = beats >= len(results) * 0.75 if len(results) > 0 else False
        
        return {
            'total_earnings': len(results),
            'beats': beats,
            'misses': misses,
            'avg_eps_surprise': avg_eps_surprise,
            'avg_revenue_surprise': avg_revenue_surprise,
            'all_beats': all_beats,
            'mostly_beats': mostly_beats
        }
    
    def check_upcoming_earnings(self, symbol, analysis_date, days_ahead=14):
        """
        Check if earnings announcement is coming soon.
        
        Minervini typically avoids buying right before earnings (2 weeks).
        
        Args:
            symbol: Stock symbol
            analysis_date: Current date (YYYY-MM-DD string)
            days_ahead: Days to look ahead (default 14)
            
        Returns:
            dict: Upcoming earnings info or None
        """
        cursor = self.conn.cursor()
        
        end_date_obj = datetime.strptime(analysis_date, "%Y-%m-%d")
        future_date = (end_date_obj + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
        
        cursor.execute("""
            SELECT 
                date,
                time,
                estimated_eps,
                date_status,
                importance
            FROM earnings
            WHERE ticker = %s
            AND date > %s
            AND date <= %s
            ORDER BY date ASC
            LIMIT 1
        """, (symbol, analysis_date, future_date))
        
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            earnings_date = result[0]
            days_until = (earnings_date - end_date_obj.date()).days if isinstance(earnings_date, date) else None
            
            return {
                'has_upcoming_earnings': True,
                'earnings_date': earnings_date,
                'days_until': days_until,
                'estimated_eps': float(result[2]) if result[2] else None,
                'date_status': result[3],
                'importance': result[4]
            }
        
        return {
            'has_upcoming_earnings': False,
            'earnings_date': None,
            'days_until': None
        }
    
    def evaluate_earnings_quality(self, symbol, date):
        """
        Evaluate overall earnings quality based on Minervini criteria.
        
        Minervini's Earnings Requirements (SEPA):
        - 25%+ quarterly EPS growth
        - Consistent YoY growth
        - Earnings acceleration preferred
        - Consistent estimate beats
        - No upcoming earnings (within 2 weeks)
        
        Returns:
            dict: Earnings quality metrics and pass/fail
        """
        # Get earnings growth
        growth = self.get_earnings_growth(symbol)
        
        # Get earnings surprises
        surprises = self.get_earnings_surprises(symbol)
        
        # Check upcoming earnings
        upcoming = self.check_upcoming_earnings(symbol, date)
        
        # Evaluate against Minervini criteria
        passes_earnings = True
        earnings_score = 0
        max_score = 100
        issues = []
        
        if not growth or not growth.get('latest_eps'):
            # No earnings data = can't evaluate
            return {
                'has_earnings_data': False,
                'passes_earnings_criteria': None,
                'earnings_score': 0,
                'issues': ['No earnings data available'],
                'growth': None,
                'surprises': None,
                'upcoming': upcoming
            }
        
        # Check YoY EPS growth (Minervini wants 25%+)
        if growth.get('yoy_eps_growth') is not None:
            yoy_growth = growth['yoy_eps_growth']
            if yoy_growth >= 25:
                earnings_score += 30  # Strong growth
            elif yoy_growth >= 15:
                earnings_score += 20  # Good growth
            elif yoy_growth >= 5:
                earnings_score += 10  # Modest growth
            else:
                passes_earnings = False
                issues.append(f"Low YoY EPS growth ({yoy_growth:.1f}% < 25%)")
        else:
            earnings_score += 10  # Give some credit if we have recent data
        
        # Check QoQ growth (acceleration signal)
        if growth.get('qoq_eps_growth') is not None:
            qoq_growth = growth['qoq_eps_growth']
            if qoq_growth >= 20:
                earnings_score += 20
            elif qoq_growth >= 10:
                earnings_score += 15
            elif qoq_growth < 0:
                passes_earnings = False
                issues.append(f"Negative QoQ EPS growth ({qoq_growth:.1f}%)")
        
        # Check earnings acceleration
        if growth.get('earnings_acceleration'):
            earnings_score += 20
        
        # Check surprise history
        if surprises:
            if surprises.get('all_beats'):
                earnings_score += 20  # Perfect beat record
            elif surprises.get('mostly_beats'):
                earnings_score += 15  # Good beat record
            elif surprises['beats'] < surprises['misses']:
                passes_earnings = False
                issues.append(f"More misses than beats ({surprises['misses']} misses)")
        
        # Check revenue growth
        if growth.get('yoy_revenue_growth') is not None:
            if growth['yoy_revenue_growth'] >= 15:
                earnings_score += 10
            elif growth['yoy_revenue_growth'] < 0:
                issues.append(f"Negative revenue growth ({growth['yoy_revenue_growth']:.1f}%)")
        
        # Upcoming earnings warning (not a disqualifier, but a caution)
        if upcoming.get('has_upcoming_earnings'):
            issues.append(f"⚠️ Earnings in {upcoming['days_until']} days - consider waiting")
        
        return {
            'has_earnings_data': True,
            'passes_earnings_criteria': passes_earnings,
            'earnings_score': min(earnings_score, max_score),
            'issues': issues,
            'growth': growth,
            'surprises': surprises,
            'upcoming': upcoming
        }

    def detect_vcp(self, symbol, end_date, lookback_days=120):
        """
        Detect Volatility Contraction Pattern (VCP).
        
        VCP characteristics:
        - Series of price contractions (usually 2-4)
        - Each contraction is tighter than the previous
        - Volume tends to dry up during contractions
        - Forms higher lows
        - Price consolidates near highs
        
        Args:
            symbol: Stock symbol
            end_date: Date to analyze
            lookback_days: How far back to look for the pattern (default 120 days)
            
        Returns:
            dict with VCP metrics:
            - vcp_detected: True if valid VCP found
            - vcp_score: Quality score 0-100
            - contraction_count: Number of contractions found
            - latest_contraction_pct: Tightness of most recent contraction
            - volume_contraction: True if volume is contracting
            - pivot_price: Potential breakout price
        """
        cursor = self.conn.cursor()
        
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        start_date = (end_date_obj - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        
        # Get price and volume data
        cursor.execute("""
            SELECT date, high, low, close, volume FROM stock_prices
            WHERE symbol = %s AND date BETWEEN %s AND %s
            ORDER BY date ASC
        """, (symbol, start_date, end_date))
        
        rows = cursor.fetchall()
        cursor.close()
        
        if len(rows) < 30:  # Need minimum data
            return {
                'vcp_detected': False,
                'vcp_score': 0,
                'contraction_count': 0,
                'latest_contraction_pct': None,
                'volume_contraction': False,
                'pivot_price': None,
                'last_contraction_low': None
            }
        
        # Convert to lists
        dates = [row[0] for row in rows]
        highs = [float(row[1]) for row in rows]
        lows = [float(row[2]) for row in rows]
        closes = [float(row[3]) for row in rows]
        volumes = [int(row[4]) for row in rows]
        
        # Find the highest high in the lookback period (potential starting point)
        max_high = max(highs)
        max_high_idx = highs.index(max_high)
        
        # Only analyze if max high is in first 60% of period (needs time to contract)
        if max_high_idx > len(highs) * 0.6:
            return {
                'vcp_detected': False,
                'vcp_score': 0,
                'contraction_count': 0,
                'latest_contraction_pct': None,
                'volume_contraction': False,
                'pivot_price': None,
                'last_contraction_low': None
            }
        
        # Analyze contractions by dividing period after max high into segments
        remaining_data = len(highs) - max_high_idx
        if remaining_data < 20:
            return {
                'vcp_detected': False,
                'vcp_score': 0,
                'contraction_count': 0,
                'latest_contraction_pct': None,
                'volume_contraction': False,
                'pivot_price': None,
                'last_contraction_low': None
            }
        
        # Divide into segments to find contractions
        segment_size = remaining_data // 4  # Look for up to 4 contractions
        if segment_size < 5:
            segment_size = 5
        
        contractions = []
        contraction_lows = []
        
        for i in range(4):
            start_idx = max_high_idx + (i * segment_size)
            end_idx = min(start_idx + segment_size, len(highs))
            
            if start_idx >= len(highs):
                break
            
            segment_highs = highs[start_idx:end_idx]
            segment_lows = lows[start_idx:end_idx]
            
            if segment_highs and segment_lows:
                seg_high = max(segment_highs)
                seg_low = min(segment_lows)
                contraction_pct = ((seg_high - seg_low) / seg_high) * 100
                contractions.append(contraction_pct)
                contraction_lows.append(seg_low)
        
        if len(contractions) < 2:
            return {
                'vcp_detected': False,
                'vcp_score': 0,
                'contraction_count': 0,
                'latest_contraction_pct': None,
                'volume_contraction': False,
                'pivot_price': None,
                'last_contraction_low': None
            }
        
        # Check for volatility contraction (each range smaller than previous)
        contraction_count = 1
        for i in range(1, len(contractions)):
            if contractions[i] < contractions[i-1] * 1.1:  # Allow 10% tolerance
                contraction_count += 1
        
        # Check for higher lows (bullish sign)
        higher_lows = True
        for i in range(1, len(contraction_lows)):
            if contraction_lows[i] < contraction_lows[i-1] * 0.98:  # Allow 2% tolerance
                higher_lows = False
                break
        
        # Analyze volume contraction
        early_volume = volumes[max_high_idx:max_high_idx + segment_size]
        late_volume = volumes[-segment_size:]
        
        avg_early_volume = sum(early_volume) / len(early_volume) if early_volume else 0
        avg_late_volume = sum(late_volume) / len(late_volume) if late_volume else 0
        
        volume_contraction = avg_late_volume < avg_early_volume * 0.7  # Volume dried up 30%+
        
        # Calculate latest contraction percentage
        latest_contraction_pct = contractions[-1] if contractions else None
        
        # Determine pivot price (recent high that acts as resistance)
        recent_highs = highs[-20:] if len(highs) >= 20 else highs
        pivot_price = max(recent_highs)
        
        # Calculate VCP score (0-100)
        vcp_score = 0
        
        # Score based on number of contractions (max 25 points)
        vcp_score += min(contraction_count * 8, 25)
        
        # Score based on tightness of latest contraction (max 25 points)
        if latest_contraction_pct:
            if latest_contraction_pct <= 5:
                vcp_score += 25
            elif latest_contraction_pct <= 10:
                vcp_score += 20
            elif latest_contraction_pct <= 15:
                vcp_score += 15
            elif latest_contraction_pct <= 20:
                vcp_score += 10
            else:
                vcp_score += 5
        
        # Score for higher lows (20 points)
        if higher_lows:
            vcp_score += 20
        
        # Score for volume contraction (20 points)
        if volume_contraction:
            vcp_score += 20
        
        # Score for price near pivot (10 points)
        current_price = closes[-1]
        distance_from_pivot = ((pivot_price - current_price) / pivot_price) * 100
        if distance_from_pivot <= 3:
            vcp_score += 10
        elif distance_from_pivot <= 5:
            vcp_score += 7
        elif distance_from_pivot <= 10:
            vcp_score += 4
        
        # Determine if VCP is detected (score >= 50 and at least 2 contractions)
        vcp_detected = vcp_score >= 50 and contraction_count >= 2
        
        # Track the low of the last (tightest) contraction for stop loss placement
        last_contraction_low = contraction_lows[-1] if contraction_lows else None
        
        return {
            'vcp_detected': vcp_detected,
            'vcp_score': vcp_score,
            'contraction_count': contraction_count,
            'latest_contraction_pct': latest_contraction_pct,
            'volume_contraction': volume_contraction,
            'pivot_price': pivot_price,
            'last_contraction_low': last_contraction_low
        }
    
    def determine_stage(self, close_price, ma_50, ma_150, ma_200, ma_200_trend, 
                       percent_from_52w_high, relative_strength):
        """
        Determine the market stage (1-4) based on Minervini's stage analysis:
        
        Stage 1 - Basing/Accumulation: Consolidating after decline, preparing for advance
        Stage 2 - Advancing/Markup: Strong uptrend (ideal buy zone)
        Stage 3 - Topping/Distribution: Peaked and losing momentum
        Stage 4 - Declining/Markdown: Downtrend
        
        Args:
            close_price: Current closing price
            ma_50, ma_150, ma_200: Moving averages
            ma_200_trend: Trend of 200-day MA
            percent_from_52w_high: Distance from 52-week high
            relative_strength: RS score (0-100)
            
        Returns:
            int: Stage number (1, 2, 3, or 4)
        """
        
        # Check MA alignment
        price_above_200 = close_price > ma_200
        price_above_150 = close_price > ma_150
        price_above_50 = close_price > ma_50
        ma_50_above_150 = ma_50 > ma_150
        ma_150_above_200 = ma_150 > ma_200
        
        # Stage 2: Strong uptrend (Advancing)
        # Price above all MAs, MAs in correct order, strong momentum
        if (price_above_200 and price_above_150 and price_above_50 and
            ma_50_above_150 and ma_150_above_200 and
            ma_200_trend is not None and ma_200_trend > 0 and
            percent_from_52w_high >= -25 and
            relative_strength is not None and relative_strength >= 60):
            return 2
        
        # Stage 4: Downtrend (Declining)
        # Price below 200 MA, MAs in wrong order, weak RS
        if (not price_above_200 and
            ma_200_trend is not None and ma_200_trend < -0.5 and
            percent_from_52w_high < -30 and
            relative_strength is not None and relative_strength < 40):
            return 4
        
        # Stage 3: Topping/Distribution
        # Price still elevated but losing momentum, MAs flattening
        if (price_above_200 and
            (not ma_150_above_200 or not ma_50_above_150 or
             (ma_200_trend is not None and ma_200_trend < 0.5) or
             percent_from_52w_high < -15 or
             (relative_strength is not None and relative_strength < 60))):
            return 3
        
        # Stage 1: Basing/Accumulation (default for everything else)
        # Consolidating, MAs converging, preparing for next move
        return 1
    
    def calculate_entry_range(self, close_price, pivot_price, ema_10, ema_21):
        """
        Determine suggested entry price range based on Minervini principles.
        
        Two entry strategies:
        A) Pivot breakout: Buy as price clears pivot on volume
           Range: [pivot, pivot * 1.05] -- max 5% chase above pivot
        B) Pullback entry: Buy on pullback to 10-day or 21-day EMA
           (when already extended above pivot)
        
        Args:
            close_price: Current closing price
            pivot_price: VCP pivot / breakout price
            ema_10: 10-day Exponential Moving Average
            ema_21: 21-day Exponential Moving Average
            
        Returns:
            tuple: (entry_low, entry_high) price range
        """
        if not pivot_price:
            return None, None
        
        distance_from_pivot = ((close_price - pivot_price) / pivot_price) * 100
        
        if distance_from_pivot > 5:
            # Already extended above pivot -- suggest pullback entry to EMAs
            if ema_10 and ema_21:
                entry_low = min(ema_10, ema_21)
                entry_high = max(ema_10, ema_21)
            elif ema_21:
                entry_low = ema_21 * 0.99
                entry_high = ema_21 * 1.01
            elif ema_10:
                entry_low = ema_10 * 0.99
                entry_high = ema_10 * 1.01
            else:
                entry_low = pivot_price
                entry_high = pivot_price * 1.05
        else:
            # At or near pivot -- breakout entry
            entry_low = pivot_price
            entry_high = pivot_price * 1.05
        
        return round(entry_low, 2), round(entry_high, 2)
    
    def calculate_stop_loss(self, entry_price, atr_14, ema_21, last_contraction_low, swing_low):
        """
        Calculate stop loss using the tightest reasonable level from multiple methods.
        
        Minervini Stop Loss Methods:
        1. Maximum loss rule: 7-8% below entry (absolute hard floor)
        2. ATR-based: Entry - 2x ATR (volatility-adjusted)
        3. 21-EMA based: Just below 21-day EMA
        4. VCP contraction low: Below last contraction's low
        5. Swing low: Below most recent swing low
        
        Uses the HIGHEST (tightest) stop, clamped between 3% and 8%.
        
        Args:
            entry_price: Expected entry price
            atr_14: 14-day Average True Range
            ema_21: 21-day EMA
            last_contraction_low: Low of VCP's last contraction
            swing_low: Most recent swing low
            
        Returns:
            float: Suggested stop loss price
        """
        if not entry_price:
            return None
        
        # Method 1: Maximum loss rule (hard floor -- Minervini's absolute max)
        max_loss_stop = entry_price * 0.92  # 8% max loss
        
        stops = [max_loss_stop]
        
        # Method 2: ATR-based (2x ATR below entry)
        if atr_14:
            atr_stop = entry_price - (2.0 * atr_14)
            if atr_stop > 0:
                stops.append(atr_stop)
        
        # Method 3: Below 21-day EMA
        if ema_21 and ema_21 < entry_price:
            ema_stop = ema_21 * 0.99  # 1% below 21-EMA
            stops.append(ema_stop)
        
        # Method 4: VCP contraction low
        if last_contraction_low and last_contraction_low < entry_price:
            stops.append(last_contraction_low * 0.99)  # 1% below contraction low
        
        # Method 5: Swing low
        if swing_low and swing_low < entry_price:
            stops.append(swing_low * 0.99)  # 1% below swing low
        
        # Use the HIGHEST stop (tightest risk)
        stop = max(stops)
        
        # Clamp: don't tighter than 3% (avoid whipsaw from noise)
        min_stop = entry_price * 0.97
        if stop > min_stop:
            stop = min_stop
        
        # Clamp: never more than 8% loss (Minervini's absolute max)
        if stop < max_loss_stop:
            stop = max_loss_stop
        
        return round(stop, 2)
    
    def calculate_sell_targets(self, entry_price, stop_loss, ma_200):
        """
        Calculate sell/profit targets using Minervini's selling rules.
        
        Minervini Profit-Taking Rules:
        1. Risk-based: Target = Entry + N * Risk (R-multiples)
           - 2:1 minimum acceptable, 3:1 standard target
        2. Percentage: Take partial profits at +20-25%
        3. Climax warning: Price 70%+ above 200-day MA = extremely extended
        
        Args:
            entry_price: Expected entry price
            stop_loss: Calculated stop loss price
            ma_200: 200-day moving average
            
        Returns:
            dict: Sell target prices and risk/reward info, or None
        """
        if not entry_price or not stop_loss:
            return None
        
        risk = entry_price - stop_loss  # Dollar risk per share
        
        if risk <= 0:
            return None
        
        # R-multiple targets
        target_2r = entry_price + (2.0 * risk)  # 2:1 reward/risk (minimum)
        target_3r = entry_price + (3.0 * risk)  # 3:1 (standard Minervini)
        
        # Percentage-based targets (Minervini's 20-25% rule)
        target_20pct = entry_price * 1.20  # Partial profit at +20%
        target_25pct = entry_price * 1.25  # More at +25%
        
        # Primary target: lower of 3R and 25% (conservative approach)
        primary_target = min(target_3r, target_25pct)
        
        # Climax/overextension price (warning level)
        climax_price = ma_200 * 1.70 if ma_200 else None
        
        # Risk/reward ratio for primary target
        risk_reward = round((primary_target - entry_price) / risk, 1) if risk > 0 else None
        
        # Risk percentage
        risk_pct = round((risk / entry_price) * 100, 1)
        
        return {
            'target_conservative': round(target_2r, 2),
            'target_primary': round(primary_target, 2),
            'target_aggressive': round(target_25pct, 2),
            'partial_profit_at': round(target_20pct, 2),
            'climax_warning_price': round(climax_price, 2) if climax_price else None,
            'risk_reward_ratio': risk_reward,
            'risk_percent': risk_pct
        }
    
    def generate_signal(self, metrics):
        """
        Generate Buy/Wait/Pass signal with entry range, stop loss, and sell targets
        based on Mark Minervini's SEPA methodology.
        
        BUY:  All conditions favorable for immediate entry.
              Price at/near pivot, VCP confirmed, strong RS/earnings, no imminent earnings.
        WAIT: Setup forming but not yet actionable.
              Monitor for breakout, pullback entry, or earnings resolution.
        PASS: Avoid -- unfavorable technical or fundamental conditions.
        
        Args:
            metrics: Dict of all calculated metrics for a stock
            
        Returns:
            dict: Signal, reasons, entry range, stop loss, sell targets
        """
        close = metrics['close_price']
        pivot = metrics.get('pivot_price')
        stage = metrics['stage']
        rs = metrics.get('relative_strength')
        vcp = metrics.get('vcp_detected', False)
        vcp_score = metrics.get('vcp_score', 0) or 0
        vol_ratio = metrics.get('volume_ratio')
        passes = metrics['passes_minervini']
        criteria_count = metrics['criteria_passed']
        upcoming_earnings = metrics.get('has_upcoming_earnings', False)
        days_until = metrics.get('days_until_earnings')
        passes_earnings = metrics.get('passes_earnings')
        industry_rs = metrics.get('industry_rs')
        ema_10 = metrics.get('ema_10')
        ema_21 = metrics.get('ema_21')
        atr_14 = metrics.get('atr_14')
        ma_200 = metrics.get('ma_200')
        last_contraction_low = metrics.get('last_contraction_low')
        swing_low = metrics.get('swing_low')
        
        signal = 'PASS'
        reasons = []
        
        # Calculate distance from pivot
        distance_from_pivot = None
        if pivot and pivot > 0:
            distance_from_pivot = ((close - pivot) / pivot) * 100
        
        # === BUY CONDITIONS ===
        # All technical criteria pass, proper setup confirmed, actionable entry
        is_buy = (
            passes and
            stage == 2 and
            vcp and vcp_score >= 50 and
            rs is not None and rs >= 80 and
            distance_from_pivot is not None and -2 <= distance_from_pivot <= 5 and
            not (upcoming_earnings and days_until is not None and days_until <= 14) and
            passes_earnings is not False
        )
        
        if is_buy:
            signal = 'BUY'
            reasons.append('All 9 trend criteria pass')
            reasons.append('Stage 2 uptrend confirmed')
            reasons.append(f'VCP pattern (score {vcp_score:.0f})')
            if vol_ratio and vol_ratio >= 1.5:
                reasons.append(f'Above-avg volume ({vol_ratio:.1f}x)')
            if metrics.get('earnings_acceleration'):
                reasons.append('Earnings accelerating')
            if industry_rs and industry_rs >= 60:
                reasons.append(f'Strong sector (RS {industry_rs:.0f})')
        
        # === WAIT CONDITIONS ===
        # Setup forming or conditions almost met -- monitor for entry
        elif stage in (1, 2) and criteria_count >= 7:
            signal = 'WAIT'
            
            if not passes:
                failed = metrics.get('criteria_failed', '')
                reasons.append(f'{criteria_count}/9 criteria met ({failed})')
            
            if vcp_score and 30 <= vcp_score < 50:
                reasons.append(f'VCP forming (score {vcp_score:.0f})')
            elif not vcp:
                reasons.append('No VCP pattern yet')
            
            if distance_from_pivot is not None:
                if distance_from_pivot < -5:
                    reasons.append(f'Price {abs(distance_from_pivot):.1f}% below pivot -- wait for breakout')
                elif distance_from_pivot > 5:
                    reasons.append(f'Extended {distance_from_pivot:.1f}% above pivot -- wait for pullback')
            
            if upcoming_earnings and days_until is not None and days_until <= 14:
                reasons.append(f'Earnings in {days_until} days -- wait')
            
            if rs is not None and 70 <= rs < 80:
                reasons.append(f'RS adequate ({rs:.0f}) but below preferred 80+')
            
            if passes_earnings is False:
                reasons.append('Earnings criteria not met -- watch for improvement')
            
            if not reasons:
                reasons.append('Setup forming -- monitor for confirmation')
        
        # === PASS CONDITIONS ===
        else:
            if stage in (3, 4):
                reasons.append(f'Stage {stage} ({"topping/distribution" if stage == 3 else "declining"})')
            if criteria_count < 7:
                reasons.append(f'Only {criteria_count}/9 trend criteria met')
            if rs is not None and rs < 70:
                reasons.append(f'Weak relative strength ({rs:.0f})')
            if passes_earnings is False:
                reasons.append('Fails earnings criteria')
            if not reasons:
                reasons.append('Does not meet Minervini setup requirements')
        
        # === CALCULATE PRICE LEVELS for BUY and WAIT ===
        entry_low = entry_high = stop_loss = None
        sell_targets = None
        
        if signal in ('BUY', 'WAIT') and pivot:
            entry_low, entry_high = self.calculate_entry_range(
                close, pivot, ema_10, ema_21
            )
            
            # Use entry_low as reference for stop/target calculations
            reference_entry = entry_low if entry_low else pivot
            
            stop_loss = self.calculate_stop_loss(
                reference_entry, atr_14, ema_21,
                last_contraction_low, swing_low
            )
            
            if stop_loss:
                sell_targets = self.calculate_sell_targets(
                    reference_entry, stop_loss, ma_200
                )
        
        return {
            'signal': signal,
            'reasons': reasons,
            'entry_low': entry_low,
            'entry_high': entry_high,
            'stop_loss': stop_loss,
            'sell_targets': sell_targets,
        }
    
    def evaluate_minervini_criteria(self, symbol, date):
        """
        Evaluate a stock against Minervini's trend template.
        Returns metrics and pass/fail status, enhanced with ticker fundamentals.
        """
        cursor = self.conn.cursor()
        
        # Get ticker details for fundamental context
        ticker_details = self.get_ticker_details(symbol)
        
        # Apply Minervini-style filters based on fundamentals
        if ticker_details:
            # Filter 1: Inactive stocks
            if ticker_details['active'] is False:
                return None  # Skip inactive stocks
            
            # Filter 2: Extremely small market caps (< $100M)
            # Minervini avoids these due to liquidity concerns
            if ticker_details['market_cap'] and ticker_details['market_cap'] < 100_000_000:
                return None  # Too small, prone to manipulation
        
        # Get current price
        cursor.execute("""
            SELECT close FROM stock_prices
            WHERE symbol = %s AND date = %s
        """, (symbol, date))
        
        result = cursor.fetchone()
        cursor.close()
        
        if not result:
            return None
        
        close_price = float(result[0])
        
        # Calculate all metrics
        ma_50 = self.calculate_moving_average(symbol, date, 50)
        ma_150 = self.calculate_moving_average(symbol, date, 150)
        ma_200 = self.calculate_moving_average(symbol, date, 200)
        week_52_high, week_52_low = self.get_52_week_high_low(symbol, date)
        ma_200_trend = self.calculate_ma_200_trend(symbol, date)
        ma_150_trend = self.calculate_ma_150_trend(symbol, date)  # NEW: 150-day MA trend
        relative_strength = self.calculate_relative_strength(symbol, date)
        
        # Detect VCP pattern
        vcp_data = self.detect_vcp(symbol, date)
        
        # Calculate enhanced metrics
        avg_dollar_volume = self.calculate_avg_dollar_volume(symbol, date)
        volume_ratio = self.calculate_volume_ratio(symbol, date)
        returns = self.calculate_returns(symbol, date)
        atr_14, atr_percent = self.calculate_atr(symbol, date)
        is_52w_high, days_since_52w_high = self.calculate_new_high_metrics(symbol, date)
        industry_rs = self.get_industry_rs(symbol, date)
        
        # Calculate short-term EMAs for entry/stop calculations
        ema_10 = self.calculate_ema(symbol, date, 10)
        ema_21 = self.calculate_ema(symbol, date, 21)
        
        # Find recent swing low for pattern-based stop loss
        swing_low = self.find_recent_swing_low(symbol, date)
        
        # Evaluate earnings quality (fundamental analysis)
        earnings_quality = self.evaluate_earnings_quality(symbol, date)
        
        # Check if we have enough data
        if None in [ma_50, ma_150, ma_200, week_52_high, week_52_low]:
            return None
        
        # Calculate percent from 52-week high and low
        percent_from_52w_high = ((close_price - week_52_high) / week_52_high) * 100
        percent_from_52w_low = ((close_price - week_52_low) / week_52_low) * 100  # NEW
        
        # Evaluate Minervini's Trend Template Criteria + RS filter
        # Reference: "Trade Like a Stock Market Wizard" by Mark Minervini
        #
        # Book criteria mapped to checks (some book criteria expand to 2 checks):
        # Book #1: Price above both 150-day and 200-day MA → checks 1, 2
        # Book #2: 150-day MA above 200-day MA → check 3
        # Book #3: 200-day MA trending up at least 1 month → check 4
        # Book #4: 50-day MA above both 150-day and 200-day MA → checks 5, 6
        # Book #5: Price above 50-day MA → check 7
        # Book #6: Price at least 30% above 52-week low → check 8
        # Book #7: Price within 25% of 52-week high → check 9
        # Book #8: RS >= 70 → separate RS filter
        criteria = {
            # Book #1a: Current price above 150-day MA
            'price_above_150ma': close_price > ma_150,
            
            # Book #1b: Current price above 200-day MA
            'price_above_200ma': close_price > ma_200,
            
            # Book #2: 150-day MA above 200-day MA (confirmed uptrend structure)
            'ma_150_above_200': ma_150 > ma_200,
            
            # Book #3: 200-day MA trending upward for at least 1 month
            'ma_200_trending_up': ma_200_trend is not None and ma_200_trend > 0,
            
            # Book #4a: 50-day MA above 150-day MA
            'ma_50_above_150': ma_50 > ma_150,
            
            # Book #4b: 50-day MA above 200-day MA
            'ma_50_above_200': ma_50 > ma_200,
            
            # Book #5: Price above 50-day MA (short-term strength)
            'price_above_50ma': close_price > ma_50,
            
            # Book #6: Price at least 30% above 52-week low
            'above_30pct_52w_low': percent_from_52w_low >= 30,
            
            # Book #7: Price within 25% of 52-week high
            'within_25pct_of_high': percent_from_52w_high >= -25,
        }
        
        # Additional filter: RS Rating >= 70 (Minervini recommends 80+ preferred)
        # This corresponds to Book criterion #8
        rs_filter = {
            'rs_above_70': relative_strength is not None and relative_strength >= 70
        }
        
        # Count criteria passed (9 core checks)
        criteria_passed = sum(criteria.values())
        
        # Stock passes if ALL 9 criteria met AND RS filter passes
        passes_all_criteria = all(criteria.values())
        passes_rs = all(rs_filter.values())
        passes_all = passes_all_criteria and passes_rs
        
        # Create list of failed criteria (include RS in the list)
        all_checks = {**criteria, **rs_filter}
        failed_criteria = [k for k, v in all_checks.items() if not v]
        
        # Determine stage
        stage = self.determine_stage(
            close_price, ma_50, ma_150, ma_200,
            ma_200_trend, percent_from_52w_high, relative_strength
        )
        
        # Apply market cap context to analysis
        market_cap_tier = 'Unknown'
        liquidity_score = 0
        institutional_quality = False
        
        if ticker_details and ticker_details['market_cap']:
            market_cap_tier, liquidity_score, institutional_quality = \
                self.classify_market_cap_tier(ticker_details['market_cap'])
            
            # Adjust passing criteria based on liquidity/quality
            # Minervini prefers stocks with institutional backing
            if relative_strength is not None:
                if not institutional_quality:
                    # Micro caps ($100M-$300M) need higher RS to compensate for liquidity risk
                    # Effectively raises the bar for these riskier stocks
                    if relative_strength < 80:
                        passes_all = False
                        if 'micro_cap_rs_too_low' not in failed_criteria:
                            failed_criteria.append('micro_cap_rs_too_low')
        
        # Apply earnings-based filtering (if earnings data available)
        # Note: We don't disqualify stocks without earnings data, but boost those with strong earnings
        if earnings_quality['has_earnings_data']:
            # If stock passes technical criteria, apply earnings filter
            if passes_all and not earnings_quality['passes_earnings_criteria']:
                # Stock has weak fundamentals - demote it
                passes_all = False
                failed_criteria.append('weak_earnings')
            
            # Additional filter for upcoming earnings warning
            if earnings_quality['upcoming']['has_upcoming_earnings']:
                days_until = earnings_quality['upcoming']['days_until']
                if days_until and days_until <= 7:
                    # Very close to earnings - high risk
                    # Note: Not a hard disqualifier, but flagged
                    pass
        
        # Extract earnings metrics for storage
        eps_growth_yoy = None
        eps_growth_qoq = None
        revenue_growth_yoy = None
        earnings_acceleration = False
        avg_eps_surprise = None
        earnings_beat_rate = None
        has_upcoming_earnings = False
        days_until_earnings = None
        earnings_quality_score = 0
        passes_earnings = None
        
        if earnings_quality['has_earnings_data']:
            growth = earnings_quality.get('growth', {})
            surprises = earnings_quality.get('surprises', {})
            upcoming = earnings_quality.get('upcoming', {})
            
            eps_growth_yoy = growth.get('yoy_eps_growth') if growth else None
            eps_growth_qoq = growth.get('qoq_eps_growth') if growth else None
            revenue_growth_yoy = growth.get('yoy_revenue_growth') if growth else None
            earnings_acceleration = growth.get('earnings_acceleration', False) if growth else False
            
            if surprises:
                avg_eps_surprise = surprises.get('avg_eps_surprise')
                total = surprises.get('total_earnings', 0)
                beats = surprises.get('beats', 0)
                earnings_beat_rate = (beats / total * 100) if total > 0 else None
            
            if upcoming:
                has_upcoming_earnings = upcoming.get('has_upcoming_earnings', False)
                days_until_earnings = upcoming.get('days_until')
            
            earnings_quality_score = earnings_quality.get('earnings_score', 0)
            passes_earnings = earnings_quality.get('passes_earnings_criteria')
        
        metrics = {
            'symbol': symbol,
            'date': date,
            'close_price': close_price,
            'ma_50': ma_50,
            'ma_150': ma_150,
            'ma_200': ma_200,
            'week_52_high': week_52_high,
            'week_52_low': week_52_low,
            'percent_from_52w_high': percent_from_52w_high,
            'percent_from_52w_low': percent_from_52w_low,
            'ma_150_trend_20d': ma_150_trend,
            'ma_200_trend_20d': ma_200_trend,
            'relative_strength': relative_strength,
            'stage': stage,
            'passes_minervini': passes_all,
            'criteria_passed': criteria_passed,
            'criteria_failed': ','.join(failed_criteria),
            # VCP metrics
            'vcp_detected': vcp_data['vcp_detected'],
            'vcp_score': vcp_data['vcp_score'],
            'contraction_count': vcp_data['contraction_count'],
            'latest_contraction_pct': vcp_data['latest_contraction_pct'],
            'volume_contraction': vcp_data['volume_contraction'],
            'pivot_price': vcp_data['pivot_price'],
            'last_contraction_low': vcp_data['last_contraction_low'],
            # Short-term EMAs (for entry/stop calculations)
            'ema_10': ema_10,
            'ema_21': ema_21,
            'swing_low': swing_low,
            # Enhanced metrics
            'avg_dollar_volume': avg_dollar_volume,
            'volume_ratio': volume_ratio,
            'return_1m': returns['return_1m'],
            'return_3m': returns['return_3m'],
            'return_6m': returns['return_6m'],
            'return_12m': returns['return_12m'],
            'atr_14': atr_14,
            'atr_percent': atr_percent,
            'is_52w_high': is_52w_high,
            'days_since_52w_high': days_since_52w_high,
            'industry_rs': industry_rs,
            # Earnings/Fundamental metrics
            'eps_growth_yoy': eps_growth_yoy,
            'eps_growth_qoq': eps_growth_qoq,
            'revenue_growth_yoy': revenue_growth_yoy,
            'earnings_acceleration': earnings_acceleration,
            'avg_eps_surprise': avg_eps_surprise,
            'earnings_beat_rate': earnings_beat_rate,
            'has_upcoming_earnings': has_upcoming_earnings,
            'days_until_earnings': days_until_earnings,
            'earnings_quality_score': earnings_quality_score,
            'passes_earnings': passes_earnings,
            # Ticker context (for logging/debugging)
            '_market_cap_tier': market_cap_tier,
            '_liquidity_score': liquidity_score,
            '_institutional_quality': institutional_quality,
            '_earnings_issues': earnings_quality.get('issues', [])
        }
        
        # Generate Buy/Wait/Pass signal with price levels
        signal_data = self.generate_signal(metrics)
        metrics['signal'] = signal_data['signal']
        metrics['signal_reasons'] = '; '.join(signal_data['reasons'])
        metrics['entry_low'] = signal_data['entry_low']
        metrics['entry_high'] = signal_data['entry_high']
        metrics['stop_loss'] = signal_data['stop_loss']
        
        # Flatten sell targets into metrics
        sell_targets = signal_data['sell_targets']
        if sell_targets:
            metrics['sell_target_conservative'] = sell_targets['target_conservative']
            metrics['sell_target_primary'] = sell_targets['target_primary']
            metrics['sell_target_aggressive'] = sell_targets['target_aggressive']
            metrics['partial_profit_at'] = sell_targets['partial_profit_at']
            metrics['risk_reward_ratio'] = sell_targets['risk_reward_ratio']
            metrics['risk_percent'] = sell_targets['risk_percent']
        else:
            metrics['sell_target_conservative'] = None
            metrics['sell_target_primary'] = None
            metrics['sell_target_aggressive'] = None
            metrics['partial_profit_at'] = None
            metrics['risk_reward_ratio'] = None
            metrics['risk_percent'] = None
        
        return metrics
    
    def save_metrics(self, metrics):
        """Save Minervini metrics to database."""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            INSERT INTO minervini_metrics
            (symbol, date, close_price, ma_50, ma_150, ma_200, 
             week_52_high, week_52_low, percent_from_52w_high, percent_from_52w_low,
             ma_150_trend_20d, ma_200_trend_20d, relative_strength, stage, passes_minervini,
             criteria_passed, criteria_failed,
             vcp_detected, vcp_score, contraction_count, latest_contraction_pct,
             volume_contraction, pivot_price, last_contraction_low,
             ema_10, ema_21, swing_low,
             avg_dollar_volume, volume_ratio,
             return_1m, return_3m, return_6m, return_12m,
             atr_14, atr_percent,
             is_52w_high, days_since_52w_high, industry_rs,
             eps_growth_yoy, eps_growth_qoq, revenue_growth_yoy,
             earnings_acceleration, avg_eps_surprise, earnings_beat_rate,
             has_upcoming_earnings, days_until_earnings,
             earnings_quality_score, passes_earnings,
             signal, signal_reasons,
             entry_low, entry_high, stop_loss,
             sell_target_conservative, sell_target_primary, sell_target_aggressive,
             partial_profit_at, risk_reward_ratio, risk_percent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, date)
            DO UPDATE SET
                close_price = EXCLUDED.close_price,
                ma_50 = EXCLUDED.ma_50,
                ma_150 = EXCLUDED.ma_150,
                ma_200 = EXCLUDED.ma_200,
                week_52_high = EXCLUDED.week_52_high,
                week_52_low = EXCLUDED.week_52_low,
                percent_from_52w_high = EXCLUDED.percent_from_52w_high,
                percent_from_52w_low = EXCLUDED.percent_from_52w_low,
                ma_150_trend_20d = EXCLUDED.ma_150_trend_20d,
                ma_200_trend_20d = EXCLUDED.ma_200_trend_20d,
                relative_strength = EXCLUDED.relative_strength,
                stage = EXCLUDED.stage,
                passes_minervini = EXCLUDED.passes_minervini,
                criteria_passed = EXCLUDED.criteria_passed,
                criteria_failed = EXCLUDED.criteria_failed,
                vcp_detected = EXCLUDED.vcp_detected,
                vcp_score = EXCLUDED.vcp_score,
                contraction_count = EXCLUDED.contraction_count,
                latest_contraction_pct = EXCLUDED.latest_contraction_pct,
                volume_contraction = EXCLUDED.volume_contraction,
                pivot_price = EXCLUDED.pivot_price,
                last_contraction_low = EXCLUDED.last_contraction_low,
                ema_10 = EXCLUDED.ema_10,
                ema_21 = EXCLUDED.ema_21,
                swing_low = EXCLUDED.swing_low,
                avg_dollar_volume = EXCLUDED.avg_dollar_volume,
                volume_ratio = EXCLUDED.volume_ratio,
                return_1m = EXCLUDED.return_1m,
                return_3m = EXCLUDED.return_3m,
                return_6m = EXCLUDED.return_6m,
                return_12m = EXCLUDED.return_12m,
                atr_14 = EXCLUDED.atr_14,
                atr_percent = EXCLUDED.atr_percent,
                is_52w_high = EXCLUDED.is_52w_high,
                days_since_52w_high = EXCLUDED.days_since_52w_high,
                industry_rs = EXCLUDED.industry_rs,
                eps_growth_yoy = EXCLUDED.eps_growth_yoy,
                eps_growth_qoq = EXCLUDED.eps_growth_qoq,
                revenue_growth_yoy = EXCLUDED.revenue_growth_yoy,
                earnings_acceleration = EXCLUDED.earnings_acceleration,
                avg_eps_surprise = EXCLUDED.avg_eps_surprise,
                earnings_beat_rate = EXCLUDED.earnings_beat_rate,
                has_upcoming_earnings = EXCLUDED.has_upcoming_earnings,
                days_until_earnings = EXCLUDED.days_until_earnings,
                earnings_quality_score = EXCLUDED.earnings_quality_score,
                passes_earnings = EXCLUDED.passes_earnings,
                signal = EXCLUDED.signal,
                signal_reasons = EXCLUDED.signal_reasons,
                entry_low = EXCLUDED.entry_low,
                entry_high = EXCLUDED.entry_high,
                stop_loss = EXCLUDED.stop_loss,
                sell_target_conservative = EXCLUDED.sell_target_conservative,
                sell_target_primary = EXCLUDED.sell_target_primary,
                sell_target_aggressive = EXCLUDED.sell_target_aggressive,
                partial_profit_at = EXCLUDED.partial_profit_at,
                risk_reward_ratio = EXCLUDED.risk_reward_ratio,
                risk_percent = EXCLUDED.risk_percent
        """, (
            metrics['symbol'],
            metrics['date'],
            metrics['close_price'],
            metrics['ma_50'],
            metrics['ma_150'],
            metrics['ma_200'],
            metrics['week_52_high'],
            metrics['week_52_low'],
            metrics['percent_from_52w_high'],
            metrics['percent_from_52w_low'],
            metrics['ma_150_trend_20d'],
            metrics['ma_200_trend_20d'],
            metrics['relative_strength'],
            metrics['stage'],
            metrics['passes_minervini'],
            metrics['criteria_passed'],
            metrics['criteria_failed'],
            metrics['vcp_detected'],
            metrics['vcp_score'],
            metrics['contraction_count'],
            metrics['latest_contraction_pct'],
            metrics['volume_contraction'],
            metrics['pivot_price'],
            metrics['last_contraction_low'],
            metrics['ema_10'],
            metrics['ema_21'],
            metrics['swing_low'],
            metrics['avg_dollar_volume'],
            metrics['volume_ratio'],
            metrics['return_1m'],
            metrics['return_3m'],
            metrics['return_6m'],
            metrics['return_12m'],
            metrics['atr_14'],
            metrics['atr_percent'],
            metrics['is_52w_high'],
            metrics['days_since_52w_high'],
            metrics['industry_rs'],
            metrics['eps_growth_yoy'],
            metrics['eps_growth_qoq'],
            metrics['revenue_growth_yoy'],
            metrics['earnings_acceleration'],
            metrics['avg_eps_surprise'],
            metrics['earnings_beat_rate'],
            metrics['has_upcoming_earnings'],
            metrics['days_until_earnings'],
            metrics['earnings_quality_score'],
            metrics['passes_earnings'],
            metrics['signal'],
            metrics['signal_reasons'],
            metrics['entry_low'],
            metrics['entry_high'],
            metrics['stop_loss'],
            metrics['sell_target_conservative'],
            metrics['sell_target_primary'],
            metrics['sell_target_aggressive'],
            metrics['partial_profit_at'],
            metrics['risk_reward_ratio'],
            metrics['risk_percent']
        ))
        
        self.conn.commit()
        cursor.close()
    
    def analyze_date(self, date):
        """Analyze all stocks for a given date."""
        cursor = self.conn.cursor()
        
        # Get all unique symbols for the date
        cursor.execute("""
            SELECT DISTINCT symbol FROM stock_prices
            WHERE date = %s
            ORDER BY symbol
        """, (date,))
        
        symbols = [row[0] for row in cursor.fetchall()]
        
        print(f"\nAnalyzing {len(symbols)} stocks for {date}...")
        
        # Pre-calculate weighted market performance (cached for all stocks)
        market_perf = self.get_market_performance(date)
        benchmark_list = ', '.join([f"{sym}({w*100:.0f}%)" for sym, w in self.market_benchmarks.items()])
        print(f"Market performance (90-day): {market_perf:+.2f}% (weighted: {benchmark_list})")
        
        # Pre-calculate IBD-style RS percentile rankings (cached for all stocks)
        print("Calculating RS percentile rankings for all stocks...")
        rs_rankings = self.calculate_rs_percentile_ranking(date)
        print(f"  Ranked {len(rs_rankings)} stocks by relative strength")
        
        # Pre-calculate sector performance for industry RS
        print("Calculating sector performance...")
        self.calculate_sector_performance(date)
        
        analyzed = 0
        passed = 0
        skipped = 0
        
        for i, symbol in enumerate(symbols):
            try:
                metrics = self.evaluate_minervini_criteria(symbol, date)
                
                if metrics:
                    self.save_metrics(metrics)
                    analyzed += 1
                    
                    if metrics['passes_minervini']:
                        passed += 1
                        
                        # Signal indicator
                        sig = metrics.get('signal', 'PASS')
                        sig_icon = {'BUY': '🟢', 'WAIT': '🟡', 'PASS': '🔴'}.get(sig, '⚪')
                        
                        vcp_info = ""
                        if metrics['vcp_detected']:
                            vcp_info = f" [VCP: {metrics['vcp_score']:.0f}]"
                        
                        # Price levels for BUY/WAIT signals
                        price_info = ""
                        if sig in ('BUY', 'WAIT') and metrics.get('entry_low'):
                            price_info = f" Entry: ${metrics['entry_low']:.2f}-${metrics['entry_high']:.2f}"
                            if metrics.get('stop_loss'):
                                price_info += f" Stop: ${metrics['stop_loss']:.2f}"
                            if metrics.get('sell_target_primary'):
                                price_info += f" Target: ${metrics['sell_target_primary']:.2f}"
                        
                        # Add market cap context if available
                        cap_info = ""
                        if metrics.get('_market_cap_tier') and metrics['_market_cap_tier'] != 'Unknown':
                            tier = metrics['_market_cap_tier']
                            quality = "⭐" if metrics.get('_institutional_quality') else ""
                            cap_info = f" [{tier}{quality}]"
                        
                        print(f"  {sig_icon} {symbol}: {sig} - Stage {metrics['stage']} ({metrics['criteria_passed']}/9, RS={metrics['relative_strength']:.0f}){vcp_info}{price_info}{cap_info}")
                else:
                    skipped += 1
                
                if (i + 1) % 100 == 0:
                    print(f"  Progress: {i + 1}/{len(symbols)} stocks processed...")
                    
            except Exception as e:
                print(f"  Error analyzing {symbol}: {e}")
                skipped += 1
                continue
        
        # Count stocks by market cap tier (for passed stocks)
        cursor.execute("""
            SELECT COUNT(DISTINCT mm.symbol) as count
            FROM minervini_metrics mm
            LEFT JOIN ticker_details td ON mm.symbol = td.symbol
            WHERE mm.date = %s AND mm.passes_minervini = true
            AND td.market_cap >= 300000000 AND td.market_cap < 2000000000
        """, (date,))
        small_cap_count = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(DISTINCT mm.symbol) as count
            FROM minervini_metrics mm
            LEFT JOIN ticker_details td ON mm.symbol = td.symbol
            WHERE mm.date = %s AND mm.passes_minervini = true
            AND td.market_cap >= 2000000000 AND td.market_cap < 10000000000
        """, (date,))
        mid_cap_count = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(DISTINCT mm.symbol) as count
            FROM minervini_metrics mm
            LEFT JOIN ticker_details td ON mm.symbol = td.symbol
            WHERE mm.date = %s AND mm.passes_minervini = true
            AND td.market_cap >= 10000000000
        """, (date,))
        large_cap_count = cursor.fetchone()[0]
        
        # Count signal distribution
        cursor.execute("""
            SELECT signal, COUNT(*) FROM minervini_metrics
            WHERE date = %s AND passes_minervini = true AND signal IS NOT NULL
            GROUP BY signal ORDER BY signal
        """, (date,))
        signal_counts = {row[0]: row[1] for row in cursor.fetchall()}
        
        print(f"\nAnalysis Summary:")
        print(f"  Total symbols: {len(symbols)}")
        print(f"  Analyzed: {analyzed}")
        print(f"  Passed Minervini: {passed} ({(passed/analyzed*100) if analyzed else 0:.1f}%)")
        print(f"  Skipped (insufficient data/filtered): {skipped}")
        
        if signal_counts:
            print(f"\n  Signal Distribution (Passing Stocks):")
            print(f"    🟢 BUY:  {signal_counts.get('BUY', 0)}")
            print(f"    🟡 WAIT: {signal_counts.get('WAIT', 0)}")
            print(f"    🔴 PASS: {signal_counts.get('PASS', 0)}")
        
        if passed > 0:
            print(f"\n  Market Cap Distribution (Passing Stocks):")
            print(f"    Small Cap ($300M-$2B): {small_cap_count}")
            print(f"    Mid Cap ($2B-$10B): {mid_cap_count}")
            print(f"    Large Cap+ ($10B+): {large_cap_count}")
            total_categorized = small_cap_count + mid_cap_count + large_cap_count
            if total_categorized < passed:
                print(f"    Unknown/Micro Cap: {passed - total_categorized}")
        
        cursor.close()
        return analyzed, passed
    
    def get_top_stocks(self, date, limit=50):
        """Get top stocks that pass Minervini criteria with signal and price levels."""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT mm.symbol, mm.close_price, mm.relative_strength, mm.stage,
                   mm.percent_from_52w_high, mm.vcp_detected, mm.vcp_score, mm.pivot_price,
                   mm.avg_dollar_volume, mm.volume_ratio, mm.industry_rs,
                   mm.return_1m, mm.return_3m, mm.atr_percent, mm.is_52w_high,
                   mm.eps_growth_yoy, mm.eps_growth_qoq, mm.earnings_acceleration,
                   mm.earnings_beat_rate, mm.has_upcoming_earnings, mm.days_until_earnings,
                   mm.earnings_quality_score, mm.passes_earnings,
                   td.market_cap, td.name,
                   mm.signal, mm.signal_reasons,
                   mm.entry_low, mm.entry_high, mm.stop_loss,
                   mm.sell_target_primary, mm.risk_reward_ratio, mm.risk_percent
            FROM minervini_metrics mm
            LEFT JOIN ticker_details td ON mm.symbol = td.symbol
            WHERE mm.date = %s AND mm.passes_minervini = true
            ORDER BY 
                CASE mm.signal WHEN 'BUY' THEN 1 WHEN 'WAIT' THEN 2 ELSE 3 END,
                mm.passes_earnings DESC NULLS LAST,
                mm.earnings_quality_score DESC NULLS LAST,
                mm.relative_strength DESC, 
                mm.industry_rs DESC NULLS LAST
            LIMIT %s
        """, (date, limit))
        
        results = cursor.fetchall()
        cursor.close()
        return results
    
    def close(self):
        """Close database connection."""
        self.conn.close()


def main():
    """Main execution function."""
    print("=" * 80)
    print("Minervini Stock Analyzer")
    print("=" * 80)
    
    # Initialize analyzer
    analyzer = MinerviniAnalyzer()
    
    # Use the latest date available in stock_prices
    cursor = analyzer.conn.cursor()
    cursor.execute("SELECT MAX(date) FROM stock_prices")
    result = cursor.fetchone()
    cursor.close()
    
    if not result or not result[0]:
        print("\n⚠ No data found in stock_prices table")
        print("Run the download script first to load data:")
        print("  python download_stock_data.py")
        analyzer.close()
        return
    
    target_date = result[0].strftime("%Y-%m-%d")
    print(f"Target date: {target_date} (latest in stock_prices)")
    
    # Analyze stocks
    analyzed, passed = analyzer.analyze_date(target_date)
    
    # Show top stocks
    if passed > 0:
        print("\n" + "=" * 160)
        print("TOP STOCKS PASSING MINERVINI CRITERIA (with Signal, Entry, Stop & Target)")
        print("=" * 160)
        
        top_stocks = analyzer.get_top_stocks(target_date, limit=30)
        
        # Header row
        print(f"\n{'Signal':<6} {'Symbol':<8} {'Price':>9} {'RS':>4} {'VCP':>4} "
              f"{'Entry Range':>17} {'Stop':>9} {'Target':>9} {'R:R':>5} {'Risk%':>6} "
              f"{'EPSyy':>7} {'Beat%':>6} {'$Vol(M)':>8}")
        print("-" * 160)
        
        current_signal_group = None
        
        for stock in top_stocks:
            (symbol, price, rs, stage, pct_high, vcp_detected, vcp_score, pivot,
             avg_dv, vol_ratio, ind_rs, ret_1m, ret_3m, atr_pct, is_new_high,
             eps_yoy, eps_qoq, accel, beat_rate, upcoming, days_until, eq_score, passes_earn,
             market_cap, name,
             sig, sig_reasons, entry_low, entry_high, stop_loss,
             sell_target, rr_ratio, risk_pct) = stock
            
            # Group separator between signal types
            if sig != current_signal_group:
                if current_signal_group is not None:
                    print()
                current_signal_group = sig
            
            # Signal indicator
            sig_icon = {'BUY': '🟢BUY', 'WAIT': '🟡WAIT', 'PASS': '🔴PASS'}.get(sig or 'PASS', '⚪-')
            
            # Format values with fallbacks for None
            rs_str = f"{rs:.0f}" if rs else "-"
            vcp_str = f"{vcp_score:.0f}" if vcp_detected else "-"
            
            # Entry range
            if entry_low and entry_high:
                entry_str = f"${entry_low:.2f}-${entry_high:.2f}"
            else:
                entry_str = "-"
            
            # Stop loss
            stop_str = f"${stop_loss:.2f}" if stop_loss else "-"
            
            # Sell target
            target_str = f"${sell_target:.2f}" if sell_target else "-"
            
            # Risk/reward
            rr_str = f"{rr_ratio:.1f}" if rr_ratio else "-"
            risk_str = f"{risk_pct:.1f}%" if risk_pct else "-"
            
            # Fundamentals
            eps_yoy_str = f"{eps_yoy:+.0f}%" if eps_yoy is not None else "-"
            if accel:
                eps_yoy_str += "🚀"
            beat_str = f"{beat_rate:.0f}%" if beat_rate is not None else "-"
            avg_dv_str = f"{avg_dv/1_000_000:.1f}" if avg_dv else "-"
            
            print(f"{sig_icon:<6} {symbol:<8} ${price:>8.2f} {rs_str:>4} {vcp_str:>4} "
                  f"{entry_str:>17} {stop_str:>9} {target_str:>9} {rr_str:>5} {risk_str:>6} "
                  f"{eps_yoy_str:>7} {beat_str:>6} {avg_dv_str:>8}")
        
        # Show signal reasons for BUY/WAIT stocks
        print("\n" + "-" * 160)
        print("\nSIGNAL DETAILS:")
        print("-" * 80)
        
        for stock in top_stocks:
            sig = stock[25]  # signal column
            symbol = stock[0]
            sig_reasons = stock[26]  # signal_reasons column
            
            if sig in ('BUY', 'WAIT') and sig_reasons:
                sig_icon = '🟢' if sig == 'BUY' else '🟡'
                print(f"  {sig_icon} {symbol}: {sig_reasons}")
        
        print("\n" + "-" * 160)
        print("Legend: RS=Relative Strength, VCP=VCP Score, R:R=Risk/Reward Ratio, Risk%=Downside Risk %,")
        print("        EPSyy=EPS YoY Growth (🚀=accelerating), Beat%=Earnings Beat Rate (last 4 qtrs),")
        print("        $Vol(M)=Avg Dollar Volume in Millions")
        print("        Entry Range=Suggested buy zone, Stop=Stop loss, Target=Primary sell target")
    
    # Close connection
    analyzer.close()
    
    print("\n" + "=" * 80)
    print(f"Database: PostgreSQL @ localhost:5432/stocks")
    print("=" * 80)


if __name__ == "__main__":
    main()
