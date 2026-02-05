#!/usr/bin/env python3
"""
Minervini Stock Analyzer (Enhanced with Company Fundamentals)

Analyzes stocks based on Mark Minervini's trend template criteria:
1. Stock price > 150-day MA and 200-day MA
2. 150-day MA > 200-day MA
3. 200-day MA trending up for at least 1 month
4. 50-day MA > 150-day MA and 200-day MA
5. Stock price > 50-day MA
6. Stock price within 25% of 52-week high
7. Relative Strength (RS) rating >= 70

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
from datetime import datetime, timedelta
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
            'user': 'postgres',
            'password': 'adamesk'
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
    
    def calculate_ma_200_trend(self, symbol, end_date):
        """
        Calculate the trend of 200-day MA over the last 20 days.
        Returns the change in MA over this period.
        """
        # Get 200-day MA for 20 days ago
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        date_20d_ago = (end_date_obj - timedelta(days=20)).strftime("%Y-%m-%d")
        
        ma_today = self.calculate_moving_average(symbol, end_date, 200)
        ma_20d_ago = self.calculate_moving_average(symbol, date_20d_ago, 200)
        
        if ma_today is None or ma_20d_ago is None:
            return None
        
        return ((ma_today - ma_20d_ago) / ma_20d_ago) * 100
    
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
    
    def calculate_relative_strength(self, symbol, end_date):
        """
        Calculate relative strength comparing stock performance vs market benchmarks.
        Uses cached market performance for efficiency.
        Returns a score 0-100.
        """
        cursor = self.conn.cursor()
        
        # Get stock performance over last 90 days
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        start_date = (end_date_obj - timedelta(days=90)).strftime("%Y-%m-%d")
        
        cursor.execute("""
            SELECT close FROM stock_prices
            WHERE symbol = %s AND date BETWEEN %s AND %s
            ORDER BY date ASC
        """, (symbol, start_date, end_date))
        
        prices = [float(row[0]) for row in cursor.fetchall()]
        
        if len(prices) < 2:
            return None
        
        stock_performance = ((prices[-1] - prices[0]) / prices[0]) * 100
        
        # Get cached market performance
        market_avg = self.get_market_performance(end_date)
        
        # Convert to 0-100 scale where 50 is average
        if stock_performance > market_avg:
            rs_score = 50 + min(50, (stock_performance - market_avg) * 2)
        else:
            rs_score = 50 - min(50, (market_avg - stock_performance) * 2)
        
        cursor.close()
        return max(0, min(100, rs_score))
    
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
            cursor.execute("""
                WITH sector_stocks AS (
                    SELECT sp.symbol,
                           FIRST_VALUE(sp.close) OVER (PARTITION BY sp.symbol ORDER BY sp.date ASC) as start_price,
                           LAST_VALUE(sp.close) OVER (PARTITION BY sp.symbol ORDER BY sp.date ASC 
                               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as end_price
                    FROM stock_prices sp
                    JOIN ticker_details td ON sp.symbol = td.symbol
                    WHERE td.sic_code = %s
                    AND sp.date BETWEEN %s AND %s
                )
                SELECT AVG((end_price - start_price) / NULLIF(start_price, 0) * 100) as avg_return,
                       COUNT(DISTINCT symbol) as stock_count
                FROM sector_stocks
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
                'pivot_price': None
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
                'pivot_price': None
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
                'pivot_price': None
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
                'pivot_price': None
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
        
        return {
            'vcp_detected': vcp_detected,
            'vcp_score': vcp_score,
            'contraction_count': contraction_count,
            'latest_contraction_pct': latest_contraction_pct,
            'volume_contraction': volume_contraction,
            'pivot_price': pivot_price
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
        
        # Check if we have enough data
        if None in [ma_50, ma_150, ma_200, week_52_high]:
            return None
        
        # Calculate percent from 52-week high
        percent_from_52w_high = ((close_price - week_52_high) / week_52_high) * 100
        
        # Evaluate Minervini criteria
        criteria = {
            'price_above_150ma': close_price > ma_150,
            'price_above_200ma': close_price > ma_200,
            'ma_150_above_200': ma_150 > ma_200,
            'ma_200_trending_up': ma_200_trend is not None and ma_200_trend > 0,
            'ma_50_above_150': ma_50 > ma_150,
            'ma_50_above_200': ma_50 > ma_200,
            'price_above_50ma': close_price > ma_50,
            'within_25_percent_of_high': percent_from_52w_high >= -25,
            'rs_above_70': relative_strength is not None and relative_strength >= 70
        }
        
        criteria_passed = sum(criteria.values())
        passes_all = all(criteria.values())
        
        # Create list of failed criteria
        failed_criteria = [k for k, v in criteria.items() if not v]
        
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
            # Enhanced metrics (new)
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
            # Ticker context (for logging/debugging)
            '_market_cap_tier': market_cap_tier,
            '_liquidity_score': liquidity_score,
            '_institutional_quality': institutional_quality
        }
        
        return metrics
    
    def save_metrics(self, metrics):
        """Save Minervini metrics to database."""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            INSERT INTO minervini_metrics
            (symbol, date, close_price, ma_50, ma_150, ma_200, 
             week_52_high, week_52_low, percent_from_52w_high,
             ma_200_trend_20d, relative_strength, stage, passes_minervini,
             criteria_passed, criteria_failed,
             vcp_detected, vcp_score, contraction_count, latest_contraction_pct,
             volume_contraction, pivot_price,
             avg_dollar_volume, volume_ratio,
             return_1m, return_3m, return_6m, return_12m,
             atr_14, atr_percent,
             is_52w_high, days_since_52w_high, industry_rs)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, date)
            DO UPDATE SET
                close_price = EXCLUDED.close_price,
                ma_50 = EXCLUDED.ma_50,
                ma_150 = EXCLUDED.ma_150,
                ma_200 = EXCLUDED.ma_200,
                week_52_high = EXCLUDED.week_52_high,
                week_52_low = EXCLUDED.week_52_low,
                percent_from_52w_high = EXCLUDED.percent_from_52w_high,
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
                industry_rs = EXCLUDED.industry_rs
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
            metrics['industry_rs']
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
                        vcp_info = ""
                        if metrics['vcp_detected']:
                            vcp_info = f" [VCP Score: {metrics['vcp_score']:.0f}]"
                        
                        # Add market cap context if available
                        cap_info = ""
                        if metrics.get('_market_cap_tier') and metrics['_market_cap_tier'] != 'Unknown':
                            tier = metrics['_market_cap_tier']
                            quality = "⭐" if metrics.get('_institutional_quality') else ""
                            cap_info = f" [{tier}{quality}]"
                        
                        print(f"  ✓ {symbol}: PASSED - Stage {metrics['stage']} ({metrics['criteria_passed']}/9 criteria){vcp_info}{cap_info}")
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
        
        print(f"\nAnalysis Summary:")
        print(f"  Total symbols: {len(symbols)}")
        print(f"  Analyzed: {analyzed}")
        print(f"  Passed Minervini: {passed} ({(passed/analyzed*100) if analyzed else 0:.1f}%)")
        print(f"  Skipped (insufficient data/filtered): {skipped}")
        
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
        """Get top stocks that pass Minervini criteria with enhanced metrics."""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT mm.symbol, mm.close_price, mm.relative_strength, mm.stage,
                   mm.percent_from_52w_high, mm.vcp_detected, mm.vcp_score, mm.pivot_price,
                   mm.avg_dollar_volume, mm.volume_ratio, mm.industry_rs,
                   mm.return_1m, mm.return_3m, mm.atr_percent, mm.is_52w_high,
                   td.market_cap, td.name
            FROM minervini_metrics mm
            LEFT JOIN ticker_details td ON mm.symbol = td.symbol
            WHERE mm.date = %s AND mm.passes_minervini = true
            ORDER BY mm.relative_strength DESC, mm.industry_rs DESC NULLS LAST
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
    # Use latest available date (today minus 2 days due to source data delay)
    latest_available = datetime.now() - timedelta(days=2)
    target_date = latest_available.strftime("%Y-%m-%d")
    
    print("=" * 80)
    print("Minervini Stock Analyzer")
    print("=" * 80)
    print(f"Target date: {target_date} (latest available)")
    
    # Initialize analyzer
    analyzer = MinerviniAnalyzer()
    
    # Check if data exists for this date
    if not analyzer.check_data_exists(target_date):
        print(f"\n⚠ No data found in database for {target_date}")
        print("Run the download script first to load data:")
        print("  python download_stock_data.py")
        analyzer.close()
        return
    
    print(f"✓ Data found in database for {target_date}")
    
    # Analyze stocks
    analyzed, passed = analyzer.analyze_date(target_date)
    
    # Show top stocks
    if passed > 0:
        print("\n" + "=" * 120)
        print("TOP STOCKS PASSING MINERVINI CRITERIA (Enhanced)")
        print("=" * 120)
        
        top_stocks = analyzer.get_top_stocks(target_date, limit=20)
        
        # Header row
        print(f"\n{'Symbol':<8} {'Price':>9} {'RS':>5} {'IndRS':>6} {'Stage':>5} "
              f"{'%52wH':>7} {'1M%':>7} {'3M%':>7} {'$Vol(M)':>9} {'VolR':>5} "
              f"{'ATR%':>5} {'VCP':>4} {'New Hi':>6}")
        print("-" * 120)
        
        for stock in top_stocks:
            (symbol, price, rs, stage, pct_high, vcp_detected, vcp_score, pivot,
             avg_dv, vol_ratio, ind_rs, ret_1m, ret_3m, atr_pct, is_new_high,
             market_cap, name) = stock
            
            # Format values with fallbacks for None
            rs_str = f"{rs:.0f}" if rs else "-"
            ind_rs_str = f"{ind_rs:.0f}" if ind_rs else "-"
            pct_high_str = f"{pct_high:.1f}%" if pct_high else "-"
            ret_1m_str = f"{ret_1m:+.1f}%" if ret_1m else "-"
            ret_3m_str = f"{ret_3m:+.1f}%" if ret_3m else "-"
            avg_dv_str = f"{avg_dv/1_000_000:.1f}" if avg_dv else "-"
            vol_ratio_str = f"{vol_ratio:.1f}" if vol_ratio else "-"
            atr_str = f"{atr_pct:.1f}" if atr_pct else "-"
            vcp_str = f"{vcp_score:.0f}" if vcp_detected else "-"
            new_hi_str = "★" if is_new_high else ""
            
            print(f"{symbol:<8} ${price:>8.2f} {rs_str:>5} {ind_rs_str:>6} {stage:>5} "
                  f"{pct_high_str:>7} {ret_1m_str:>7} {ret_3m_str:>7} {avg_dv_str:>9} {vol_ratio_str:>5} "
                  f"{atr_str:>5} {vcp_str:>4} {new_hi_str:>6}")
        
        print("\n" + "-" * 120)
        print("Legend: RS=Relative Strength, IndRS=Industry RS, $Vol(M)=Avg Dollar Volume in Millions,")
        print("        VolR=Volume Ratio (today vs 50d avg), ATR%=Volatility, VCP=VCP Score, ★=New 52w High")
    
    # Close connection
    analyzer.close()
    
    print("\n" + "=" * 80)
    print(f"Database: PostgreSQL @ localhost:5432/stocks")
    print("=" * 80)


if __name__ == "__main__":
    main()
