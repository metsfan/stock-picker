#!/usr/bin/env python3
"""
Minervini Stock Analyzer

Analyzes stocks based on Mark Minervini's trend template criteria:
1. Stock price > 150-day MA and 200-day MA
2. 150-day MA > 200-day MA
3. 200-day MA trending up for at least 1 month
4. 50-day MA > 150-day MA and 200-day MA
5. Stock price > 50-day MA
6. Stock price within 25% of 52-week high
7. Relative Strength (RS) rating >= 70

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

This script processes daily stock data and stores metrics in SQLite for trend analysis.
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
        return (float(result[0]) if result[0] is not None else None,
                float(result[1]) if result[1] is not None else None)
    
    def calculate_ma_200_trend(self, symbol, end_date):
        """
        Calculate the trend of 200-day MA over the last 20 days.
        Returns the change in MA over this period.
        """
        cursor = self.conn.cursor()
        
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
        Returns metrics and pass/fail status.
        """
        cursor = self.conn.cursor()
        
        # Get current price
        cursor.execute("""
            SELECT close FROM stock_prices
            WHERE symbol = %s AND date = %s
        """, (symbol, date))
        
        result = cursor.fetchone()
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
            'pivot_price': vcp_data['pivot_price']
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
             volume_contraction, pivot_price)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                pivot_price = EXCLUDED.pivot_price
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
            metrics['pivot_price']
        ))
        
        self.conn.commit()
    
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
                        print(f"  ✓ {symbol}: PASSED - Stage {metrics['stage']} ({metrics['criteria_passed']}/9 criteria){vcp_info}")
                else:
                    skipped += 1
                
                if (i + 1) % 100 == 0:
                    print(f"  Progress: {i + 1}/{len(symbols)} stocks processed...")
                    
            except Exception as e:
                print(f"  Error analyzing {symbol}: {e}")
                skipped += 1
                continue
        
        print(f"\nAnalysis Summary:")
        print(f"  Total symbols: {len(symbols)}")
        print(f"  Analyzed: {analyzed}")
        print(f"  Passed Minervini: {passed} ({(passed/analyzed*100) if analyzed else 0:.1f}%)")
        print(f"  Skipped (insufficient data): {skipped}")
        
        return analyzed, passed
    
    def get_top_stocks(self, date, limit=50):
        """Get top stocks that pass Minervini criteria."""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT symbol, close_price, ma_50, ma_150, ma_200,
                   percent_from_52w_high, relative_strength, stage, criteria_passed,
                   vcp_detected, vcp_score, pivot_price
            FROM minervini_metrics
            WHERE date = %s AND passes_minervini = true
            ORDER BY relative_strength DESC, criteria_passed DESC
            LIMIT %s
        """, (date, limit))
        
        return cursor.fetchall()
    
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
        print("\n" + "=" * 80)
        print("TOP STOCKS PASSING MINERVINI CRITERIA")
        print("=" * 80)
        
        top_stocks = analyzer.get_top_stocks(target_date, limit=20)
        
        print(f"\n{'Symbol':<10} {'Price':<10} {'MA50':<10} {'MA150':<10} "
              f"{'MA200':<10} {'%52wH':<8} {'RS':<6} {'Stage':<6} {'VCP':<5} {'Pivot':<10}")
        print("-" * 100)
        
        for stock in top_stocks:
            symbol, price, ma50, ma150, ma200, pct_high, rs, stage, score, vcp_detected, vcp_score, pivot = stock
            vcp_str = f"{vcp_score:.0f}" if vcp_detected else "-"
            pivot_str = f"${pivot:.2f}" if pivot else "-"
            print(f"{symbol:<10} ${price:<9.2f} ${ma50:<9.2f} ${ma150:<9.2f} "
                  f"${ma200:<9.2f} {pct_high:<7.1f}% {rs:<5.0f} {stage:<6} {vcp_str:<5} {pivot_str:<10}")
    
    # Close connection
    analyzer.close()
    
    print("\n" + "=" * 80)
    print(f"Database: PostgreSQL @ localhost:5432/stocks")
    print("=" * 80)


if __name__ == "__main__":
    main()
