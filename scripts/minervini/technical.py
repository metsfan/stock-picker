"""
Technical indicator calculations for Minervini analysis.

Includes moving averages (SMA/EMA), ATR, 52-week high/low metrics,
MA trend analysis, and swing low detection.
"""

from datetime import datetime, timedelta


class TechnicalIndicators:
    """Calculates price-based technical indicators from stock_prices data."""

    def __init__(self, conn):
        """
        Args:
            conn: psycopg2 database connection
        """
        self.conn = conn

    def calculate_moving_average(self, symbol, end_date, days):
        """Calculate simple moving average for a symbol over specified days."""
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

    def calculate_ma_trend(self, symbol, end_date, ma_period, trend_days=20):
        """
        Calculate the trend (slope) of a moving average over specified days.

        Args:
            symbol: Stock symbol
            end_date: Date to calculate trend for
            ma_period: Moving average period (e.g., 150, 200)
            trend_days: Days to measure trend over (default 20 ~ 1 month)

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

    def calculate_ma_200_trend(self, symbol, end_date):
        """
        Calculate the trend of 200-day MA over the last ~1 month (30 calendar days).
        Returns the percentage change in MA over this period.

        Minervini requires 200-day MA trending up for at least 1 month
        (~22 trading days ≈ 30 calendar days).
        """
        return self.calculate_ma_trend(symbol, end_date, 200, 30)

    def calculate_ma_150_trend(self, symbol, end_date):
        """
        Calculate the trend of 150-day MA over the last ~1 month (30 calendar days).
        Returns the percentage change in MA over this period.

        Minervini requires 150-day MA trending upward.
        """
        return self.calculate_ma_trend(symbol, end_date, 150, 30)

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
