"""
Volume and return calculations for Minervini analysis.

Includes average dollar volume, volume ratio (today vs average),
and multi-timeframe return calculations.
"""


class VolumeAnalyzer:
    """Calculates volume metrics and multi-timeframe returns."""

    def __init__(self, conn):
        """
        Args:
            conn: psycopg2 database connection
        """
        self.conn = conn

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
        return round(sum(dollar_volumes) / len(dollar_volumes))

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

        # Get average volume (excluding today).
        # Use COUNT to enforce a minimum data threshold -- need at least
        # half the requested days for a reliable average (same rule as
        # calculate_avg_dollar_volume).
        cursor.execute("""
            SELECT AVG(volume), COUNT(*) FROM (
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
        row_count = int(result[1])

        if row_count < avg_days // 2:
            return None

        return round(today_volume / avg_volume, 2)

    def calculate_returns(self, symbol, end_date):
        """
        Calculate multi-timeframe returns (1m, 3m, 6m, 12m).

        IBD-style momentum analysis uses weighted returns across timeframes.
        Strong stocks show positive returns across all timeframes.

        Uses exact trading-day counting (OFFSET) rather than a calendar-day
        approximation, so the 12-month return is a true 252-trading-day return.

        Args:
            symbol: Stock symbol
            end_date: Date to calculate returns from

        Returns:
            dict: Returns for each timeframe or None values if insufficient data
        """
        cursor = self.conn.cursor()

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
            # Count back exactly N trading days using OFFSET.
            # OFFSET 0 = most recent day <= end_date, so OFFSET days = (days+1)th row back,
            # which is the close from exactly `days` trading days ago.
            cursor.execute("""
                SELECT close FROM stock_prices
                WHERE symbol = %s AND date <= %s
                ORDER BY date DESC
                LIMIT 1 OFFSET %s
            """, (symbol, end_date, days))

            result = cursor.fetchone()
            if result and result[0]:
                past_price = float(result[0])
                returns[key] = round(((current_price - past_price) / past_price) * 100, 2)
            else:
                returns[key] = None

        cursor.close()
        return returns
