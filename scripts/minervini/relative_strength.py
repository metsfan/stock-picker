"""
Relative strength calculations for Minervini analysis.

Computes weighted market benchmark performance, IBD-style quarterly-weighted
stock performance, and percentile RS rankings.
"""

from datetime import datetime, timedelta


class RelativeStrengthCalculator:
    """Calculates IBD-style relative strength ratings and market benchmarks."""

    def __init__(self, conn, market_benchmarks):
        """
        Args:
            conn: psycopg2 database connection
            market_benchmarks: Dict of {symbol: weight} for benchmark indices
        """
        self.conn = conn
        self.market_benchmarks = market_benchmarks

        # Caches
        self.market_performance_cache = {}
        self._perf_cache = {}
        self._rs_cache = {}

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
        if cache_key in self._perf_cache:
            return self._perf_cache[cache_key]

        cursor = self.conn.cursor()
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")

        # Define quarters for weighted calculation
        # Each quarter = 63 trading days ≈ 91 calendar days (252 trading days / year ≈ 365 calendar days)
        # Previous code incorrectly used 63 calendar days per quarter, covering only ~8 months total
        quarters = [
            ((end_date_obj - timedelta(days=91)).strftime("%Y-%m-%d"), end_date, 0.40),    # Q1: most recent ~3 months
            ((end_date_obj - timedelta(days=182)).strftime("%Y-%m-%d"),
             (end_date_obj - timedelta(days=91)).strftime("%Y-%m-%d"), 0.20),              # Q2: ~3-6 months ago
            ((end_date_obj - timedelta(days=273)).strftime("%Y-%m-%d"),
             (end_date_obj - timedelta(days=182)).strftime("%Y-%m-%d"), 0.20),             # Q3: ~6-9 months ago
            ((end_date_obj - timedelta(days=365)).strftime("%Y-%m-%d"),
             (end_date_obj - timedelta(days=273)).strftime("%Y-%m-%d"), 0.20),             # Q4: ~9-12 months ago
        ]

        # Get all symbols with sufficient data
        cursor.execute("""
            SELECT DISTINCT symbol FROM ticker_details
        """)
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
                performances[symbol] = weighted_perf / total_weight

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
        if cache_key in self._rs_cache:
            return self._rs_cache[cache_key]

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
