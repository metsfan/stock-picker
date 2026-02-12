"""
Sector analysis for Minervini analysis.

Includes market cap tier classification, sector performance calculation,
and industry relative strength lookups.
"""

from datetime import datetime, timedelta


class SectorAnalyzer:
    """Analyzes sector performance and market cap classification."""

    def __init__(self, conn):
        """
        Args:
            conn: psycopg2 database connection
        """
        self.conn = conn

    @staticmethod
    def classify_market_cap_tier(market_cap):
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

    def calculate_sector_performance(self, date, market_perf):
        """
        Calculate and store sector performance for all SIC codes.
        This creates the sector_performance table data for the given date.

        Should be called once per date before analyzing individual stocks.

        Args:
            date: Date to calculate sector performance for
            market_perf: Pre-calculated weighted market performance (from RelativeStrengthCalculator)
        """
        cursor = self.conn.cursor()

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

    def update_sector_aggregates(self, date):
        """
        Update sector_performance with pre-computed stock-level aggregates.

        Must be called AFTER all stocks have been analyzed and saved to
        minervini_metrics for the given date, because it reads from that table.

        Computes per-sector: total market cap, BUY count, passing count,
        Stage 2 count, and VCP count.
        """
        cursor = self.conn.cursor()

        cursor.execute("""
            UPDATE sector_performance sp SET
                sector_market_cap = agg.sector_market_cap,
                buy_count         = agg.buy_count,
                passing_count     = agg.passing_count,
                stage2_count      = agg.stage2_count,
                vcp_count         = agg.vcp_count
            FROM (
                SELECT
                    td.sic_code,
                    COALESCE(SUM(td.market_cap), 0)                           AS sector_market_cap,
                    COUNT(*) FILTER (WHERE mm.signal = 'BUY')                 AS buy_count,
                    COUNT(*) FILTER (WHERE mm.passes_minervini = TRUE)        AS passing_count,
                    COUNT(*) FILTER (WHERE mm.stage = 2)                      AS stage2_count,
                    COUNT(*) FILTER (WHERE mm.vcp_detected = TRUE)            AS vcp_count
                FROM minervini_metrics mm
                JOIN ticker_details td ON td.symbol = mm.symbol
                WHERE mm.date = %s
                  AND td.sic_code IS NOT NULL AND td.sic_code != ''
                GROUP BY td.sic_code
            ) agg
            WHERE sp.date = %s
              AND sp.sic_code = agg.sic_code
        """, (date, date))

        self.conn.commit()
        updated = cursor.rowcount
        cursor.close()
        print(f"  Updated {updated} sector aggregate records")

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
