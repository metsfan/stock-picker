"""
Earnings and fundamental analysis for Minervini analysis.

Includes EPS/revenue growth calculations, earnings surprise history,
upcoming earnings detection, and overall earnings quality evaluation.
"""

from datetime import datetime, timedelta, date


class EarningsAnalyzer:
    """Analyzes earnings fundamentals per Minervini's SEPA methodology."""

    def __init__(self, conn):
        """
        Args:
            conn: psycopg2 database connection
        """
        self.conn = conn

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

        # Build a lookup by (fiscal_year, fiscal_quarter) for explicit YoY matching.
        # This avoids positional assumptions that break when quarterly reports
        # have gaps, restated periods, or fiscal year changes.
        quarter_lookup = {}
        for q in quarters:
            fy = q[1]  # fiscal_year
            fq = q[2]  # fiscal_quarter
            if fy is not None and fq is not None:
                quarter_lookup[(fy, fq)] = q

        def _find_year_ago(q):
            """Find the same fiscal quarter from the prior year."""
            fy = q[1]
            fq = q[2]
            if fy is not None and fq is not None:
                return quarter_lookup.get((fy - 1, fq))
            return None

        # Year-over-year (YoY) growth for latest quarter
        yoy_eps_growth = None
        yoy_revenue_growth = None
        year_ago = _find_year_ago(latest)

        if year_ago:
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

        # Walk the most recent quarters and pair each with its year-ago match
        for q in quarters[:4]:
            year_ago_q = _find_year_ago(q)
            if not year_ago_q:
                continue

            current_eps = float(q[3]) if q[3] else None
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
        # Use count of non-null surprises (beats + misses) as denominator,
        # not len(results), so null surprise rows don't penalize the stock.
        evaluated = beats + misses
        all_beats = evaluated > 0 and misses == 0
        mostly_beats = beats >= evaluated * 0.75 if evaluated > 0 else False

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

    def evaluate_earnings_quality(self, symbol, analysis_date):
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
        upcoming = self.check_upcoming_earnings(symbol, analysis_date)

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
