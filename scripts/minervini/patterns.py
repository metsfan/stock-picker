"""
Chart pattern detection for Minervini analysis.

Implements:
- Volatility Contraction Pattern (VCP) detection
- Primary Base detection for IPOs/new issues
"""

from datetime import datetime, timedelta


class PatternDetector:
    """Detects chart patterns relevant to Minervini's methodology."""

    def __init__(self, conn):
        """
        Args:
            conn: psycopg2 database connection
        """
        self.conn = conn

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

        empty_result = {
            'vcp_detected': False,
            'vcp_score': 0,
            'contraction_count': 0,
            'latest_contraction_pct': None,
            'volume_contraction': False,
            'pivot_price': None,
            'last_contraction_low': None
        }

        if len(rows) < 30:  # Need minimum data
            return empty_result

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
            return empty_result

        # Analyze contractions by dividing period after max high into segments
        remaining_data = len(highs) - max_high_idx
        if remaining_data < 20:
            return empty_result

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
            return empty_result

        # Check for volatility contraction (each range smaller than previous)
        # A true VCP requires continuously tightening contractions;
        # reset the count if a contraction expands beyond the 10% tolerance.
        contraction_count = 1
        for i in range(1, len(contractions)):
            if contractions[i] < contractions[i-1] * 1.1:  # Allow 10% tolerance
                contraction_count += 1
            else:
                contraction_count = 1  # Reset on expansion

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

    def detect_primary_base(self, symbol, end_date, list_date):
        """
        Detect whether an IPO/new issue has formed a proper primary base.

        Minervini's Primary Base Requirements (from "Trade Like a Stock
        Market Wizard"):
        - Recent new issues must form a base before they are buyable.
        - Minimum base duration: 3-5 weeks.
        - Correction thresholds are tiered by duration:
            * 3-week consolidation: max 25% correction
            * 3-5 weeks: max 25-35% (linear scale)
            * Longer bases (up to ~1 year): can correct up to 50%
        - Corrections exceeding these limits are unreliable.

        Args:
            symbol: Stock symbol
            end_date: Date to analyze (YYYY-MM-DD string)
            list_date: Stock's listing/IPO date (date object or None)

        Returns:
            dict with primary base metrics:
            - is_new_issue: True if stock listed within last 2 years
            - has_primary_base: True/False/None (None = N/A)
            - primary_base_weeks: Duration of base in weeks
            - primary_base_correction_pct: Max correction depth %
            - primary_base_status: N/A / TOO_EARLY / FORMING / COMPLETE / FAILED
            - days_since_ipo: Calendar days since listing
        """
        empty_result = {
            'is_new_issue': False,
            'has_primary_base': None,
            'primary_base_weeks': None,
            'primary_base_correction_pct': None,
            'primary_base_status': 'N/A',
            'days_since_ipo': None,
        }

        if not list_date:
            return empty_result

        # Parse dates
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        if isinstance(list_date, str):
            list_date_obj = datetime.strptime(list_date, "%Y-%m-%d")
        else:
            # date or datetime object
            list_date_obj = datetime(list_date.year, list_date.month, list_date.day)

        days_since_ipo = (end_date_obj - list_date_obj).days

        # Only consider stocks listed within the last 2 years as "new issues"
        if days_since_ipo > 730:
            return empty_result

        result = {
            'is_new_issue': True,
            'has_primary_base': None,
            'primary_base_weeks': None,
            'primary_base_correction_pct': None,
            'primary_base_status': 'TOO_EARLY',
            'days_since_ipo': days_since_ipo,
        }

        # Need at least ~3 weeks of calendar days to even check
        if days_since_ipo < 15:
            return result

        # Get all price data since listing
        cursor = self.conn.cursor()
        list_date_str = list_date_obj.strftime("%Y-%m-%d")

        cursor.execute("""
            SELECT date, high, low, close FROM stock_prices
            WHERE symbol = %s AND date >= %s AND date <= %s
            ORDER BY date ASC
        """, (symbol, list_date_str, end_date))

        rows = cursor.fetchall()
        cursor.close()

        # Need at least ~3 weeks of trading days (~15 days)
        if len(rows) < 15:
            return result

        # Extract price series
        dates = [row[0] for row in rows]
        highs = [float(row[1]) for row in rows]
        lows = [float(row[2]) for row in rows]
        closes = [float(row[3]) for row in rows]

        # Find the post-IPO high
        max_high = max(highs)
        max_high_idx = highs.index(max_high)

        # If the high is at the very end, there's no base yet
        if max_high_idx >= len(highs) - 3:
            return result

        # Measure correction from post-IPO high
        post_high_lows = lows[max_high_idx:]
        min_low = min(post_high_lows)
        correction_pct = ((max_high - min_low) / max_high) * 100

        # Base duration: trading days from post-IPO high to current date, in weeks
        base_trading_days = len(rows) - max_high_idx
        base_weeks = base_trading_days / 5.0  # ~5 trading days per week

        result['primary_base_weeks'] = round(base_weeks, 1)
        result['primary_base_correction_pct'] = round(correction_pct, 1)

        # Evaluate base quality based on Minervini's tiered rules
        if base_weeks < 3:
            result['primary_base_status'] = 'TOO_EARLY'
            return result

        # Determine max allowed correction based on duration
        if base_weeks <= 3:
            max_correction = 25.0
        elif base_weeks <= 5:
            # Linear scale: 25% at 3 weeks -> 35% at 5 weeks
            max_correction = 25.0 + (base_weeks - 3.0) * 5.0
        elif base_weeks <= 52:
            # Linear scale: 35% at 5 weeks -> 50% at 52 weeks
            max_correction = 35.0 + (base_weeks - 5.0) / (52.0 - 5.0) * 15.0
        else:
            max_correction = 50.0

        if correction_pct > max_correction:
            # Correction too deep for this timeframe
            result['has_primary_base'] = False
            result['primary_base_status'] = 'FAILED'
            return result

        # Correction is acceptable -- check if base is completing or still forming
        current_price = closes[-1]
        distance_from_high = ((max_high - current_price) / max_high) * 100

        if distance_from_high <= 15:
            # Price has recovered near the high -- base is complete
            result['has_primary_base'] = True
            result['primary_base_status'] = 'COMPLETE'
        else:
            # Still consolidating -- base is forming
            result['has_primary_base'] = False
            result['primary_base_status'] = 'FORMING'

        return result
