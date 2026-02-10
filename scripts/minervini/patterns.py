"""
Chart pattern detection for Minervini analysis.

Implements:
- Volatility Contraction Pattern (VCP) detection
- Cup-and-Handle detection (with VCP integration)
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
        Detect Volatility Contraction Pattern (VCP) and cup-and-handle.

        VCP characteristics:
        - Series of price contractions (usually 2-4)
        - Each contraction is tighter than the previous
        - Volume tends to dry up during contractions
        - Forms higher lows
        - Price consolidates near highs

        Also runs cup-and-handle detection and integrates scoring:
        - Cup+Handle+VCP in handle = premium setup (+20 score)
        - Cup+Handle without VCP = standard cup-handle (+10 score)
        - VCP only = standalone VCP (normal scoring)

        Args:
            symbol: Stock symbol
            end_date: Date to analyze
            lookback_days: How far back to look for the pattern (default 120 days)

        Returns:
            dict with VCP + cup-and-handle metrics
        """
        cursor = self.conn.cursor()

        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        # Fetch extra history for cup-and-handle (needs wider window)
        cup_lookback = max(lookback_days, 180)
        start_date = (end_date_obj - timedelta(days=cup_lookback)).strftime("%Y-%m-%d")

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
            'last_contraction_low': None,
            # Cup-and-handle fields
            'cup_detected': False,
            'cup_depth_pct': None,
            'cup_duration_weeks': None,
            'handle_detected': False,
            'handle_depth_pct': None,
            'handle_duration_weeks': None,
            'handle_has_vcp': False,
            'pattern_type': None,
        }

        if len(rows) < 30:  # Need minimum data
            return empty_result

        # Convert to lists
        dates = [row[0] for row in rows]
        highs = [float(row[1]) for row in rows]
        lows = [float(row[2]) for row in rows]
        closes = [float(row[3]) for row in rows]
        volumes = [int(row[4]) for row in rows]

        # --- Cup-and-handle detection (uses full data window) ----------
        cup_handle = self.detect_cup_and_handle(highs, lows, closes, volumes, dates)

        # --- Standard VCP detection (uses lookback_days window) --------
        # Trim to the original lookback_days for VCP analysis
        if cup_lookback > lookback_days:
            trading_days_per_calendar = len(rows) / cup_lookback if cup_lookback > 0 else 0.7
            target_bars = int(lookback_days * trading_days_per_calendar)
            if target_bars < len(rows):
                offset = len(rows) - target_bars
                vcp_highs = highs[offset:]
                vcp_lows = lows[offset:]
                vcp_closes = closes[offset:]
                vcp_volumes = volumes[offset:]
            else:
                vcp_highs = highs
                vcp_lows = lows
                vcp_closes = closes
                vcp_volumes = volumes
        else:
            vcp_highs = highs
            vcp_lows = lows
            vcp_closes = closes
            vcp_volumes = volumes

        # Find the highest high in the lookback period (potential starting point)
        max_high = max(vcp_highs)
        max_high_idx = vcp_highs.index(max_high)

        # Only analyze if max high is in first 60% of period (needs time to contract)
        if max_high_idx > len(vcp_highs) * 0.6:
            # Even if VCP fails, return cup-handle data if found
            result = dict(empty_result)
            result.update(cup_handle)
            if cup_handle.get('pattern_type') is None and not cup_handle.get('cup_detected'):
                result['pattern_type'] = None
            return result

        # Analyze contractions by dividing period after max high into segments
        remaining_data = len(vcp_highs) - max_high_idx
        if remaining_data < 20:
            result = dict(empty_result)
            result.update(cup_handle)
            if cup_handle.get('pattern_type') is None and not cup_handle.get('cup_detected'):
                result['pattern_type'] = None
            return result

        # Divide into segments to find contractions
        segment_size = remaining_data // 4  # Look for up to 4 contractions
        if segment_size < 5:
            segment_size = 5

        contractions = []
        contraction_lows = []

        for i in range(4):
            start_idx = max_high_idx + (i * segment_size)
            end_idx = min(start_idx + segment_size, len(vcp_highs))

            if start_idx >= len(vcp_highs):
                break

            segment_highs = vcp_highs[start_idx:end_idx]
            segment_lows = vcp_lows[start_idx:end_idx]

            if segment_highs and segment_lows:
                seg_high = max(segment_highs)
                seg_low = min(segment_lows)
                contraction_pct = ((seg_high - seg_low) / seg_high) * 100
                contractions.append(contraction_pct)
                contraction_lows.append(seg_low)

        if len(contractions) < 2:
            result = dict(empty_result)
            result.update(cup_handle)
            if cup_handle.get('pattern_type') is None and not cup_handle.get('cup_detected'):
                result['pattern_type'] = None
            return result

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
        early_volume = vcp_volumes[max_high_idx:max_high_idx + segment_size]
        late_volume = vcp_volumes[-segment_size:]

        avg_early_volume = sum(early_volume) / len(early_volume) if early_volume else 0
        avg_late_volume = sum(late_volume) / len(late_volume) if late_volume else 0

        volume_contraction = avg_late_volume < avg_early_volume * 0.7  # Volume dried up 30%+

        # Calculate latest contraction percentage
        latest_contraction_pct = contractions[-1] if contractions else None

        # Determine pivot price (recent high that acts as resistance)
        recent_highs = vcp_highs[-20:] if len(vcp_highs) >= 20 else vcp_highs
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
        current_price = vcp_closes[-1]
        distance_from_pivot = ((pivot_price - current_price) / pivot_price) * 100
        if distance_from_pivot <= 3:
            vcp_score += 10
        elif distance_from_pivot <= 5:
            vcp_score += 7
        elif distance_from_pivot <= 10:
            vcp_score += 4

        # --- Integrate cup-and-handle scoring --------------------------
        pattern_type = cup_handle.get('pattern_type')

        if cup_handle['cup_detected'] and cup_handle['handle_detected']:
            if cup_handle['handle_has_vcp']:
                vcp_score += 20  # Premium: Cup + Handle + VCP
                pattern_type = 'CUP_HANDLE_VCP'
            else:
                vcp_score += 10  # Standard cup-and-handle
                pattern_type = 'CUP_HANDLE'
        elif cup_handle['cup_detected']:
            vcp_score += 5  # Cup structure found but no proper handle
            # Keep pattern_type as None or VCP_ONLY below

        # Determine if VCP is detected (score >= 50 and at least 2 contractions)
        vcp_detected = vcp_score >= 50 and contraction_count >= 2

        # If VCP is detected but no cup-handle pattern, label as VCP_ONLY
        if vcp_detected and pattern_type is None:
            pattern_type = 'VCP_ONLY'

        # Track the low of the last (tightest) contraction for stop loss placement
        last_contraction_low = contraction_lows[-1] if contraction_lows else None

        # Cap VCP score at 100
        vcp_score = min(vcp_score, 100)

        return {
            'vcp_detected': vcp_detected,
            'vcp_score': vcp_score,
            'contraction_count': contraction_count,
            'latest_contraction_pct': latest_contraction_pct,
            'volume_contraction': volume_contraction,
            'pivot_price': pivot_price,
            'last_contraction_low': last_contraction_low,
            # Cup-and-handle fields
            'cup_detected': cup_handle['cup_detected'],
            'cup_depth_pct': cup_handle['cup_depth_pct'],
            'cup_duration_weeks': cup_handle['cup_duration_weeks'],
            'handle_detected': cup_handle['handle_detected'],
            'handle_depth_pct': cup_handle['handle_depth_pct'],
            'handle_duration_weeks': cup_handle['handle_duration_weeks'],
            'handle_has_vcp': cup_handle['handle_has_vcp'],
            'pattern_type': pattern_type,
        }

    def detect_cup_and_handle(self, highs, lows, closes, volumes, dates):
        """
        Detect cup-and-handle pattern within the provided price data.

        Cup-and-handle (O'Neil / Minervini):
        - Cup: U-shaped correction, 4-65 weeks, 12-33% depth, rounded bottom
        - Handle: short consolidation in upper half of cup, 1-4 weeks, max 20% depth
        - Premium setups have VCP characteristics in the handle

        This method operates on pre-fetched price arrays so that detect_vcp()
        can call it without issuing a second database query.

        Args:
            highs: list[float]   daily high prices, chronological
            lows: list[float]    daily low prices
            closes: list[float]  daily close prices
            volumes: list[int]   daily volumes
            dates: list           date objects

        Returns:
            dict:
            - cup_detected: bool
            - cup_depth_pct: float or None
            - cup_duration_weeks: int or None
            - handle_detected: bool
            - handle_depth_pct: float or None
            - handle_duration_weeks: int or None
            - handle_has_vcp: bool
            - pattern_type: 'CUP_HANDLE_VCP' / 'CUP_HANDLE' / 'VCP_ONLY' / None
        """
        empty = {
            'cup_detected': False,
            'cup_depth_pct': None,
            'cup_duration_weeks': None,
            'handle_detected': False,
            'handle_depth_pct': None,
            'handle_duration_weeks': None,
            'handle_has_vcp': False,
            'pattern_type': None,
        }

        n = len(closes)
        if n < 30:  # ~6 weeks minimum for any cup structure
            return empty

        # --- Step 1: Identify the cup ----------------------------------
        # The "left lip" is the highest high in roughly the first 60% of data.
        search_end = int(n * 0.6)
        if search_end < 10:
            return empty

        left_lip_high = max(highs[:search_end])
        left_lip_idx = highs.index(left_lip_high)

        # Look for the cup bottom: lowest low between the left lip and the
        # point where price recovers close to that high.
        if left_lip_idx >= n - 10:
            return empty

        # Find the minimum low after the left lip
        post_lip_lows = lows[left_lip_idx:]
        cup_bottom = min(post_lip_lows)
        cup_bottom_idx = left_lip_idx + post_lip_lows.index(cup_bottom)

        # Measure cup depth
        cup_depth_pct = ((left_lip_high - cup_bottom) / left_lip_high) * 100

        # Cup depth must be 12-50% (O'Neil/Minervini range)
        if cup_depth_pct < 12 or cup_depth_pct > 50:
            return empty

        # The cup bottom must NOT be at the very end of the data
        if cup_bottom_idx >= n - 5:
            return empty

        # --- Step 2: Check for U-shape (not V-shape) -------------------
        # A rounded bottom has multiple days near the low, whereas a V-shape
        # recovers immediately.  Measure how many bars after the bottom are
        # within 3% of the cup bottom.
        near_bottom_count = 0
        check_range = min(cup_bottom_idx + 20, n)
        for i in range(cup_bottom_idx, check_range):
            if lows[i] <= cup_bottom * 1.03:
                near_bottom_count += 1

        # Require at least 3 bars near bottom for U-shape
        if near_bottom_count < 3:
            return empty

        # --- Step 3: Find the right lip (cup recovery) -----------------
        # Price must recover to within 15% of the left-lip high after the
        # cup bottom to form the right side of the cup.
        right_lip_idx = None
        recovery_threshold = left_lip_high * 0.85

        for i in range(cup_bottom_idx + 1, n):
            if highs[i] >= recovery_threshold:
                right_lip_idx = i
                break

        if right_lip_idx is None:
            return empty

        # Cup duration in trading weeks
        cup_trading_days = right_lip_idx - left_lip_idx
        cup_duration_weeks = round(cup_trading_days / 5)

        # Duration check: 4-65 weeks
        if cup_duration_weeks < 4 or cup_duration_weeks > 65:
            return empty

        # Volume at bottom should be lower than at the left lip (accumulation)
        lip_vol_window = max(1, min(5, right_lip_idx - left_lip_idx))
        bottom_vol_window = max(1, min(5, n - cup_bottom_idx))

        lip_avg_vol = (sum(volumes[left_lip_idx:left_lip_idx + lip_vol_window])
                       / lip_vol_window)
        bottom_avg_vol = (sum(volumes[cup_bottom_idx:cup_bottom_idx + bottom_vol_window])
                          / bottom_vol_window)

        # Optional but informative -- doesn't reject if volume doesn't decline
        cup_volume_declining = bottom_avg_vol < lip_avg_vol * 0.85 if lip_avg_vol > 0 else False

        # --- Step 4: Detect the handle ---------------------------------
        # The handle forms after the right lip; it is a short, shallow pullback.
        handle_detected = False
        handle_depth_pct = None
        handle_duration_weeks = None
        handle_has_vcp = False

        handle_data_start = right_lip_idx
        handle_data = n - handle_data_start

        if handle_data >= 5:  # Need at least ~1 week for a handle
            handle_highs = highs[handle_data_start:]
            handle_lows = lows[handle_data_start:]
            handle_closes = closes[handle_data_start:]
            handle_volumes = volumes[handle_data_start:]

            handle_high = max(handle_highs)
            handle_low = min(handle_lows)
            handle_depth_pct_raw = ((handle_high - handle_low) / handle_high) * 100
            handle_weeks = round(handle_data / 5)

            # Handle must be in upper half of cup range
            cup_range = left_lip_high - cup_bottom
            cup_midpoint = cup_bottom + cup_range * 0.5

            # Validity: shallow (max 20%), short (max 8 weeks), in upper half
            if (handle_depth_pct_raw <= 20 and
                    handle_weeks <= 8 and
                    handle_low >= cup_midpoint):
                handle_detected = True
                handle_depth_pct = round(handle_depth_pct_raw, 2)
                handle_duration_weeks = max(handle_weeks, 1)

                # --- Step 5: Check for VCP in the handle ---------------
                # Divide handle into mini-segments and look for tightening
                if len(handle_highs) >= 10:
                    seg = max(len(handle_highs) // 3, 3)
                    handle_contractions = []

                    for j in range(3):
                        s = j * seg
                        e = min(s + seg, len(handle_highs))
                        if s >= len(handle_highs):
                            break
                        sh = max(handle_highs[s:e])
                        sl = min(handle_lows[s:e])
                        if sh > 0:
                            handle_contractions.append(((sh - sl) / sh) * 100)

                    # VCP in handle: at least 2 segments with tightening
                    if len(handle_contractions) >= 2:
                        tightening = all(
                            handle_contractions[i] <= handle_contractions[i - 1] * 1.1
                            for i in range(1, len(handle_contractions))
                        )
                        if tightening:
                            handle_has_vcp = True

                    # Also check for volume dry-up in handle
                    if len(handle_volumes) >= 6:
                        first_half_vol = sum(handle_volumes[:len(handle_volumes) // 2])
                        second_half_vol = sum(handle_volumes[len(handle_volumes) // 2:])
                        half_len = len(handle_volumes) // 2
                        if half_len > 0:
                            avg_first = first_half_vol / half_len
                            avg_second = second_half_vol / (len(handle_volumes) - half_len)
                            if avg_first > 0 and avg_second < avg_first * 0.75:
                                # Volume drying up supports VCP in handle
                                pass  # handle_has_vcp already set by contraction check

        # --- Determine pattern type ------------------------------------
        cup_detected = True
        if handle_detected and handle_has_vcp:
            pattern_type = 'CUP_HANDLE_VCP'
        elif handle_detected:
            pattern_type = 'CUP_HANDLE'
        else:
            # Cup without a proper handle -- not a complete cup-and-handle
            pattern_type = None
            cup_detected = True  # Cup itself was found

        return {
            'cup_detected': cup_detected,
            'cup_depth_pct': round(cup_depth_pct, 2),
            'cup_duration_weeks': cup_duration_weeks,
            'handle_detected': handle_detected,
            'handle_depth_pct': handle_depth_pct,
            'handle_duration_weeks': handle_duration_weeks,
            'handle_has_vcp': handle_has_vcp,
            'pattern_type': pattern_type,
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
