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

    # ------------------------------------------------------------------
    # Private helpers for swing-point-based VCP detection
    # ------------------------------------------------------------------

    def _find_swing_points(self, highs, lows, n_bars):
        """
        Find swing highs and swing lows using an N-bar method.

        A swing high is a bar whose high is the highest of the surrounding
        N bars on each side.  A swing low is a bar whose low is the lowest
        of the surrounding N bars on each side.

        Args:
            highs: list[float] daily high prices
            lows:  list[float] daily low prices
            n_bars: number of bars on each side to compare

        Returns:
            list of tuples (index, price, 'high'|'low') sorted by index
        """
        points = []
        for i in range(n_bars, len(highs) - n_bars):
            # Check swing high
            is_swing_high = True
            for j in range(1, n_bars + 1):
                if highs[i] < highs[i - j] or highs[i] < highs[i + j]:
                    is_swing_high = False
                    break
            if is_swing_high:
                points.append((i, highs[i], 'high'))

            # Check swing low
            is_swing_low = True
            for j in range(1, n_bars + 1):
                if lows[i] > lows[i - j] or lows[i] > lows[i + j]:
                    is_swing_low = False
                    break
            if is_swing_low:
                points.append((i, lows[i], 'low'))

        points.sort(key=lambda x: x[0])
        return points

    def _pair_contractions(self, swing_points, volumes, start_idx):
        """
        Walk through swing points after *start_idx* and pair each
        (swing_high, next_swing_low) as a contraction.

        Args:
            swing_points: list of (index, price, type) from _find_swing_points
            volumes: list[int] daily volumes (full VCP window)
            start_idx: index of the peak -- only consider swings after this

        Returns:
            list of dicts, each with keys: range_pct, high, low,
            high_idx, low_idx, avg_volume, duration_bars
        """
        post_peak = [p for p in swing_points if p[0] > start_idx]

        contractions = []
        i = 0
        while i < len(post_peak) - 1:
            if post_peak[i][2] == 'high':
                sh_idx, sh_price, _ = post_peak[i]
                # Find the next swing low after this high
                for j in range(i + 1, len(post_peak)):
                    if post_peak[j][2] == 'low':
                        sl_idx, sl_price, _ = post_peak[j]
                        range_pct = (
                            ((sh_price - sl_price) / sh_price) * 100
                            if sh_price > 0 else 0
                        )
                        vol_slice = volumes[sh_idx:sl_idx + 1]
                        avg_vol = (
                            sum(vol_slice) / len(vol_slice) if vol_slice else 0
                        )
                        contractions.append({
                            'range_pct': range_pct,
                            'high': sh_price,
                            'low': sl_price,
                            'high_idx': sh_idx,
                            'low_idx': sl_idx,
                            'avg_volume': avg_vol,
                            'duration_bars': sl_idx - sh_idx,
                        })
                        i = j + 1  # advance past this low
                        break
                else:
                    i += 1  # no low found after this high
            else:
                i += 1

        return contractions

    def _calculate_local_atr(self, highs, lows, closes, end_idx, period=14):
        """
        Calculate ATR from in-memory OHLCV arrays (no DB query).

        Args:
            highs:   list[float]
            lows:    list[float]
            closes:  list[float]
            end_idx: calculate ATR ending at this bar index
            period:  ATR period (default 14)

        Returns:
            (atr, atr_pct) -- both floats, or (None, None)
        """
        start = max(0, end_idx - period)
        if end_idx - start < 2:
            return None, None

        true_ranges = []
        for i in range(start + 1, end_idx + 1):
            high_low = highs[i] - lows[i]
            high_prev_close = abs(highs[i] - closes[i - 1])
            low_prev_close = abs(lows[i] - closes[i - 1])
            true_ranges.append(max(high_low, high_prev_close, low_prev_close))

        if not true_ranges:
            return None, None

        atr = true_ranges[0]
        for tr in true_ranges[1:]:
            atr = (atr * (period - 1) + tr) / period

        atr_pct = (atr / closes[end_idx]) * 100 if closes[end_idx] > 0 else None
        return atr, atr_pct

    def _validate_uptrend(self, closes, peak_idx, uptrend_bars=90):
        """
        Validate that a meaningful uptrend preceded the peak.

        Measures the percentage gain from *uptrend_bars* bars before the
        peak to the peak itself.

        Args:
            closes: list[float] close prices (full data array)
            peak_idx: index of the consolidation peak
            uptrend_bars: trading bars to look back (default 90 ~ 4.5 months)

        Returns:
            float: percentage gain into the peak, or None if insufficient data
        """
        lookback_idx = max(0, peak_idx - uptrend_bars)
        if lookback_idx >= peak_idx:
            return None

        start_price = closes[lookback_idx]
        peak_price = closes[peak_idx]

        if start_price <= 0:
            return None

        return ((peak_price - start_price) / start_price) * 100

    # ------------------------------------------------------------------
    # Main VCP + cup-and-handle entry point
    # ------------------------------------------------------------------

    def detect_vcp(self, symbol, end_date, lookback_days=120):
        """
        Detect Volatility Contraction Pattern (VCP) and cup-and-handle.

        Uses swing-point-based detection to find natural contractions:
        1. Validate prior uptrend (20%+ gain into peak)
        2. Find swing highs / lows via adaptive N-bar method
        3. Pair swings into contractions and check for tightening
        4. Analyze per-contraction volume dry-up
        5. Compute pivot from the final contraction's swing high
        6. Score the pattern (0-100) with ATR-relative tolerances

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
        # Fetch extra history:
        #   - 180 days for cup-and-handle
        #   - ~130 extra days before lookback for uptrend validation (~90 trading days)
        total_lookback = lookback_days + 180
        start_date = (end_date_obj - timedelta(days=total_lookback)).strftime("%Y-%m-%d")

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

        # --- Cup-and-handle detection (~180-day window from the end) ---
        cup_bar_estimate = int(len(rows) * 180 / total_lookback) if total_lookback > 0 else len(rows)
        cup_bar_estimate = max(cup_bar_estimate, 30)
        cup_offset = max(0, len(rows) - cup_bar_estimate)
        cup_handle = self.detect_cup_and_handle(
            highs[cup_offset:], lows[cup_offset:], closes[cup_offset:],
            volumes[cup_offset:], dates[cup_offset:]
        )

        # --- VCP detection (lookback_days window from the end) ---------
        trading_days_ratio = len(rows) / total_lookback if total_lookback > 0 else 0.7
        vcp_target_bars = int(lookback_days * trading_days_ratio)
        vcp_target_bars = min(vcp_target_bars, len(rows))
        vcp_start = max(0, len(rows) - vcp_target_bars)

        vcp_highs = highs[vcp_start:]
        vcp_lows = lows[vcp_start:]
        vcp_closes = closes[vcp_start:]
        vcp_volumes = volumes[vcp_start:]

        def _early_exit_with_cup():
            """Return empty VCP result merged with any cup-handle data."""
            result = dict(empty_result)
            result.update(cup_handle)
            return result

        if len(vcp_highs) < 20:
            return _early_exit_with_cup()

        # Find the highest high in the lookback window (left wall / peak)
        # Use last occurrence so that double-tops pick the most recent peak
        max_high = max(vcp_highs)
        max_high_local_idx = len(vcp_highs) - 1 - vcp_highs[::-1].index(max_high)

        # Need at least 15 bars after the peak for contractions to form
        remaining_bars = len(vcp_highs) - max_high_local_idx
        if remaining_bars < 15:
            return _early_exit_with_cup()

        # Map local VCP index to full-array index for uptrend validation
        peak_full_idx = vcp_start + max_high_local_idx

        # --- Step 1: Validate prior uptrend ---
        uptrend_gain = self._validate_uptrend(closes, peak_full_idx, uptrend_bars=90)

        if uptrend_gain is not None and uptrend_gain < 20:
            # No meaningful uptrend -- sideways chop, not a VCP
            return _early_exit_with_cup()

        # --- Step 1b: Validate base depth (correction from peak) ---
        # Minervini looks for bases with 10-50% correction from the peak.
        # < 10% is just noise (not a real base), > 50% is a broken stock.
        post_peak_lows = vcp_lows[max_high_local_idx:]
        if post_peak_lows:
            deepest_low = min(post_peak_lows)
            base_depth_pct = ((max_high - deepest_low) / max_high) * 100
            if base_depth_pct < 10 or base_depth_pct > 50:
                return _early_exit_with_cup()

        # --- Step 2: Swing-point detection (adaptive N) ---
        if remaining_bars < 40:
            n_bars = 3
        elif remaining_bars < 80:
            n_bars = 4
        else:
            n_bars = 5

        swing_points = self._find_swing_points(vcp_highs, vcp_lows, n_bars)

        # --- Step 3: Pair swings into contractions ---
        contractions = self._pair_contractions(
            swing_points, vcp_volumes, max_high_local_idx
        )

        if len(contractions) < 2:
            return _early_exit_with_cup()

        # --- Step 4: Tightening and higher lows ---
        # ATR at the peak for volatility-relative tolerance
        atr, atr_pct = self._calculate_local_atr(
            vcp_highs, vcp_lows, vcp_closes, max_high_local_idx
        )
        atr_tolerance = min(atr_pct * 0.5, 1.5) if atr_pct else 1.0  # fallback

        # Count consecutive tightening from the end (recent matters most)
        consecutive_tightening = 1
        for i in range(len(contractions) - 1, 0, -1):
            if contractions[i]['range_pct'] <= contractions[i - 1]['range_pct'] + atr_tolerance:
                consecutive_tightening += 1
            else:
                break

        # Descending highs -- each contraction's swing high should be
        # lower than (or approximately equal to) the previous one.
        descending_high_count = 0
        for i in range(1, len(contractions)):
            if contractions[i]['high'] <= contractions[i - 1]['high'] * 1.02:
                descending_high_count += 1
        descending_high_ratio = (
            descending_high_count / (len(contractions) - 1)
            if len(contractions) > 1 else 0
        )

        # Higher lows -- graduated ratio
        higher_low_count = 0
        for i in range(1, len(contractions)):
            if contractions[i]['low'] >= contractions[i - 1]['low'] * 0.98:
                higher_low_count += 1
        higher_low_ratio = (
            higher_low_count / (len(contractions) - 1)
            if len(contractions) > 1 else 0
        )

        # --- Step 5: Per-contraction volume analysis ---
        first_vol = contractions[0]['avg_volume']
        last_vol = contractions[-1]['avg_volume']
        volume_dryup_ratio = last_vol / first_vol if first_vol > 0 else 1.0

        # Binary flag for backward compatibility
        volume_contraction = volume_dryup_ratio < 0.70

        # --- Step 6: Pivot price ---
        # Use the swing high of the final contraction
        pivot_price = contractions[-1]['high']
        current_price = vcp_closes[-1]

        # If price has already broken above, use the highest recent swing high
        if current_price > pivot_price:
            recent_swing_highs = [
                p for p in swing_points
                if p[2] == 'high' and p[0] > max_high_local_idx
            ]
            if recent_swing_highs:
                pivot_price = max(sh[1] for sh in recent_swing_highs)

        # Last contraction metrics (for stop loss and downstream consumers)
        last_contraction_low = contractions[-1]['low']
        latest_contraction_pct = contractions[-1]['range_pct']

        # --- Step 7: Scoring (0-100) ---
        vcp_score = 0

        # Prior uptrend quality (10 pts max)
        if uptrend_gain is not None:
            if uptrend_gain >= 30:
                vcp_score += 10
            elif uptrend_gain >= 20:
                vcp_score += 5

        # Contraction count (15 pts max) -- 5 per contraction
        vcp_score += min(len(contractions) * 5, 15)

        # Tightening quality (20 pts max)
        tightening_ratio = (
            consecutive_tightening / len(contractions) if contractions else 0
        )
        vcp_score += int(tightening_ratio * 20)

        # Final contraction tightness (15 pts max) -- ATR-relative
        if atr_pct and atr_pct > 0:
            atr_multiple = latest_contraction_pct / atr_pct
            if atr_multiple <= 1.0:
                vcp_score += 15   # Extremely tight
            elif atr_multiple <= 2.0:
                vcp_score += 12
            elif atr_multiple <= 3.0:
                vcp_score += 8
            elif atr_multiple <= 5.0:
                vcp_score += 4
            else:
                vcp_score += 1
        else:
            # Fallback: absolute tightness thresholds
            if latest_contraction_pct <= 5:
                vcp_score += 15
            elif latest_contraction_pct <= 10:
                vcp_score += 12
            elif latest_contraction_pct <= 15:
                vcp_score += 8
            elif latest_contraction_pct <= 20:
                vcp_score += 4
            else:
                vcp_score += 1

        # Higher lows (10 pts max) -- graduated
        vcp_score += int(higher_low_ratio * 10)

        # Descending highs (10 pts max) -- each contraction high <= previous
        vcp_score += int(descending_high_ratio * 10)

        # Volume dry-up (15 pts max) -- graduated
        if volume_dryup_ratio < 0.50:
            vcp_score += 15
        elif volume_dryup_ratio < 0.70:
            vcp_score += 12
        elif volume_dryup_ratio < 0.85:
            vcp_score += 7
        else:
            vcp_score += 2

        # Price near pivot (10 pts max)
        distance_from_pivot = (
            ((pivot_price - current_price) / pivot_price) * 100
            if pivot_price > 0 else 100
        )
        abs_distance = abs(distance_from_pivot)

        if distance_from_pivot >= 0:
            # Below pivot -- reward proximity
            if abs_distance <= 3:
                vcp_score += 10
            elif abs_distance <= 5:
                vcp_score += 7
            elif abs_distance <= 10:
                vcp_score += 4
        else:
            # Above pivot -- breakout zone, taper off
            if abs_distance <= 3:
                vcp_score += 8
            elif abs_distance <= 5:
                vcp_score += 5
            else:
                vcp_score += 0  # Too extended

        # --- Cup-and-handle integration (same as before) ---------------
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

        # Detection threshold: score >= 50 and at least 2 contractions
        contraction_count = len(contractions)
        vcp_detected = vcp_score >= 50 and contraction_count >= 2

        if vcp_detected and pattern_type is None:
            pattern_type = 'VCP_ONLY'

        vcp_score = min(vcp_score, 100)

        return {
            'vcp_detected': vcp_detected,
            'vcp_score': vcp_score,
            'contraction_count': contraction_count,
            'latest_contraction_pct': (
                round(latest_contraction_pct, 2)
                if latest_contraction_pct is not None else None
            ),
            'volume_contraction': volume_contraction,
            'pivot_price': (
                round(pivot_price, 2)
                if pivot_price is not None else None
            ),
            'last_contraction_low': (
                round(last_contraction_low, 2)
                if last_contraction_low is not None else None
            ),
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

        left_lip_idx, left_lip_high = max(enumerate(highs[:search_end]), key=lambda x: x[1])

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
                                # Volume dry-up confirms VCP in handle
                                handle_has_vcp = True

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
