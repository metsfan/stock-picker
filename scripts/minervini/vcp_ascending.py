"""
Ascending Base VCP Detection

Simplified VCP algorithm that identifies an ascending base pattern:
successive higher highs and higher lows with contracting time and
volume characteristics over the past ~6 months.

Algorithm:
    1. Take the 4 candles with the highest highs and 3 with the lowest lows.
    2. Validate date ordering proving an ascending staircase pattern.
    3. Require minimum spacing, recency, favorable volume, and time contraction.
"""

from datetime import datetime, timedelta


class AscendingBaseDetector:
    """Detects ascending base patterns (simplified VCP)."""

    EMPTY_RESULT = {
        'vcp_detected': False,
        'vcp_score': 0,
        'vcp_breakout_confirmed': False,
        'contraction_count': 0,
        'latest_contraction_pct': None,
        'volume_contraction': False,
        'pivot_price': None,
        'last_contraction_low': None,
        'cup_detected': False,
        'cup_depth_pct': None,
        'cup_duration_weeks': None,
        'handle_detected': False,
        'handle_depth_pct': None,
        'handle_duration_weeks': None,
        'handle_has_vcp': False,
        'pattern_type': None,
    }

    def __init__(self, conn):
        self.conn = conn

    def _fetch_candles(self, symbol, end_date, calendar_days=200):
        """Fetch ~6 months of OHLCV candles ending on *end_date*."""
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        start_date = (end_dt - timedelta(days=calendar_days)).strftime("%Y-%m-%d")

        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT date, open, high, low, close, volume
            FROM stock_prices
            WHERE symbol = %s AND date BETWEEN %s AND %s
            ORDER BY date ASC
        """, (symbol, start_date, end_date))
        rows = cursor.fetchall()
        cursor.close()

        candles = []
        for row in rows:
            if row[2] is None or row[3] is None or row[4] is None or row[5] is None:
                continue
            candles.append({
                'date': row[0],
                'open': float(row[1]) if row[1] is not None else None,
                'high': float(row[2]),
                'low': float(row[3]),
                'close': float(row[4]),
                'volume': int(row[5]),
                'idx': len(candles),
            })
        return candles

    def _is_red(self, candle):
        if candle['open'] is not None:
            return candle['close'] < candle['open']
        return False

    def _score_volume(self, candles, segments):
        """
        Score volume behaviour across the three high-low-high segments.

        Per segment:
          +25  if avg red-candle volume < avg green-candle volume
          +5   if the low-point candle has below-average volume for the segment
        """
        score = 0
        all_pass = True

        for seg_start, seg_low, seg_end in segments:
            start_idx = seg_start['idx']
            end_idx = seg_end['idx']
            seg = [c for c in candles if start_idx <= c['idx'] <= end_idx]
            if not seg:
                continue

            red_vols = [c['volume'] for c in seg if self._is_red(c)]
            green_vols = [c['volume'] for c in seg if not self._is_red(c)]

            avg_red = sum(red_vols) / len(red_vols) if red_vols else 0
            avg_green = sum(green_vols) / len(green_vols) if green_vols else 1

            if avg_red < avg_green:
                score += 25
            else:
                all_pass = False

            seg_avg_vol = sum(c['volume'] for c in seg) / len(seg)
            if seg_low['volume'] < seg_avg_vol:
                score += 5

        return score, all_pass

    def detect_vcp(self, symbol, end_date, lookback_days=126):
        """
        Detect an ascending base pattern in the last ~6 months of price data.

        Returns the same dict structure as PatternDetector.detect_vcp() for
        drop-in compatibility with analyzer / signals / database layers.
        """
        candles = self._fetch_candles(symbol, end_date)
        if len(candles) < 30:
            return dict(self.EMPTY_RESULT)

        # --- Step 1: key points ----------------------------------------
        top_highs = sorted(candles, key=lambda c: c['high'], reverse=True)[:4]
        top_highs.sort(key=lambda c: c['high'], reverse=True)

        bottom_lows = sorted(candles, key=lambda c: c['low'])[:3]
        bottom_lows.sort(key=lambda c: c['low'], reverse=True)

        if len(top_highs) < 4 or len(bottom_lows) < 3:
            return dict(self.EMPTY_RESULT)

        # Reject if any candle appears in both lists (wide-range day)
        if {c['idx'] for c in top_highs} & {c['idx'] for c in bottom_lows}:
            return dict(self.EMPTY_RESULT)

        # --- Step 2: date ordering -------------------------------------
        # highs[0].date > lows[0].date > highs[1].date > lows[1].date
        #   > highs[2].date > lows[2].date > highs[3].date
        chain = [
            top_highs[0], bottom_lows[0], top_highs[1], bottom_lows[1],
            top_highs[2], bottom_lows[2], top_highs[3],
        ]
        for i in range(len(chain) - 1):
            if chain[i]['date'] <= chain[i + 1]['date']:
                return dict(self.EMPTY_RESULT)

        # --- Step 3: minimum spacing (at least 2 candles between) ------
        for i in range(len(chain) - 1):
            if abs(chain[i]['idx'] - chain[i + 1]['idx']) < 3:
                return dict(self.EMPTY_RESULT)

        # --- Step 4: recency -- highs[0] within last 2 weeks ----------
        last_date = candles[-1]['date']
        if (last_date - top_highs[0]['date']).days > 14:
            return dict(self.EMPTY_RESULT)

        # --- Step 5 & 6: volume + time contraction scoring -------------
        # Three chronological segments (earliest first):
        #   Seg 1: highs[3] -> lows[2] -> highs[2]
        #   Seg 2: highs[2] -> lows[1] -> highs[1]
        #   Seg 3: highs[1] -> lows[0] -> highs[0]
        segments = [
            (top_highs[3], bottom_lows[2], top_highs[2]),
            (top_highs[2], bottom_lows[1], top_highs[1]),
            (top_highs[1], bottom_lows[0], top_highs[0]),
        ]

        vcp_score, volume_contraction = self._score_volume(candles, segments)

        # Time contraction: each segment should span fewer candles
        seg_lengths = [seg_end['idx'] - seg_start['idx']
                       for seg_start, _, seg_end in segments]
        if all(seg_lengths[i] > seg_lengths[i + 1]
               for i in range(len(seg_lengths) - 1)):
            vcp_score += 10

        vcp_score = min(vcp_score, 100)

        if vcp_score < 50:
            return dict(self.EMPTY_RESULT)

        # --- Build result ----------------------------------------------
        pivot_price = top_highs[0]['high']
        last_contraction_low = bottom_lows[0]['low']

        last_seg_range = top_highs[0]['high'] - bottom_lows[0]['low']
        latest_contraction_pct = (
            (last_seg_range / top_highs[0]['high']) * 100
            if top_highs[0]['high'] > 0 else None
        )

        # Breakout: current close >= pivot on above-average volume
        vcp_breakout_confirmed = False
        current = candles[-1]
        if current['close'] >= pivot_price:
            recent = candles[-20:] if len(candles) >= 20 else candles
            avg_vol = sum(c['volume'] for c in recent) / len(recent)
            if current['volume'] >= avg_vol * 1.5:
                vcp_breakout_confirmed = True

        return {
            'vcp_detected': True,
            'vcp_score': vcp_score,
            'vcp_breakout_confirmed': vcp_breakout_confirmed,
            'contraction_count': 3,
            'latest_contraction_pct': (
                round(latest_contraction_pct, 2)
                if latest_contraction_pct is not None else None
            ),
            'volume_contraction': volume_contraction,
            'pivot_price': round(pivot_price, 2),
            'last_contraction_low': round(last_contraction_low, 2),
            'cup_detected': False,
            'cup_depth_pct': None,
            'cup_duration_weeks': None,
            'handle_detected': False,
            'handle_depth_pct': None,
            'handle_duration_weeks': None,
            'handle_has_vcp': False,
            'pattern_type': 'ASCENDING_BASE',
        }
