"""
Post-Correction Rally Leaders Screener

Identifies the true institutional leaders emerging from a market correction --
the stocks Mark Minervini would focus on when a big rally day follows weeks
of market downturn.

The thesis: stocks that HELD UP during the decline (relative strength) and
then SURGE on the rally day with heavy volume are where institutions are
deploying capital first. These are the potential superperformers of the
next leg up.

Designed to run AFTER the daily pipeline (minervini.py + fetch_macd.py)
has already populated minervini_metrics. This module computes additional
rally-specific metrics from stock_prices and ranks the results.

Reference: "Trade Like a Stock Market Wizard" by Mark Minervini,
           Ch. 3 (Leader Profile), Ch. 7 (Relative Strength),
           Ch. 11 (Market Direction & Timing)
"""

import psycopg2
from datetime import datetime, timedelta

from .database import DatabaseManager


# =====================================================================
# HARD PARAMETERS -- Post-Correction Rally Leader Criteria
# =====================================================================
#
# Minervini's framework applied to the specific scenario of a big rally
# day after a multi-week market correction. These are non-negotiable
# gates that a stock must clear, plus scoring factors for ranking.

# --- GATE 1: Rally Day Performance ---
# The stock must have participated meaningfully in the rally.
# A 4%+ move on 1.5x+ volume signals institutional conviction, not
# retail noise. Closing in the upper portion of the day's range shows
# buyers held control into the close.
RALLY_MIN_DAY_CHANGE_PCT = 4.0
RALLY_MIN_VOLUME_RATIO = 1.5
RALLY_MIN_CLOSE_POSITION = 0.60  # close in upper 40% of day's range

# --- GATE 2: Survived the Downturn ---
# The stock held its structural trend through weeks of selling.
# Stage 1 (basing) or 2 (advancing) means it didn't break down.
# RS >= 70 means it outperformed 70% of stocks during the decline.
# Price near/above 200-day MA means the long-term trend is intact.
RALLY_ALLOWED_STAGES = (1, 2)
RALLY_MIN_RS = 70
RALLY_MAX_PCT_BELOW_200MA = 3.0  # price can be up to 3% below 200 MA

# --- GATE 3: Structural Integrity ---
# The MA stack and base structure weren't destroyed by the correction.
# 150 MA still above 200 MA = long-term trend order intact.
# Not more than 35% from 52-week high = stock not broken.
# At least 6/9 Minervini criteria still holding.
RALLY_MIN_CRITERIA_PASSED = 6
RALLY_MAX_PCT_FROM_HIGH = -35.0  # percent_from_52w_high >= -35
RALLY_REQUIRE_150_ABOVE_200 = True

# --- GATE 4: Tradability ---
# Minervini insists on institutional-quality names with liquidity.
RALLY_MIN_DOLLAR_VOLUME = 5_000_000
RALLY_MIN_PRICE = 5.00
RALLY_MIN_MARKET_CAP = 300_000_000

# --- POWER SCORE WEIGHTS (0-100 composite) ---
# How the final ranking score is built from each factor.
SCORE_WEIGHTS = {
    'rally_strength': 25,    # Day change magnitude + volume surge
    'relative_strength': 20, # RS rating (higher = better)
    'base_tightness': 15,    # How close to 52w high (survived better)
    'vcp_quality': 10,       # Pattern formation quality
    'earnings_quality': 10,  # Fundamental backing
    'volume_surge': 10,      # Extra credit for massive volume
    'sector_strength': 5,    # Industry RS leadership
    'bonus_flags': 5,        # New 52w high, MACD bullish, etc.
}


class RallyDayScreener:
    """
    Screens for post-correction rally leaders using Minervini's methodology.

    Operates on data already in PostgreSQL (minervini_metrics + stock_prices).
    Computes rally-day-specific metrics, applies hard gates, scores, and ranks.
    """

    def __init__(self, conn=None):
        if conn:
            self.conn = conn
            self._owns_conn = False
        else:
            self.db = DatabaseManager()
            self.conn = self.db.conn
            self._owns_conn = True

    def close(self):
        if self._owns_conn:
            self.db.close()

    # ------------------------------------------------------------------
    # Rally-specific metrics (computed from stock_prices)
    # ------------------------------------------------------------------

    def _get_rally_day_metrics(self, date):
        """
        Compute rally-day performance for all stocks: % change from
        prior close, close position in day's range, and raw volume.

        Returns dict keyed by symbol.
        """
        cursor = self.conn.cursor()

        cursor.execute("""
            WITH today AS (
                SELECT symbol, open, high, low, close, volume
                FROM stock_prices
                WHERE date = %s
            ),
            prev AS (
                SELECT DISTINCT ON (symbol)
                    symbol, close AS prev_close
                FROM stock_prices
                WHERE date < %s
                ORDER BY symbol, date DESC
            )
            SELECT
                t.symbol,
                t.close,
                t.open,
                t.high,
                t.low,
                t.volume,
                p.prev_close,
                CASE WHEN p.prev_close > 0
                     THEN ((t.close - p.prev_close) / p.prev_close) * 100
                     ELSE NULL END AS day_change_pct,
                CASE WHEN (t.high - t.low) > 0
                     THEN (t.close - t.low) / (t.high - t.low)
                     ELSE 0.5 END AS close_position
            FROM today t
            JOIN prev p ON t.symbol = p.symbol
        """, (date, date))

        results = {}
        for row in cursor.fetchall():
            results[row[0]] = {
                'close': float(row[1]),
                'open': float(row[2]),
                'high': float(row[3]),
                'low': float(row[4]),
                'volume': int(row[5]),
                'prev_close': float(row[6]),
                'day_change_pct': float(row[7]) if row[7] is not None else None,
                'close_position': float(row[8]),
            }

        cursor.close()
        return results

    def _get_market_rally_stats(self, date):
        """
        Get the broad market's rally day performance (SPY) for context.
        Used to compute relative rally strength.
        """
        cursor = self.conn.cursor()

        cursor.execute("""
            WITH today AS (
                SELECT close FROM stock_prices
                WHERE symbol = 'SPY' AND date = %s
            ),
            prev AS (
                SELECT close FROM stock_prices
                WHERE symbol = 'SPY' AND date < %s
                ORDER BY date DESC LIMIT 1
            )
            SELECT
                ((t.close - p.close) / p.close) * 100 AS spy_change_pct
            FROM today t, prev p
        """, (date, date))

        result = cursor.fetchone()
        cursor.close()

        return float(result[0]) if result else 0.0

    def _get_drawdown_vs_market(self, date, lookback_days=30):
        """
        For each stock, compare its drawdown over the correction period
        against SPY's drawdown. Stocks that fell LESS than the market
        have superior relative strength through the decline.

        Returns dict: {symbol: {'stock_drawdown': float, 'market_drawdown': float,
                                'relative_drawdown': float}}
        """
        cursor = self.conn.cursor()

        start_date = (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

        # Market drawdown over the lookback period
        cursor.execute("""
            SELECT MAX(close), MIN(close) FROM stock_prices
            WHERE symbol = 'SPY' AND date BETWEEN %s AND %s
        """, (start_date, date))
        spy_row = cursor.fetchone()
        if spy_row and spy_row[0] and spy_row[1]:
            spy_max = float(spy_row[0])
            spy_min = float(spy_row[1])
            market_drawdown = ((spy_min - spy_max) / spy_max) * 100
        else:
            market_drawdown = 0.0

        # Stock drawdowns: peak-to-trough over the same period
        cursor.execute("""
            SELECT symbol, MAX(high), MIN(low)
            FROM stock_prices
            WHERE date BETWEEN %s AND %s
            GROUP BY symbol
        """, (start_date, date))

        results = {}
        for row in cursor.fetchall():
            symbol = row[0]
            peak = float(row[1]) if row[1] else 0
            trough = float(row[2]) if row[2] else 0

            if peak > 0:
                stock_dd = ((trough - peak) / peak) * 100
            else:
                stock_dd = 0.0

            results[symbol] = {
                'stock_drawdown': round(stock_dd, 2),
                'market_drawdown': round(market_drawdown, 2),
                'relative_drawdown': round(stock_dd - market_drawdown, 2),
            }

        cursor.close()
        return results

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def _calculate_power_score(self, metrics, rally, drawdown_data):
        """
        Calculate the Rally Leader Power Score (0-100).

        Combines rally day strength, relative strength persistence,
        base integrity, pattern quality, and fundamentals into a single
        composite score for ranking.
        """
        score = 0.0

        # --- Rally Strength (25 pts) ---
        day_chg = rally.get('day_change_pct', 0) or 0
        vol_ratio = float(metrics.get('volume_ratio') or 0)

        # Scale: 4% = baseline, 10%+ = max credit for the gain portion
        gain_pts = min((day_chg - RALLY_MIN_DAY_CHANGE_PCT) / 6.0, 1.0) * 15
        # Volume portion: 1.5x = baseline, 3x+ = max credit
        vol_pts = min((vol_ratio - RALLY_MIN_VOLUME_RATIO) / 1.5, 1.0) * 10
        score += max(0, gain_pts) + max(0, vol_pts)

        # --- Relative Strength (20 pts) ---
        rs = float(metrics.get('relative_strength') or 0)
        # Scale: 70 = baseline (gate), 90+ = full marks
        rs_pts = min((rs - 70) / 25.0, 1.0) * SCORE_WEIGHTS['relative_strength']
        score += max(0, rs_pts)

        # --- Base Tightness / Proximity to 52w High (15 pts) ---
        pct_from_high = float(metrics.get('percent_from_52w_high') or -50)
        # 0% from high = full marks, -35% = zero
        tight_pts = max(0, (35 + pct_from_high) / 35.0) * SCORE_WEIGHTS['base_tightness']
        score += tight_pts

        # --- VCP Quality (10 pts) ---
        vcp_detected = metrics.get('vcp_detected', False)
        vcp_score = float(metrics.get('vcp_score') or 0)
        if vcp_detected:
            # Scale VCP score (50-100) into 0-10 points
            score += min(vcp_score / 100.0, 1.0) * SCORE_WEIGHTS['vcp_quality']
        elif vcp_score >= 30:
            # Forming but not yet detected -- partial credit
            score += (vcp_score / 100.0) * SCORE_WEIGHTS['vcp_quality'] * 0.5

        # --- Earnings Quality (10 pts) ---
        eq_score = float(metrics.get('earnings_quality_score') or 0)
        eps_yoy = metrics.get('eps_growth_yoy')
        passes_earnings = metrics.get('passes_earnings')

        earnings_pts = 0
        if passes_earnings:
            earnings_pts += 5
        if eps_yoy is not None and float(eps_yoy) >= 25:
            earnings_pts += 3
        if eq_score >= 50:
            earnings_pts += 2
        score += min(earnings_pts, SCORE_WEIGHTS['earnings_quality'])

        # --- Volume Surge (10 pts) ---
        # Extra credit for really explosive volume (beyond the gate)
        if vol_ratio >= 3.0:
            score += SCORE_WEIGHTS['volume_surge']
        elif vol_ratio >= 2.5:
            score += SCORE_WEIGHTS['volume_surge'] * 0.8
        elif vol_ratio >= 2.0:
            score += SCORE_WEIGHTS['volume_surge'] * 0.5

        # --- Sector Strength (5 pts) ---
        ind_rs = metrics.get('industry_rs')
        if ind_rs is not None:
            ind_rs = float(ind_rs)
            if ind_rs >= 70:
                score += SCORE_WEIGHTS['sector_strength']
            elif ind_rs >= 50:
                score += SCORE_WEIGHTS['sector_strength'] * 0.5

        # --- Bonus Flags (5 pts) ---
        bonus = 0
        if metrics.get('is_52w_high'):
            bonus += 2
        if metrics.get('earnings_acceleration'):
            bonus += 1.5

        macd_dv = metrics.get('macd_daily_value')
        macd_ds = metrics.get('macd_daily_signal')
        if macd_dv is not None and macd_ds is not None:
            if float(macd_dv) > 0 and float(macd_dv) > float(macd_ds):
                bonus += 1.5

        score += min(bonus, SCORE_WEIGHTS['bonus_flags'])

        # --- Relative Drawdown Bonus (up to 5 pts extra) ---
        if drawdown_data:
            rel_dd = drawdown_data.get('relative_drawdown', 0)
            # Positive = stock fell less than market
            if rel_dd > 0:
                score += min(rel_dd / 10.0, 1.0) * 5

        return round(min(score, 100), 1)

    # ------------------------------------------------------------------
    # Main screening pipeline
    # ------------------------------------------------------------------

    def screen(self, date=None, correction_lookback_days=30, limit=50):
        """
        Run the full rally leader screening pipeline.

        Args:
            date: Analysis date (YYYY-MM-DD). Defaults to latest in stock_prices.
            correction_lookback_days: How many calendar days back to measure
                                     the correction drawdown (default 30).
            limit: Max results to return.

        Returns:
            list of dicts, each representing a rally leader candidate,
            sorted by power_score descending.
        """
        cursor = self.conn.cursor()

        # Resolve date
        if not date:
            cursor.execute("SELECT MAX(date) FROM stock_prices")
            result = cursor.fetchone()
            if not result or not result[0]:
                cursor.close()
                return []
            date = result[0].strftime("%Y-%m-%d")

        print(f"\n{'=' * 80}")
        print(f"POST-CORRECTION RALLY LEADERS SCREENER")
        print(f"{'=' * 80}")
        print(f"Date: {date}")
        print(f"Correction lookback: {correction_lookback_days} days")

        # Step 1: Get rally day metrics from stock_prices
        print("\nComputing rally day metrics...")
        rally_metrics = self._get_rally_day_metrics(date)
        print(f"  {len(rally_metrics)} stocks with price data")

        # Market context
        spy_change = self._get_market_rally_stats(date)
        print(f"  SPY change: {spy_change:+.2f}%")

        # Step 2: Get drawdown comparison
        print("Computing drawdown vs market...")
        drawdown_data = self._get_drawdown_vs_market(date, correction_lookback_days)

        # Step 3: Get all minervini_metrics for the date
        print("Loading Minervini metrics...")
        cursor.execute("""
            SELECT mm.symbol, mm.close_price, mm.ma_50, mm.ma_150, mm.ma_200,
                   mm.relative_strength, mm.stage, mm.passes_minervini,
                   mm.criteria_passed, mm.criteria_failed,
                   mm.percent_from_52w_high, mm.percent_from_52w_low,
                   mm.vcp_detected, mm.vcp_score, mm.vcp_breakout_confirmed,
                   mm.pivot_price, mm.volume_ratio, mm.avg_dollar_volume,
                   mm.return_1m, mm.return_3m, mm.return_6m, mm.return_12m,
                   mm.industry_rs, mm.is_52w_high,
                   mm.eps_growth_yoy, mm.earnings_acceleration,
                   mm.earnings_quality_score, mm.passes_earnings,
                   mm.signal, mm.signal_reasons,
                   mm.entry_low, mm.entry_high, mm.stop_loss,
                   mm.sell_target_primary, mm.risk_reward_ratio, mm.risk_percent,
                   mm.atr_percent,
                   mm.macd_daily_value, mm.macd_daily_signal,
                   mm.macd_weekly_value, mm.macd_weekly_signal,
                   mm.has_upcoming_earnings, mm.days_until_earnings,
                   mm.pattern_type,
                   td.market_cap, td.name
            FROM minervini_metrics mm
            LEFT JOIN ticker_details td ON mm.symbol = td.symbol
            WHERE mm.date = %s
        """, (date,))

        columns = [desc[0] for desc in cursor.description]
        all_metrics = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()

        print(f"  {len(all_metrics)} stocks in minervini_metrics")

        # Step 4: Apply gates
        print("\nApplying hard gates...")
        candidates = []
        gate_stats = {
            'no_rally_data': 0,
            'weak_rally': 0,
            'low_volume': 0,
            'weak_close': 0,
            'wrong_stage': 0,
            'low_rs': 0,
            'below_200ma': 0,
            'broken_ma_stack': 0,
            'too_far_from_high': 0,
            'low_criteria': 0,
            'low_dollar_volume': 0,
            'low_price': 0,
            'small_cap': 0,
        }

        for m in all_metrics:
            symbol = m['symbol']
            rally = rally_metrics.get(symbol)

            # GATE 1: Rally Day Performance
            if not rally or rally['day_change_pct'] is None:
                gate_stats['no_rally_data'] += 1
                continue

            if rally['day_change_pct'] < RALLY_MIN_DAY_CHANGE_PCT:
                gate_stats['weak_rally'] += 1
                continue

            vol_ratio = float(m['volume_ratio']) if m['volume_ratio'] else 0
            if vol_ratio < RALLY_MIN_VOLUME_RATIO:
                gate_stats['low_volume'] += 1
                continue

            if rally['close_position'] < RALLY_MIN_CLOSE_POSITION:
                gate_stats['weak_close'] += 1
                continue

            # GATE 2: Survived the Downturn
            stage = m.get('stage')
            if stage not in RALLY_ALLOWED_STAGES:
                gate_stats['wrong_stage'] += 1
                continue

            rs = float(m['relative_strength']) if m['relative_strength'] else 0
            if rs < RALLY_MIN_RS:
                gate_stats['low_rs'] += 1
                continue

            close_price = float(m['close_price']) if m['close_price'] else 0
            ma_200 = float(m['ma_200']) if m['ma_200'] else 0
            if ma_200 > 0:
                pct_below_200 = ((ma_200 - close_price) / ma_200) * 100
                if pct_below_200 > RALLY_MAX_PCT_BELOW_200MA:
                    gate_stats['below_200ma'] += 1
                    continue

            # GATE 3: Structural Integrity
            ma_150 = float(m['ma_150']) if m['ma_150'] else 0
            if RALLY_REQUIRE_150_ABOVE_200 and ma_200 > 0 and ma_150 <= ma_200:
                gate_stats['broken_ma_stack'] += 1
                continue

            pct_high = float(m['percent_from_52w_high']) if m['percent_from_52w_high'] else -100
            if pct_high < RALLY_MAX_PCT_FROM_HIGH:
                gate_stats['too_far_from_high'] += 1
                continue

            criteria = m.get('criteria_passed') or 0
            if criteria < RALLY_MIN_CRITERIA_PASSED:
                gate_stats['low_criteria'] += 1
                continue

            # GATE 4: Tradability
            adv = m.get('avg_dollar_volume') or 0
            if adv < RALLY_MIN_DOLLAR_VOLUME:
                gate_stats['low_dollar_volume'] += 1
                continue

            if close_price < RALLY_MIN_PRICE:
                gate_stats['low_price'] += 1
                continue

            mcap = m.get('market_cap') or 0
            if mcap < RALLY_MIN_MARKET_CAP:
                gate_stats['small_cap'] += 1
                continue

            # --- PASSED ALL GATES ---
            dd = drawdown_data.get(symbol, {})
            power_score = self._calculate_power_score(m, rally, dd)

            candidates.append({
                'symbol': symbol,
                'name': m.get('name') or '',
                'close_price': close_price,
                'day_change_pct': round(rally['day_change_pct'], 2),
                'close_position': round(rally['close_position'], 2),
                'volume_ratio': vol_ratio,
                'relative_strength': rs,
                'stage': stage,
                'criteria_passed': criteria,
                'percent_from_52w_high': pct_high,
                'passes_minervini': m.get('passes_minervini', False),
                'vcp_detected': m.get('vcp_detected', False),
                'vcp_score': float(m.get('vcp_score') or 0),
                'pattern_type': m.get('pattern_type'),
                'signal': m.get('signal'),
                'entry_low': m.get('entry_low'),
                'entry_high': m.get('entry_high'),
                'stop_loss': m.get('stop_loss'),
                'sell_target_primary': m.get('sell_target_primary'),
                'risk_reward_ratio': m.get('risk_reward_ratio'),
                'risk_percent': m.get('risk_percent'),
                'eps_growth_yoy': m.get('eps_growth_yoy'),
                'earnings_acceleration': m.get('earnings_acceleration'),
                'passes_earnings': m.get('passes_earnings'),
                'earnings_quality_score': m.get('earnings_quality_score'),
                'industry_rs': m.get('industry_rs'),
                'is_52w_high': m.get('is_52w_high', False),
                'has_upcoming_earnings': m.get('has_upcoming_earnings', False),
                'days_until_earnings': m.get('days_until_earnings'),
                'market_cap': mcap,
                'avg_dollar_volume': adv,
                'atr_percent': m.get('atr_percent'),
                'return_1m': m.get('return_1m'),
                'return_3m': m.get('return_3m'),
                'stock_drawdown': dd.get('stock_drawdown'),
                'market_drawdown': dd.get('market_drawdown'),
                'relative_drawdown': dd.get('relative_drawdown'),
                'spy_change': spy_change,
                'power_score': power_score,
            })

        # Sort by power score
        candidates.sort(key=lambda x: x['power_score'], reverse=True)

        # Print gate statistics
        total_filtered = sum(gate_stats.values())
        print(f"\n  Gate Results:")
        print(f"    Total stocks evaluated: {len(all_metrics)}")
        print(f"    Filtered out: {total_filtered}")
        print(f"    Rally Leaders found: {len(candidates)}")

        if any(gate_stats.values()):
            print(f"\n  Filter Breakdown:")
            gate_labels = {
                'no_rally_data': 'No rally day price data',
                'weak_rally': f'Day change < {RALLY_MIN_DAY_CHANGE_PCT}%',
                'low_volume': f'Volume ratio < {RALLY_MIN_VOLUME_RATIO}x',
                'weak_close': f'Close position < {RALLY_MIN_CLOSE_POSITION:.0%}',
                'wrong_stage': f'Stage not in {RALLY_ALLOWED_STAGES}',
                'low_rs': f'RS < {RALLY_MIN_RS}',
                'below_200ma': f'Price > {RALLY_MAX_PCT_BELOW_200MA}% below 200 MA',
                'broken_ma_stack': '150 MA below 200 MA',
                'too_far_from_high': f'> {abs(RALLY_MAX_PCT_FROM_HIGH)}% from 52w high',
                'low_criteria': f'< {RALLY_MIN_CRITERIA_PASSED}/9 criteria',
                'low_dollar_volume': f'Dollar volume < ${RALLY_MIN_DOLLAR_VOLUME/1e6:.0f}M',
                'low_price': f'Price < ${RALLY_MIN_PRICE:.2f}',
                'small_cap': f'Market cap < ${RALLY_MIN_MARKET_CAP/1e6:.0f}M',
            }
            for key, count in sorted(gate_stats.items(), key=lambda x: -x[1]):
                if count > 0:
                    print(f"    {gate_labels.get(key, key)}: {count}")

        return candidates[:limit]


def print_rally_leaders(candidates, spy_change=None):
    """Pretty-print the rally leader results to console."""
    if not candidates:
        print("\nNo rally leaders found matching all criteria.")
        print("This could mean:")
        print("  - Today wasn't a strong enough rally day (need 4%+ movers)")
        print("  - Few stocks maintained RS >= 70 through the correction")
        print("  - Volume wasn't institutional quality (need 1.5x+)")
        return

    if spy_change is not None:
        print(f"\nMarket Context: SPY {spy_change:+.2f}% on the day")

    print(f"\n{'=' * 170}")
    print(f"TOP RALLY LEADERS -- Post-Correction Breakout Candidates")
    print(f"{'=' * 170}")

    # Header
    print(f"\n{'Rank':<5} {'Sym':<7} {'Score':>5} {'Signal':<6} "
          f"{'Price':>9} {'Chg%':>6} {'Vol':>5} {'RS':>4} {'Stg':>3} "
          f"{'%High':>6} {'VCP':>4} "
          f"{'Entry Range':>17} {'Stop':>9} {'Target':>9} {'R:R':>5} "
          f"{'EPSyy':>7} {'IndRS':>5} {'$Vol(M)':>8}")
    print("-" * 170)

    for i, c in enumerate(candidates, 1):
        # Signal formatting
        sig = c.get('signal', 'PASS')
        sig_icon = {'BUY': '🟢', 'WAIT': '🟡', 'PASS': '🔴'}.get(sig, '⚪')

        vcp_str = f"{c['vcp_score']:.0f}" if c['vcp_detected'] else "-"

        if c.get('entry_low') and c.get('entry_high'):
            entry_str = f"${float(c['entry_low']):.2f}-${float(c['entry_high']):.2f}"
        else:
            entry_str = "-"

        stop_str = f"${float(c['stop_loss']):.2f}" if c.get('stop_loss') else "-"
        tgt_str = f"${float(c['sell_target_primary']):.2f}" if c.get('sell_target_primary') else "-"
        rr_str = f"{float(c['risk_reward_ratio']):.1f}" if c.get('risk_reward_ratio') else "-"

        eps_yoy = c.get('eps_growth_yoy')
        eps_str = f"{float(eps_yoy):+.0f}%" if eps_yoy is not None else "-"
        if c.get('earnings_acceleration'):
            eps_str += "🚀"

        ind_rs = c.get('industry_rs')
        ind_str = f"{float(ind_rs):.0f}" if ind_rs is not None else "-"

        adv_str = f"{c['avg_dollar_volume']/1e6:.1f}" if c['avg_dollar_volume'] else "-"

        flags = ""
        if c.get('is_52w_high'):
            flags += "⬆"
        if c.get('has_upcoming_earnings'):
            flags += "📅"
        if c.get('passes_minervini'):
            flags += "✓"

        print(f"{i:<5} {c['symbol']:<7} {c['power_score']:>5.1f} "
              f"{sig_icon}{sig:<5} "
              f"${c['close_price']:>8.2f} {c['day_change_pct']:>+5.1f}% "
              f"{c['volume_ratio']:>4.1f}x {c['relative_strength']:>4.0f} "
              f"{c['stage']:>3} "
              f"{c['percent_from_52w_high']:>+5.1f}% {vcp_str:>4} "
              f"{entry_str:>17} {stop_str:>9} {tgt_str:>9} {rr_str:>5} "
              f"{eps_str:>7} {ind_str:>5} {adv_str:>8} {flags}")

    # Drawdown context for top stocks
    print(f"\n{'-' * 170}")
    print("\nCORRECTION RESILIENCE (Top Stocks):")
    print("-" * 60)

    for c in candidates[:15]:
        stock_dd = c.get('stock_drawdown')
        mkt_dd = c.get('market_drawdown')
        rel_dd = c.get('relative_drawdown')

        if stock_dd is not None:
            rel_str = f"{rel_dd:+.1f}%" if rel_dd is not None else "N/A"
            print(f"  {c['symbol']:<7} Stock max drawdown: {stock_dd:+.1f}% | "
                  f"Market: {mkt_dd:+.1f}% | "
                  f"Relative: {rel_str} "
                  f"{'(OUTPERFORMED)' if rel_dd and rel_dd > 0 else ''}")

    print(f"\n{'-' * 170}")
    print("Legend: Score=Rally Leader Power Score, Chg%=Today's change, "
          "Vol=Volume ratio vs 50-day avg,")
    print("        RS=Relative Strength, Stg=Stage, %High=Distance from 52w high, "
          "VCP=VCP Score,")
    print("        ⬆=New 52w high, 📅=Upcoming earnings, ✓=Passes full Minervini, "
          "🚀=Earnings accelerating")
    print(f"{'=' * 170}")
