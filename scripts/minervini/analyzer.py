"""
Minervini Stock Analyzer - Orchestrator

Composes all analysis sub-modules (technical indicators, relative strength,
volume, sector, earnings, patterns, signals) and coordinates the full
evaluation pipeline.

Reference: "Trade Like a Stock Market Wizard" by Mark Minervini
"""

import psycopg2

from .database import DatabaseManager
from .technical import TechnicalIndicators
from .relative_strength import RelativeStrengthCalculator
from .volume import VolumeAnalyzer
from .sector import SectorAnalyzer
from .earnings import EarningsAnalyzer
from .patterns import PatternDetector
from .signals import SignalGenerator
from .notifications import NotificationManager


class MinerviniAnalyzer:
    """
    Minervini Stock Analyzer (Enhanced with Company Fundamentals)

    Analyzes stocks based on Mark Minervini's Trend Template Criteria:

    Book Criterion #1: Current price above both 150-day and 200-day MA
    Book Criterion #2: 150-day MA above 200-day MA
    Book Criterion #3: 200-day MA trending upward for at least 1 month
    Book Criterion #4: 50-day MA above both 150-day and 200-day MA
    Book Criterion #5: Current price above 50-day MA
    Book Criterion #6: Price at least 30% above 52-week low
    Book Criterion #7: Price within 25% of 52-week high
    Book Criterion #8: RS Rating >= 70 (IBD-style percentile ranking)

    These 8 book criteria expand to 9 discrete checks + RS filter.
    """

    def __init__(self):
        """Initialize the analyzer with database connection and all sub-modules."""
        # Market benchmark symbols and their weights for relative strength calculation
        self.market_benchmarks = {
            'SPY': 0.35,   # S&P 500 - Broad US large cap (most important)
            'DIA': 0.15,   # Dow Jones Industrial Average - Blue chip stocks
            'QQQ': 0.25,   # Nasdaq-100 - Tech-heavy index
            'VTI': 0.20,   # Total US Stock Market - Comprehensive US coverage
            'VT': 0.05     # Total World Stock - Global diversification
        }

        # Initialize database manager
        self.db = DatabaseManager()

        # Expose connection for backward compatibility (used by main() and run_analysis.py)
        self.conn = self.db.conn

        # Initialize analysis sub-modules (all share the same DB connection)
        self.technical = TechnicalIndicators(self.conn)
        self.rs = RelativeStrengthCalculator(self.conn, self.market_benchmarks)
        self.volume = VolumeAnalyzer(self.conn)
        self.sector = SectorAnalyzer(self.conn)
        self.earnings = EarningsAnalyzer(self.conn)
        self.patterns = PatternDetector(self.conn)
        self.signals = SignalGenerator()
        self.notifications = NotificationManager(self.conn)

    def check_data_exists(self, date):
        """Check if data exists in the database for a given date."""
        return self.db.check_data_exists(date)

    def evaluate_minervini_criteria(self, symbol, date):
        """
        Evaluate a stock against Minervini's trend template.
        Returns metrics and pass/fail status, enhanced with ticker fundamentals.
        """
        cursor = self.conn.cursor()

        # Get ticker details for fundamental context
        ticker_details = self.db.get_ticker_details(symbol)

        # Apply Minervini-style filters based on fundamentals
        if ticker_details:
            # Filter 1: Inactive stocks
            if ticker_details['active'] is False:
                return None  # Skip inactive stocks

            # Filter 2: Extremely small market caps (< $100M)
            # Minervini avoids these due to liquidity concerns
            if ticker_details['market_cap'] and ticker_details['market_cap'] < 100_000_000:
                return None  # Too small, prone to manipulation

        # Get current price
        cursor.execute("""
            SELECT close FROM stock_prices
            WHERE symbol = %s AND date = %s
        """, (symbol, date))

        result = cursor.fetchone()
        cursor.close()

        if not result:
            return None

        close_price = float(result[0])

        # Calculate all metrics via sub-modules
        ma_50 = self.technical.calculate_moving_average(symbol, date, 50)
        ma_150 = self.technical.calculate_moving_average(symbol, date, 150)
        ma_200 = self.technical.calculate_moving_average(symbol, date, 200)
        week_52_high, week_52_low = self.technical.get_52_week_high_low(symbol, date)
        ma_200_trend = self.technical.calculate_ma_200_trend(symbol, date)
        ma_150_trend = self.technical.calculate_ma_150_trend(symbol, date)
        relative_strength = self.rs.calculate_relative_strength(symbol, date)

        # Detect VCP pattern
        vcp_data = self.patterns.detect_vcp(symbol, date)

        # Detect Primary Base for IPOs/new issues
        list_date = ticker_details['list_date'] if ticker_details else None
        primary_base_data = self.patterns.detect_primary_base(symbol, date, list_date)

        # Calculate enhanced metrics
        avg_dollar_volume = self.volume.calculate_avg_dollar_volume(symbol, date)
        volume_ratio = self.volume.calculate_volume_ratio(symbol, date)
        returns = self.volume.calculate_returns(symbol, date)
        atr_14, atr_percent = self.technical.calculate_atr(symbol, date)
        is_52w_high, days_since_52w_high = self.technical.calculate_new_high_metrics(symbol, date)
        industry_rs = self.sector.get_industry_rs(symbol, date)

        # Calculate short-term EMAs for entry/stop calculations
        ema_10 = self.technical.calculate_ema(symbol, date, 10)
        ema_21 = self.technical.calculate_ema(symbol, date, 21)

        # Find recent swing low for pattern-based stop loss
        swing_low = self.technical.find_recent_swing_low(symbol, date)

        # Evaluate earnings quality (fundamental analysis)
        earnings_quality = self.earnings.evaluate_earnings_quality(symbol, date)

        # Check if we have enough data
        if None in [ma_50, ma_150, ma_200, week_52_high, week_52_low]:
            return None

        # Calculate percent from 52-week high and low
        percent_from_52w_high = ((close_price - week_52_high) / week_52_high) * 100
        percent_from_52w_low = ((close_price - week_52_low) / week_52_low) * 100

        # Evaluate Minervini's Trend Template Criteria + RS filter
        # Reference: "Trade Like a Stock Market Wizard" by Mark Minervini
        #
        # Book criteria mapped to checks (some book criteria expand to 2 checks):
        # Book #1: Price above both 150-day and 200-day MA -> checks 1, 2
        # Book #2: 150-day MA above 200-day MA -> check 3
        # Book #3: 200-day MA trending up at least 1 month -> check 4
        # Book #4: 50-day MA above both 150-day and 200-day MA -> checks 5, 6
        # Book #5: Price above 50-day MA -> check 7
        # Book #6: Price at least 30% above 52-week low -> check 8
        # Book #7: Price within 25% of 52-week high -> check 9
        # Book #8: RS >= 70 -> separate RS filter
        criteria = {
            # Book #1a: Current price above 150-day MA
            'price_above_150ma': close_price > ma_150,

            # Book #1b: Current price above 200-day MA
            'price_above_200ma': close_price > ma_200,

            # Book #2: 150-day MA above 200-day MA (confirmed uptrend structure)
            'ma_150_above_200': ma_150 > ma_200,

            # Book #3: 200-day MA trending upward for at least 1 month
            'ma_200_trending_up': ma_200_trend is not None and ma_200_trend > 0,

            # Book #4a: 50-day MA above 150-day MA
            'ma_50_above_150': ma_50 > ma_150,

            # Book #4b: 50-day MA above 200-day MA
            'ma_50_above_200': ma_50 > ma_200,

            # Book #5: Price above 50-day MA (short-term strength)
            'price_above_50ma': close_price > ma_50,

            # Book #6: Price at least 30% above 52-week low
            'above_30pct_52w_low': percent_from_52w_low >= 30,

            # Book #7: Price within 25% of 52-week high
            'within_25pct_of_high': percent_from_52w_high >= -25,
        }

        # Additional filter: RS Rating >= 70 (Minervini recommends 80+ preferred)
        # This corresponds to Book criterion #8
        rs_filter = {
            'rs_above_70': relative_strength is not None and relative_strength >= 70
        }

        # Count criteria passed (9 core checks)
        criteria_passed = sum(criteria.values())

        # Stock passes if ALL 9 criteria met AND RS filter passes
        passes_all_criteria = all(criteria.values())
        passes_rs = all(rs_filter.values())
        passes_all = passes_all_criteria and passes_rs

        # Create list of failed criteria (include RS in the list)
        all_checks = {**criteria, **rs_filter}
        failed_criteria = [k for k, v in all_checks.items() if not v]

        # Determine stage
        stage = self.signals.determine_stage(
            close_price, ma_50, ma_150, ma_200,
            ma_200_trend, percent_from_52w_high, relative_strength
        )

        # Apply market cap context to analysis
        market_cap_tier = 'Unknown'
        liquidity_score = 0
        institutional_quality = False

        if ticker_details and ticker_details['market_cap']:
            market_cap_tier, liquidity_score, institutional_quality = \
                self.sector.classify_market_cap_tier(ticker_details['market_cap'])

            # Adjust passing criteria based on liquidity/quality
            # Minervini prefers stocks with institutional backing
            if relative_strength is not None:
                if not institutional_quality:
                    # Micro caps ($100M-$300M) need higher RS to compensate for liquidity risk
                    # Effectively raises the bar for these riskier stocks
                    if relative_strength < 80:
                        passes_all = False
                        if 'micro_cap_rs_too_low' not in failed_criteria:
                            failed_criteria.append('micro_cap_rs_too_low')

        # Apply earnings-based filtering (if earnings data available)
        # Note: We don't disqualify stocks without earnings data, but boost those with strong earnings
        if earnings_quality['has_earnings_data']:
            # If stock passes technical criteria, apply earnings filter
            if passes_all and not earnings_quality['passes_earnings_criteria']:
                # Stock has weak fundamentals - demote it
                passes_all = False
                failed_criteria.append('weak_earnings')

            # Additional filter for upcoming earnings warning
            if earnings_quality['upcoming']['has_upcoming_earnings']:
                days_until = earnings_quality['upcoming']['days_until']
                if days_until and days_until <= 7:
                    # Very close to earnings - high risk
                    # Note: Not a hard disqualifier, but flagged
                    pass

        # Apply Primary Base filter for new issues
        # Minervini requires IPOs to form a proper base before buying
        if primary_base_data['is_new_issue'] and not primary_base_data['has_primary_base']:
            if passes_all:
                passes_all = False
                if 'no_primary_base' not in failed_criteria:
                    failed_criteria.append('no_primary_base')

        # Extract earnings metrics for storage
        eps_growth_yoy = None
        eps_growth_qoq = None
        revenue_growth_yoy = None
        earnings_acceleration = False
        avg_eps_surprise = None
        earnings_beat_rate = None
        has_upcoming_earnings = False
        days_until_earnings = None
        earnings_quality_score = 0
        passes_earnings = None

        if earnings_quality['has_earnings_data']:
            growth = earnings_quality.get('growth', {})
            surprises = earnings_quality.get('surprises', {})
            upcoming = earnings_quality.get('upcoming', {})

            eps_growth_yoy = growth.get('yoy_eps_growth') if growth else None
            eps_growth_qoq = growth.get('qoq_eps_growth') if growth else None
            revenue_growth_yoy = growth.get('yoy_revenue_growth') if growth else None
            earnings_acceleration = growth.get('earnings_acceleration', False) if growth else False

            if surprises:
                avg_eps_surprise = surprises.get('avg_eps_surprise')
                total = surprises.get('total_earnings', 0)
                beats = surprises.get('beats', 0)
                earnings_beat_rate = (beats / total * 100) if total > 0 else None

            if upcoming:
                has_upcoming_earnings = upcoming.get('has_upcoming_earnings', False)
                days_until_earnings = upcoming.get('days_until')

            earnings_quality_score = earnings_quality.get('earnings_score', 0)
            passes_earnings = earnings_quality.get('passes_earnings_criteria')

        metrics = {
            'symbol': symbol,
            'date': date,
            'close_price': close_price,
            'ma_50': ma_50,
            'ma_150': ma_150,
            'ma_200': ma_200,
            'week_52_high': week_52_high,
            'week_52_low': week_52_low,
            'percent_from_52w_high': percent_from_52w_high,
            'percent_from_52w_low': percent_from_52w_low,
            'ma_150_trend_20d': ma_150_trend,
            'ma_200_trend_20d': ma_200_trend,
            'relative_strength': relative_strength,
            'stage': stage,
            'passes_minervini': passes_all,
            'criteria_passed': criteria_passed,
            'criteria_failed': ','.join(failed_criteria),
            # VCP metrics
            'vcp_detected': vcp_data['vcp_detected'],
            'vcp_score': vcp_data['vcp_score'],
            'contraction_count': vcp_data['contraction_count'],
            'latest_contraction_pct': vcp_data['latest_contraction_pct'],
            'volume_contraction': vcp_data['volume_contraction'],
            'pivot_price': vcp_data['pivot_price'],
            'last_contraction_low': vcp_data['last_contraction_low'],
            # Short-term EMAs (for entry/stop calculations)
            'ema_10': ema_10,
            'ema_21': ema_21,
            'swing_low': swing_low,
            # Enhanced metrics
            'avg_dollar_volume': avg_dollar_volume,
            'volume_ratio': volume_ratio,
            'return_1m': returns['return_1m'],
            'return_3m': returns['return_3m'],
            'return_6m': returns['return_6m'],
            'return_12m': returns['return_12m'],
            'atr_14': atr_14,
            'atr_percent': atr_percent,
            'is_52w_high': is_52w_high,
            'days_since_52w_high': days_since_52w_high,
            'industry_rs': industry_rs,
            # Earnings/Fundamental metrics
            'eps_growth_yoy': eps_growth_yoy,
            'eps_growth_qoq': eps_growth_qoq,
            'revenue_growth_yoy': revenue_growth_yoy,
            'earnings_acceleration': earnings_acceleration,
            'avg_eps_surprise': avg_eps_surprise,
            'earnings_beat_rate': earnings_beat_rate,
            'has_upcoming_earnings': has_upcoming_earnings,
            'days_until_earnings': days_until_earnings,
            'earnings_quality_score': earnings_quality_score,
            'passes_earnings': passes_earnings,
            # Primary Base metrics (IPO/new issues)
            'is_new_issue': primary_base_data['is_new_issue'],
            'has_primary_base': primary_base_data['has_primary_base'],
            'primary_base_weeks': primary_base_data['primary_base_weeks'],
            'primary_base_correction_pct': primary_base_data['primary_base_correction_pct'],
            'primary_base_status': primary_base_data['primary_base_status'],
            'days_since_ipo': primary_base_data['days_since_ipo'],
            # Ticker context (for logging/debugging)
            '_market_cap_tier': market_cap_tier,
            '_liquidity_score': liquidity_score,
            '_institutional_quality': institutional_quality,
            '_earnings_issues': earnings_quality.get('issues', [])
        }

        # Generate Buy/Wait/Pass signal with price levels
        signal_data = self.signals.generate_signal(metrics)
        metrics['signal'] = signal_data['signal']
        metrics['signal_reasons'] = '; '.join(signal_data['reasons'])
        metrics['entry_low'] = signal_data['entry_low']
        metrics['entry_high'] = signal_data['entry_high']
        metrics['stop_loss'] = signal_data['stop_loss']

        # Flatten sell targets into metrics
        sell_targets = signal_data['sell_targets']
        if sell_targets:
            metrics['sell_target_conservative'] = sell_targets['target_conservative']
            metrics['sell_target_primary'] = sell_targets['target_primary']
            metrics['sell_target_aggressive'] = sell_targets['target_aggressive']
            metrics['partial_profit_at'] = sell_targets['partial_profit_at']
            metrics['risk_reward_ratio'] = sell_targets['risk_reward_ratio']
            metrics['risk_percent'] = sell_targets['risk_percent']
        else:
            metrics['sell_target_conservative'] = None
            metrics['sell_target_primary'] = None
            metrics['sell_target_aggressive'] = None
            metrics['partial_profit_at'] = None
            metrics['risk_reward_ratio'] = None
            metrics['risk_percent'] = None

        return metrics

    def analyze_date(self, date):
        """Analyze all stocks for a given date."""
        cursor = self.conn.cursor()

        # Get all unique symbols for the date
        cursor.execute("""
            SELECT DISTINCT symbol FROM stock_prices
            WHERE date = %s
            ORDER BY symbol
        """, (date,))

        symbols = [row[0] for row in cursor.fetchall()]

        print(f"\nAnalyzing {len(symbols)} stocks for {date}...")

        # Pre-calculate weighted market performance (cached for all stocks)
        market_perf = self.rs.get_market_performance(date)
        benchmark_list = ', '.join([f"{sym}({w*100:.0f}%)" for sym, w in self.market_benchmarks.items()])
        print(f"Market performance (90-day): {market_perf:+.2f}% (weighted: {benchmark_list})")

        # Pre-calculate IBD-style RS percentile rankings (cached for all stocks)
        print("Calculating RS percentile rankings for all stocks...")
        rs_rankings = self.rs.calculate_rs_percentile_ranking(date)
        print(f"  Ranked {len(rs_rankings)} stocks by relative strength")

        # Pre-calculate sector performance for industry RS
        print("Calculating sector performance...")
        self.sector.calculate_sector_performance(date, market_perf)

        analyzed = 0
        passed = 0
        skipped = 0
        buffered_metrics = []  # accumulate in memory, write at the end

        for i, symbol in enumerate(symbols):
            try:
                metrics = self.evaluate_minervini_criteria(symbol, date)

                if metrics:
                    buffered_metrics.append(metrics)
                    analyzed += 1

                    if metrics['passes_minervini']:
                        passed += 1

                        # Signal indicator
                        sig = metrics.get('signal', 'PASS')
                        sig_icon = {'BUY': '🟢', 'WAIT': '🟡', 'PASS': '🔴'}.get(sig, '⚪')

                        vcp_info = ""
                        if metrics['vcp_detected']:
                            vcp_info = f" [VCP: {metrics['vcp_score']:.0f}]"

                        # Price levels for BUY/WAIT signals
                        price_info = ""
                        if sig in ('BUY', 'WAIT') and metrics.get('entry_low'):
                            price_info = f" Entry: ${metrics['entry_low']:.2f}-${metrics['entry_high']:.2f}"
                            if metrics.get('stop_loss'):
                                price_info += f" Stop: ${metrics['stop_loss']:.2f}"
                            if metrics.get('sell_target_primary'):
                                price_info += f" Target: ${metrics['sell_target_primary']:.2f}"

                        # Add market cap context if available
                        cap_info = ""
                        if metrics.get('_market_cap_tier') and metrics['_market_cap_tier'] != 'Unknown':
                            tier = metrics['_market_cap_tier']
                            quality = "⭐" if metrics.get('_institutional_quality') else ""
                            cap_info = f" [{tier}{quality}]"

                        print(f"  {sig_icon} {symbol}: {sig} - Stage {metrics['stage']} ({metrics['criteria_passed']}/9, RS={metrics['relative_strength']:.0f}){vcp_info}{price_info}{cap_info}")
                else:
                    skipped += 1

                if (i + 1) % 100 == 0:
                    print(f"  Progress: {i + 1}/{len(symbols)} stocks processed...")

            except Exception as e:
                print(f"  Error analyzing {symbol}: {e}")
                skipped += 1
                continue

        # Compute summary statistics from in-memory buffer
        passing_metrics = [m for m in buffered_metrics if m.get('passes_minervini')]

        signal_counts = {}
        small_cap_count = 0
        mid_cap_count = 0
        large_cap_count = 0

        for m in passing_metrics:
            sig = m.get('signal')
            if sig:
                signal_counts[sig] = signal_counts.get(sig, 0) + 1

            tier = m.get('_market_cap_tier', '')
            if 'Small' in tier:
                small_cap_count += 1
            elif 'Mid' in tier:
                mid_cap_count += 1
            elif 'Large' in tier or 'Mega' in tier:
                large_cap_count += 1

        print(f"\nAnalysis Summary:")
        print(f"  Total symbols: {len(symbols)}")
        print(f"  Analyzed: {analyzed}")
        print(f"  Passed Minervini: {passed} ({(passed/analyzed*100) if analyzed else 0:.1f}%)")
        print(f"  Skipped (insufficient data/filtered): {skipped}")

        if signal_counts:
            print(f"\n  Signal Distribution (Passing Stocks):")
            print(f"    🟢 BUY:  {signal_counts.get('BUY', 0)}")
            print(f"    🟡 WAIT: {signal_counts.get('WAIT', 0)}")
            print(f"    🔴 PASS: {signal_counts.get('PASS', 0)}")

        if passed > 0:
            print(f"\n  Market Cap Distribution (Passing Stocks):")
            print(f"    Small Cap ($300M-$2B): {small_cap_count}")
            print(f"    Mid Cap ($2B-$10B): {mid_cap_count}")
            print(f"    Large Cap+ ($10B+): {large_cap_count}")
            total_categorized = small_cap_count + mid_cap_count + large_cap_count
            if total_categorized < passed:
                print(f"    Unknown/Micro Cap: {passed - total_categorized}")

        cursor.close()
        return analyzed, passed, buffered_metrics

    def close(self):
        """Close database connection."""
        self.db.close()


def main():
    """Main execution function."""
    print("=" * 80)
    print("Minervini Stock Analyzer")
    print("=" * 80)

    # Initialize analyzer
    analyzer = MinerviniAnalyzer()

    # Use the latest date available in stock_prices
    cursor = analyzer.conn.cursor()
    cursor.execute("SELECT MAX(date) FROM stock_prices")
    result = cursor.fetchone()
    cursor.close()

    if not result or not result[0]:
        print("\n⚠ No data found in stock_prices table")
        print("Run the download script first to load data:")
        print("  python download_stock_data.py")
        analyzer.close()
        return

    target_date = result[0].strftime("%Y-%m-%d")
    print(f"Target date: {target_date} (latest in stock_prices)")

    # Analyze stocks (metrics buffered in memory)
    analyzed, passed, buffered_metrics = analyzer.analyze_date(target_date)

    # ------------------------------------------------------------------
    # Generate notifications for watchlist stocks (before writing to DB
    # so we can compare new metrics against previous data still in the DB)
    # ------------------------------------------------------------------
    watchlist_symbols = analyzer.db.get_watchlist_symbols()
    notifications = []

    if watchlist_symbols:
        metrics_by_symbol = {m['symbol']: m for m in buffered_metrics}
        notifications = analyzer.notifications.generate_notifications(
            watchlist_symbols, metrics_by_symbol, target_date, analyzer.db
        )

    # ------------------------------------------------------------------
    # Persist everything: metrics first, then notifications
    # ------------------------------------------------------------------
    print("\nSaving metrics to database...")
    analyzer.db.save_metrics_batch(buffered_metrics)

    if notifications:
        analyzer.db.save_notifications_batch(notifications)
        print(f"✓ Saved {len(notifications)} notification(s)")

    # Print notifications
    analyzer.notifications.print_notifications(notifications)

    # Show top stocks
    if passed > 0:
        print("\n" + "=" * 160)
        print("TOP STOCKS PASSING MINERVINI CRITERIA (with Signal, Entry, Stop & Target)")
        print("=" * 160)

        top_stocks = analyzer.db.get_top_stocks(target_date, limit=30)

        # Header row
        print(f"\n{'Signal':<6} {'Symbol':<8} {'Price':>9} {'RS':>4} {'VCP':>4} "
              f"{'Entry Range':>17} {'Stop':>9} {'Target':>9} {'R:R':>5} {'Risk%':>6} "
              f"{'EPSyy':>7} {'Beat%':>6} {'$Vol(M)':>8}")
        print("-" * 160)

        current_signal_group = None

        for stock in top_stocks:
            (symbol, price, rs, stage, pct_high, vcp_detected, vcp_score, pivot,
             avg_dv, vol_ratio, ind_rs, ret_1m, ret_3m, atr_pct, is_new_high,
             eps_yoy, eps_qoq, accel, beat_rate, upcoming, days_until, eq_score, passes_earn,
             market_cap, name,
             sig, sig_reasons, entry_low, entry_high, stop_loss,
             sell_target, rr_ratio, risk_pct) = stock

            # Group separator between signal types
            if sig != current_signal_group:
                if current_signal_group is not None:
                    print()
                current_signal_group = sig

            # Signal indicator
            sig_icon = {'BUY': '🟢BUY', 'WAIT': '🟡WAIT', 'PASS': '🔴PASS'}.get(sig or 'PASS', '⚪-')

            # Format values with fallbacks for None
            rs_str = f"{rs:.0f}" if rs else "-"
            vcp_str = f"{vcp_score:.0f}" if vcp_detected else "-"

            # Entry range
            if entry_low and entry_high:
                entry_str = f"${entry_low:.2f}-${entry_high:.2f}"
            else:
                entry_str = "-"

            # Stop loss
            stop_str = f"${stop_loss:.2f}" if stop_loss else "-"

            # Sell target
            target_str = f"${sell_target:.2f}" if sell_target else "-"

            # Risk/reward
            rr_str = f"{rr_ratio:.1f}" if rr_ratio else "-"
            risk_str = f"{risk_pct:.1f}%" if risk_pct else "-"

            # Fundamentals
            eps_yoy_str = f"{eps_yoy:+.0f}%" if eps_yoy is not None else "-"
            if accel:
                eps_yoy_str += "🚀"
            beat_str = f"{beat_rate:.0f}%" if beat_rate is not None else "-"
            avg_dv_str = f"{avg_dv/1_000_000:.1f}" if avg_dv else "-"

            print(f"{sig_icon:<6} {symbol:<8} ${price:>8.2f} {rs_str:>4} {vcp_str:>4} "
                  f"{entry_str:>17} {stop_str:>9} {target_str:>9} {rr_str:>5} {risk_str:>6} "
                  f"{eps_yoy_str:>7} {beat_str:>6} {avg_dv_str:>8}")

        # Show signal reasons for BUY/WAIT stocks
        print("\n" + "-" * 160)
        print("\nSIGNAL DETAILS:")
        print("-" * 80)

        for stock in top_stocks:
            sig = stock[25]  # signal column
            symbol = stock[0]
            sig_reasons = stock[26]  # signal_reasons column

            if sig in ('BUY', 'WAIT') and sig_reasons:
                sig_icon = '🟢' if sig == 'BUY' else '🟡'
                print(f"  {sig_icon} {symbol}: {sig_reasons}")

        print("\n" + "-" * 160)
        print("Legend: RS=Relative Strength, VCP=VCP Score, R:R=Risk/Reward Ratio, Risk%=Downside Risk %,")
        print("        EPSyy=EPS YoY Growth (🚀=accelerating), Beat%=Earnings Beat Rate (last 4 qtrs),")
        print("        $Vol(M)=Avg Dollar Volume in Millions")
        print("        Entry Range=Suggested buy zone, Stop=Stop loss, Target=Primary sell target")

    # Close connection
    analyzer.close()

    print("\n" + "=" * 80)
    print(f"Database: PostgreSQL @ localhost:5432/stocks")
    print("=" * 80)
