"""
Notification manager for Minervini analysis.

Detects actionable events for watchlist stocks and persists them
as notification rows:
  - WAIT_TO_BUY:       Signal upgraded from WAIT to BUY
  - HOLD_TO_SELL:      Holder signal changed from HOLD to SELL
  - METRIC_CHANGE:     Significant change in stage, VCP, RS, etc.
  - EARNINGS_SURPRISE: Tremendous earnings surprise in last 24 hours
"""


class NotificationManager:
    """Generates and stores notifications for watchlist stocks."""

    # ----- configurable thresholds -----
    RS_CHANGE_THRESHOLD = 10       # notify if RS moves >= 10 points
    VCP_SCORE_THRESHOLD = 20       # notify if VCP score changes >= 20
    EARNINGS_SURPRISE_PCT = 15     # notify if |eps_surprise_%| >= 15

    def __init__(self, conn):
        """
        Args:
            conn: Active psycopg2 connection (shared with DatabaseManager).
        """
        self.conn = conn
        # Accumulated notifications to be bulk-saved later
        self._pending = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_notifications(self, watchlist_symbols, metrics_by_symbol,
                               analysis_date, db):
        """
        Run all notification checks for every watchlist symbol.

        Args:
            watchlist_symbols: list[str] from the watchlist table.
            metrics_by_symbol: dict mapping symbol -> new metrics dict
                               (computed in the current run, still in memory).
            analysis_date: str  (YYYY-MM-DD) being analysed.
            db: DatabaseManager instance (for previous-metrics lookups).

        Returns:
            list[dict] of notification rows ready for bulk insert.
        """
        self._pending = []

        for symbol in watchlist_symbols:
            new = metrics_by_symbol.get(symbol)
            if new is None:
                # Stock wasn't analysed this run (no price data, filtered, etc.)
                continue

            prev = db.get_previous_metrics(symbol, analysis_date)

            self._check_wait_to_buy(symbol, new, prev, analysis_date)
            self._check_hold_to_sell(symbol, new, prev, analysis_date)
            self._check_metric_changes(symbol, new, prev, analysis_date)
            self._check_earnings_surprise(symbol, analysis_date)

        return self._pending

    def print_notifications(self, notifications):
        """Pretty-print notifications grouped by type."""
        if not notifications:
            return

        print("\n" + "=" * 80)
        print("NOTIFICATIONS (Watchlist Alerts)")
        print("=" * 80)

        by_type = {}
        for n in notifications:
            by_type.setdefault(n['notification_type'], []).append(n)

        type_labels = {
            'WAIT_TO_BUY': '🟢 WAIT → BUY',
            'HOLD_TO_SELL': '🚨 HOLD → SELL',
            'METRIC_CHANGE': '🔄 METRIC CHANGES',
            'EARNINGS_SURPRISE': '💰 EARNINGS SURPRISES',
        }

        for ntype in ('WAIT_TO_BUY', 'HOLD_TO_SELL', 'METRIC_CHANGE', 'EARNINGS_SURPRISE'):
            items = by_type.get(ntype, [])
            if not items:
                continue
            print(f"\n  {type_labels.get(ntype, ntype)}:")
            for n in items:
                print(f"    {n['symbol']}: {n['message']}")

        print("\n" + "=" * 80)

    # ------------------------------------------------------------------
    # Internal checks
    # ------------------------------------------------------------------

    def _check_wait_to_buy(self, symbol, new, prev, date):
        """Create notification when signal upgrades from WAIT to BUY."""
        if prev is None:
            return

        if prev.get('signal') == 'WAIT' and new.get('signal') == 'BUY':
            entry_str = ""
            if new.get('entry_low') and new.get('entry_high'):
                entry_str = f" Entry: ${new['entry_low']:.2f}-${new['entry_high']:.2f}"
            stop_str = ""
            if new.get('stop_loss'):
                stop_str = f" Stop: ${new['stop_loss']:.2f}"
            target_str = ""
            if new.get('sell_target_primary'):
                target_str = f" Target: ${new['sell_target_primary']:.2f}"

            rs_val = new.get('relative_strength')
            rs_str = f", RS={rs_val:.0f}" if rs_val is not None else ""
            vcp_val = new.get('vcp_score')
            vcp_str = f", VCP={vcp_val:.0f}" if new.get('vcp_detected') and vcp_val else ""

            message = (
                f"Signal upgraded to BUY - Stage {new.get('stage')}"
                f"{rs_str}{vcp_str}.{entry_str}{stop_str}{target_str}"
            )

            self._pending.append({
                'symbol': symbol,
                'date': date,
                'notification_type': 'WAIT_TO_BUY',
                'title': f"{symbol} upgraded from WAIT to BUY",
                'message': message,
                'metadata': {
                    'previous_signal': prev.get('signal'),
                    'new_signal': 'BUY',
                    'stage': new.get('stage'),
                    'relative_strength': float(rs_val) if rs_val is not None else None,
                    'entry_low': float(new['entry_low']) if new.get('entry_low') else None,
                    'entry_high': float(new['entry_high']) if new.get('entry_high') else None,
                    'stop_loss': float(new['stop_loss']) if new.get('stop_loss') else None,
                    'sell_target_primary': float(new['sell_target_primary']) if new.get('sell_target_primary') else None,
                    'signal_reasons': new.get('signal_reasons'),
                },
            })

    def _check_hold_to_sell(self, symbol, new, prev, date):
        """Create notification when holder signal changes from HOLD to SELL."""
        if prev is None:
            return

        prev_holder = prev.get('holder_signal')
        new_holder = new.get('holder_signal')

        if prev_holder == 'HOLD' and new_holder == 'SELL':
            stage = new.get('stage')
            stage_str = f"Stage {stage}" if stage is not None else "Unknown stage"
            
            rs_val = new.get('relative_strength')
            rs_str = f", RS={rs_val:.0f}" if rs_val is not None else ""
            
            # Extract primary sell reason from reasons list
            reasons = new.get('holder_signal_reasons', '')
            primary_reason = reasons.split(';')[0].strip() if reasons else "Technical deterioration"
            
            initial_stop = new.get('holder_stop_initial')
            trailing_stop = new.get('holder_stop_trailing')
            stop_str = ""
            if initial_stop:
                stop_str = f" Initial stop: ${initial_stop:.2f}"
            if trailing_stop:
                method = new.get('holder_trailing_method', '')
                method_str = f" ({method})" if method else ""
                if stop_str:
                    stop_str += f", Trailing: ${trailing_stop:.2f}{method_str}"
                else:
                    stop_str = f" Trailing stop: ${trailing_stop:.2f}{method_str}"

            message = (
                f"Holder signal changed to SELL - {primary_reason}. "
                f"{stage_str}{rs_str}.{stop_str}"
            )

            self._pending.append({
                'symbol': symbol,
                'date': date,
                'notification_type': 'HOLD_TO_SELL',
                'title': f"{symbol} holder signal changed to SELL",
                'message': message,
                'metadata': {
                    'previous_holder_signal': prev_holder,
                    'new_holder_signal': 'SELL',
                    'stage': stage,
                    'relative_strength': float(rs_val) if rs_val is not None else None,
                    'holder_stop_initial': float(initial_stop) if initial_stop else None,
                    'holder_stop_trailing': float(trailing_stop) if trailing_stop else None,
                    'holder_trailing_method': new.get('holder_trailing_method'),
                    'holder_signal_reasons': reasons,
                },
            })

    def _check_metric_changes(self, symbol, new, prev, date):
        """Create notifications for significant metric changes."""
        if prev is None:
            return

        # --- Stage change ---
        new_stage = new.get('stage')
        prev_stage = prev.get('stage')
        if new_stage is not None and prev_stage is not None and new_stage != prev_stage:
            self._pending.append({
                'symbol': symbol,
                'date': date,
                'notification_type': 'METRIC_CHANGE',
                'title': f"{symbol} stage changed",
                'message': f"Stage changed from {prev_stage} to {new_stage}",
                'metadata': {
                    'metric': 'stage',
                    'old_value': prev_stage,
                    'new_value': new_stage,
                },
            })

        # --- VCP detection change ---
        new_vcp = new.get('vcp_detected')
        prev_vcp = prev.get('vcp_detected')
        new_vcp_score = new.get('vcp_score') or 0
        prev_vcp_score = prev.get('vcp_score') or 0

        if not prev_vcp and new_vcp:
            self._pending.append({
                'symbol': symbol,
                'date': date,
                'notification_type': 'METRIC_CHANGE',
                'title': f"{symbol} VCP detected",
                'message': f"VCP pattern detected (score: {new_vcp_score:.0f})",
                'metadata': {
                    'metric': 'vcp_detected',
                    'old_value': False,
                    'new_value': True,
                    'vcp_score': float(new_vcp_score),
                },
            })
        elif prev_vcp and new_vcp and abs(new_vcp_score - prev_vcp_score) >= self.VCP_SCORE_THRESHOLD:
            direction = "improved" if new_vcp_score > prev_vcp_score else "weakened"
            self._pending.append({
                'symbol': symbol,
                'date': date,
                'notification_type': 'METRIC_CHANGE',
                'title': f"{symbol} VCP score {direction}",
                'message': f"VCP score {direction} from {prev_vcp_score:.0f} to {new_vcp_score:.0f}",
                'metadata': {
                    'metric': 'vcp_score',
                    'old_value': float(prev_vcp_score),
                    'new_value': float(new_vcp_score),
                },
            })

        # --- Relative strength change ---
        new_rs = new.get('relative_strength')
        prev_rs = prev.get('relative_strength')
        if new_rs is not None and prev_rs is not None:
            rs_delta = new_rs - float(prev_rs)
            if abs(rs_delta) >= self.RS_CHANGE_THRESHOLD:
                direction = "jumped" if rs_delta > 0 else "dropped"
                self._pending.append({
                    'symbol': symbol,
                    'date': date,
                    'notification_type': 'METRIC_CHANGE',
                    'title': f"{symbol} RS {direction}",
                    'message': f"RS {direction} from {float(prev_rs):.0f} to {new_rs:.0f}",
                    'metadata': {
                        'metric': 'relative_strength',
                        'old_value': float(prev_rs),
                        'new_value': float(new_rs),
                        'delta': float(rs_delta),
                    },
                })

        # --- passes_minervini flip ---
        new_pass = new.get('passes_minervini')
        prev_pass = prev.get('passes_minervini')
        if new_pass is not None and prev_pass is not None and new_pass != prev_pass:
            if new_pass:
                msg = "Now passes all Minervini criteria"
            else:
                msg = "No longer passes Minervini criteria"
            self._pending.append({
                'symbol': symbol,
                'date': date,
                'notification_type': 'METRIC_CHANGE',
                'title': f"{symbol} Minervini criteria {'met' if new_pass else 'lost'}",
                'message': msg,
                'metadata': {
                    'metric': 'passes_minervini',
                    'old_value': prev_pass,
                    'new_value': new_pass,
                },
            })

        # --- passes_earnings flip ---
        new_earn = new.get('passes_earnings')
        prev_earn = prev.get('passes_earnings')
        if new_earn is not None and prev_earn is not None and new_earn != prev_earn:
            if new_earn:
                msg = "Now passes earnings quality filter"
            else:
                msg = "No longer passes earnings quality filter"
            self._pending.append({
                'symbol': symbol,
                'date': date,
                'notification_type': 'METRIC_CHANGE',
                'title': f"{symbol} earnings quality {'passed' if new_earn else 'failed'}",
                'message': msg,
                'metadata': {
                    'metric': 'passes_earnings',
                    'old_value': prev_earn,
                    'new_value': new_earn,
                },
            })

    def _check_earnings_surprise(self, symbol, date):
        """
        Check the earnings table for a tremendous surprise reported
        on or one trading day before the analysis date.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT actual_eps, estimated_eps, eps_surprise_percent,
                   actual_revenue, estimated_revenue, revenue_surprise_percent,
                   fiscal_period, fiscal_year
            FROM earnings
            WHERE ticker = %s
              AND date BETWEEN (%s::date - INTERVAL '1 day') AND %s::date
              AND actual_eps IS NOT NULL
            ORDER BY date DESC
            LIMIT 1
        """, (symbol, date, date))

        row = cursor.fetchone()
        cursor.close()

        if row is None:
            return

        (actual_eps, est_eps, eps_surp_pct,
         actual_rev, est_rev, rev_surp_pct,
         fiscal_period, fiscal_year) = row

        eps_surp_pct = float(eps_surp_pct) if eps_surp_pct is not None else None
        rev_surp_pct = float(rev_surp_pct) if rev_surp_pct is not None else None

        # Need at least one surprise that exceeds the threshold
        eps_notable = eps_surp_pct is not None and abs(eps_surp_pct) >= self.EARNINGS_SURPRISE_PCT
        rev_notable = rev_surp_pct is not None and abs(rev_surp_pct) >= self.EARNINGS_SURPRISE_PCT

        if not eps_notable and not rev_notable:
            return

        # Build message parts
        parts = []
        if eps_surp_pct is not None:
            eps_dir = "beat" if eps_surp_pct > 0 else "miss"
            actual_str = f"${actual_eps:.2f}" if actual_eps is not None else "N/A"
            est_str = f"${est_eps:.2f}" if est_eps is not None else "N/A"
            parts.append(f"EPS {eps_dir} {eps_surp_pct:+.1f}% (actual {actual_str} vs est {est_str})")
        if rev_surp_pct is not None and abs(rev_surp_pct) >= 1:
            parts.append(f"Revenue {rev_surp_pct:+.1f}%")

        period_str = f"{fiscal_period} {fiscal_year}" if fiscal_period and fiscal_year else ""
        message = "; ".join(parts)
        if period_str:
            message = f"[{period_str}] {message}"

        self._pending.append({
            'symbol': symbol,
            'date': date,
            'notification_type': 'EARNINGS_SURPRISE',
            'title': f"{symbol} earnings surprise",
            'message': message,
            'metadata': {
                'actual_eps': float(actual_eps) if actual_eps is not None else None,
                'estimated_eps': float(est_eps) if est_eps is not None else None,
                'eps_surprise_percent': eps_surp_pct,
                'actual_revenue': float(actual_rev) if actual_rev is not None else None,
                'estimated_revenue': float(est_rev) if est_rev is not None else None,
                'revenue_surprise_percent': rev_surp_pct,
                'fiscal_period': fiscal_period,
                'fiscal_year': str(fiscal_year) if fiscal_year else None,
            },
        })
