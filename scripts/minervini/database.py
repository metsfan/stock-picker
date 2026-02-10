"""
Database operations for Minervini analysis.

Handles PostgreSQL connection, metrics persistence, ticker details lookups,
notifications, and top-stocks queries.
"""

import json
import psycopg2
from psycopg2.extras import Json


class DatabaseManager:
    """Manages database connection and CRUD operations for Minervini analysis."""

    def __init__(self, conn_params=None):
        """
        Initialize database manager.

        Args:
            conn_params: Dict of psycopg2 connection parameters.
                         Defaults to local stocks database.
        """
        self.conn_params = conn_params or {
            'host': 'localhost',
            'port': 5432,
            'database': 'stocks',
            'user': 'stocks',
            'password': 'stocks'
        }

        self.conn = psycopg2.connect(**self.conn_params)
        self.conn.autocommit = False

    def check_data_exists(self, date):
        """Check if data exists in the database for a given date."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM stock_prices WHERE date = %s
        """, (date,))
        count = cursor.fetchone()[0]
        cursor.close()
        return count > 0

    def get_ticker_details(self, symbol):
        """
        Get ticker details from ticker_details table.
        Returns dict with company fundamentals or None if not found.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT symbol, name, market_cap, active, total_employees,
                   primary_exchange, sic_description, list_date
            FROM ticker_details
            WHERE symbol = %s
        """, (symbol,))

        result = cursor.fetchone()
        cursor.close()

        if not result:
            return None

        return {
            'symbol': result[0],
            'name': result[1],
            'market_cap': result[2],
            'active': result[3],
            'total_employees': result[4],
            'primary_exchange': result[5],
            'industry': result[6],
            'list_date': result[7]
        }

    def save_metrics(self, metrics):
        """Save Minervini metrics to database."""
        cursor = self.conn.cursor()

        cursor.execute("""
            INSERT INTO minervini_metrics
            (symbol, date, close_price, ma_50, ma_150, ma_200, 
             week_52_high, week_52_low, percent_from_52w_high, percent_from_52w_low,
             ma_150_trend_20d, ma_200_trend_20d, relative_strength, stage, passes_minervini,
             criteria_passed, criteria_failed,
             vcp_detected, vcp_score, contraction_count, latest_contraction_pct,
             volume_contraction, pivot_price, last_contraction_low,
             ema_10, ema_21, swing_low,
             avg_dollar_volume, volume_ratio,
             return_1m, return_3m, return_6m, return_12m,
             atr_14, atr_percent,
             is_52w_high, days_since_52w_high, industry_rs,
             eps_growth_yoy, eps_growth_qoq, revenue_growth_yoy,
             earnings_acceleration, avg_eps_surprise, earnings_beat_rate,
             has_upcoming_earnings, days_until_earnings,
             earnings_quality_score, passes_earnings,
                     is_new_issue, has_primary_base, primary_base_weeks,
             primary_base_correction_pct, primary_base_status, days_since_ipo,
             signal, signal_reasons,
             entry_low, entry_high, stop_loss,
             sell_target_conservative, sell_target_primary, sell_target_aggressive,
             partial_profit_at, risk_reward_ratio, risk_percent,
             holder_signal, holder_signal_reasons,
             holder_stop_initial, holder_stop_trailing, holder_trailing_method,
             cup_detected, cup_depth_pct, cup_duration_weeks,
             handle_detected, handle_depth_pct, handle_duration_weeks,
             handle_has_vcp, pattern_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, date)
            DO UPDATE SET
                close_price = EXCLUDED.close_price,
                ma_50 = EXCLUDED.ma_50,
                ma_150 = EXCLUDED.ma_150,
                ma_200 = EXCLUDED.ma_200,
                week_52_high = EXCLUDED.week_52_high,
                week_52_low = EXCLUDED.week_52_low,
                percent_from_52w_high = EXCLUDED.percent_from_52w_high,
                percent_from_52w_low = EXCLUDED.percent_from_52w_low,
                ma_150_trend_20d = EXCLUDED.ma_150_trend_20d,
                ma_200_trend_20d = EXCLUDED.ma_200_trend_20d,
                relative_strength = EXCLUDED.relative_strength,
                stage = EXCLUDED.stage,
                passes_minervini = EXCLUDED.passes_minervini,
                criteria_passed = EXCLUDED.criteria_passed,
                criteria_failed = EXCLUDED.criteria_failed,
                vcp_detected = EXCLUDED.vcp_detected,
                vcp_score = EXCLUDED.vcp_score,
                contraction_count = EXCLUDED.contraction_count,
                latest_contraction_pct = EXCLUDED.latest_contraction_pct,
                volume_contraction = EXCLUDED.volume_contraction,
                pivot_price = EXCLUDED.pivot_price,
                last_contraction_low = EXCLUDED.last_contraction_low,
                ema_10 = EXCLUDED.ema_10,
                ema_21 = EXCLUDED.ema_21,
                swing_low = EXCLUDED.swing_low,
                avg_dollar_volume = EXCLUDED.avg_dollar_volume,
                volume_ratio = EXCLUDED.volume_ratio,
                return_1m = EXCLUDED.return_1m,
                return_3m = EXCLUDED.return_3m,
                return_6m = EXCLUDED.return_6m,
                return_12m = EXCLUDED.return_12m,
                atr_14 = EXCLUDED.atr_14,
                atr_percent = EXCLUDED.atr_percent,
                is_52w_high = EXCLUDED.is_52w_high,
                days_since_52w_high = EXCLUDED.days_since_52w_high,
                industry_rs = EXCLUDED.industry_rs,
                eps_growth_yoy = EXCLUDED.eps_growth_yoy,
                eps_growth_qoq = EXCLUDED.eps_growth_qoq,
                revenue_growth_yoy = EXCLUDED.revenue_growth_yoy,
                earnings_acceleration = EXCLUDED.earnings_acceleration,
                avg_eps_surprise = EXCLUDED.avg_eps_surprise,
                earnings_beat_rate = EXCLUDED.earnings_beat_rate,
                has_upcoming_earnings = EXCLUDED.has_upcoming_earnings,
                days_until_earnings = EXCLUDED.days_until_earnings,
                earnings_quality_score = EXCLUDED.earnings_quality_score,
                passes_earnings = EXCLUDED.passes_earnings,
                is_new_issue = EXCLUDED.is_new_issue,
                has_primary_base = EXCLUDED.has_primary_base,
                primary_base_weeks = EXCLUDED.primary_base_weeks,
                primary_base_correction_pct = EXCLUDED.primary_base_correction_pct,
                primary_base_status = EXCLUDED.primary_base_status,
                days_since_ipo = EXCLUDED.days_since_ipo,
                signal = EXCLUDED.signal,
                signal_reasons = EXCLUDED.signal_reasons,
                entry_low = EXCLUDED.entry_low,
                entry_high = EXCLUDED.entry_high,
                stop_loss = EXCLUDED.stop_loss,
                sell_target_conservative = EXCLUDED.sell_target_conservative,
                sell_target_primary = EXCLUDED.sell_target_primary,
                sell_target_aggressive = EXCLUDED.sell_target_aggressive,
                partial_profit_at = EXCLUDED.partial_profit_at,
                risk_reward_ratio = EXCLUDED.risk_reward_ratio,
                risk_percent = EXCLUDED.risk_percent,
                holder_signal = EXCLUDED.holder_signal,
                holder_signal_reasons = EXCLUDED.holder_signal_reasons,
                holder_stop_initial = EXCLUDED.holder_stop_initial,
                holder_stop_trailing = EXCLUDED.holder_stop_trailing,
                holder_trailing_method = EXCLUDED.holder_trailing_method,
                cup_detected = EXCLUDED.cup_detected,
                cup_depth_pct = EXCLUDED.cup_depth_pct,
                cup_duration_weeks = EXCLUDED.cup_duration_weeks,
                handle_detected = EXCLUDED.handle_detected,
                handle_depth_pct = EXCLUDED.handle_depth_pct,
                handle_duration_weeks = EXCLUDED.handle_duration_weeks,
                handle_has_vcp = EXCLUDED.handle_has_vcp,
                pattern_type = EXCLUDED.pattern_type
        """, (
            metrics['symbol'],
            metrics['date'],
            metrics['close_price'],
            metrics['ma_50'],
            metrics['ma_150'],
            metrics['ma_200'],
            metrics['week_52_high'],
            metrics['week_52_low'],
            metrics['percent_from_52w_high'],
            metrics['percent_from_52w_low'],
            metrics['ma_150_trend_20d'],
            metrics['ma_200_trend_20d'],
            metrics['relative_strength'],
            metrics['stage'],
            metrics['passes_minervini'],
            metrics['criteria_passed'],
            metrics['criteria_failed'],
            metrics['vcp_detected'],
            metrics['vcp_score'],
            metrics['contraction_count'],
            metrics['latest_contraction_pct'],
            metrics['volume_contraction'],
            metrics['pivot_price'],
            metrics['last_contraction_low'],
            metrics['ema_10'],
            metrics['ema_21'],
            metrics['swing_low'],
            metrics['avg_dollar_volume'],
            metrics['volume_ratio'],
            metrics['return_1m'],
            metrics['return_3m'],
            metrics['return_6m'],
            metrics['return_12m'],
            metrics['atr_14'],
            metrics['atr_percent'],
            metrics['is_52w_high'],
            metrics['days_since_52w_high'],
            metrics['industry_rs'],
            metrics['eps_growth_yoy'],
            metrics['eps_growth_qoq'],
            metrics['revenue_growth_yoy'],
            metrics['earnings_acceleration'],
            metrics['avg_eps_surprise'],
            metrics['earnings_beat_rate'],
            metrics['has_upcoming_earnings'],
            metrics['days_until_earnings'],
            metrics['earnings_quality_score'],
            metrics['passes_earnings'],
            metrics['is_new_issue'],
            metrics['has_primary_base'],
            metrics['primary_base_weeks'],
            metrics['primary_base_correction_pct'],
            metrics['primary_base_status'],
            metrics['days_since_ipo'],
            metrics['signal'],
            metrics['signal_reasons'],
            metrics['entry_low'],
            metrics['entry_high'],
            metrics['stop_loss'],
            metrics['sell_target_conservative'],
            metrics['sell_target_primary'],
            metrics['sell_target_aggressive'],
            metrics['partial_profit_at'],
            metrics['risk_reward_ratio'],
            metrics['risk_percent'],
            metrics['holder_signal'],
            metrics['holder_signal_reasons'],
            metrics['holder_stop_initial'],
            metrics['holder_stop_trailing'],
            metrics['holder_trailing_method'],
            metrics['cup_detected'],
            metrics['cup_depth_pct'],
            metrics['cup_duration_weeks'],
            metrics['handle_detected'],
            metrics['handle_depth_pct'],
            metrics['handle_duration_weeks'],
            metrics['handle_has_vcp'],
            metrics['pattern_type']
        ))

        self.conn.commit()
        cursor.close()

    def get_top_stocks(self, date, limit=50):
        """Get top stocks that pass Minervini criteria with signal and price levels."""
        cursor = self.conn.cursor()

        cursor.execute("""
            SELECT mm.symbol, mm.close_price, mm.relative_strength, mm.stage,
                   mm.percent_from_52w_high, mm.vcp_detected, mm.vcp_score, mm.pivot_price,
                   mm.avg_dollar_volume, mm.volume_ratio, mm.industry_rs,
                   mm.return_1m, mm.return_3m, mm.atr_percent, mm.is_52w_high,
                   mm.eps_growth_yoy, mm.eps_growth_qoq, mm.earnings_acceleration,
                   mm.earnings_beat_rate, mm.has_upcoming_earnings, mm.days_until_earnings,
                   mm.earnings_quality_score, mm.passes_earnings,
                   td.market_cap, td.name,
                   mm.signal, mm.signal_reasons,
                   mm.entry_low, mm.entry_high, mm.stop_loss,
                   mm.sell_target_primary, mm.risk_reward_ratio, mm.risk_percent
            FROM minervini_metrics mm
            LEFT JOIN ticker_details td ON mm.symbol = td.symbol
            WHERE mm.date = %s AND mm.passes_minervini = true
            ORDER BY 
                CASE mm.signal WHEN 'BUY' THEN 1 WHEN 'WAIT' THEN 2 ELSE 3 END,
                mm.passes_earnings DESC NULLS LAST,
                mm.earnings_quality_score DESC NULLS LAST,
                mm.relative_strength DESC, 
                mm.industry_rs DESC NULLS LAST
            LIMIT %s
        """, (date, limit))

        results = cursor.fetchall()
        cursor.close()
        return results

    def save_metrics_batch(self, metrics_list):
        """
        Bulk upsert a list of metrics dicts in a single transaction.
        Uses the same INSERT ... ON CONFLICT logic as save_metrics.
        """
        if not metrics_list:
            return

        cursor = self.conn.cursor()
        try:
            for metrics in metrics_list:
                cursor.execute("""
                    INSERT INTO minervini_metrics
                    (symbol, date, close_price, ma_50, ma_150, ma_200,
                     week_52_high, week_52_low, percent_from_52w_high, percent_from_52w_low,
                     ma_150_trend_20d, ma_200_trend_20d, relative_strength, stage, passes_minervini,
                     criteria_passed, criteria_failed,
                     vcp_detected, vcp_score, contraction_count, latest_contraction_pct,
                     volume_contraction, pivot_price, last_contraction_low,
                     ema_10, ema_21, swing_low,
                     avg_dollar_volume, volume_ratio,
                     return_1m, return_3m, return_6m, return_12m,
                     atr_14, atr_percent,
                     is_52w_high, days_since_52w_high, industry_rs,
                     eps_growth_yoy, eps_growth_qoq, revenue_growth_yoy,
                     earnings_acceleration, avg_eps_surprise, earnings_beat_rate,
                     has_upcoming_earnings, days_until_earnings,
                     earnings_quality_score, passes_earnings,
                     is_new_issue, has_primary_base, primary_base_weeks,
                     primary_base_correction_pct, primary_base_status, days_since_ipo,
                     signal, signal_reasons,
                     entry_low, entry_high, stop_loss,
                     sell_target_conservative, sell_target_primary, sell_target_aggressive,
                     partial_profit_at, risk_reward_ratio, risk_percent,
                     holder_signal, holder_signal_reasons,
                     holder_stop_initial, holder_stop_trailing, holder_trailing_method,
                     cup_detected, cup_depth_pct, cup_duration_weeks,
                     handle_detected, handle_depth_pct, handle_duration_weeks,
                     handle_has_vcp, pattern_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, date)
                    DO UPDATE SET
                        close_price = EXCLUDED.close_price,
                        ma_50 = EXCLUDED.ma_50,
                        ma_150 = EXCLUDED.ma_150,
                        ma_200 = EXCLUDED.ma_200,
                        week_52_high = EXCLUDED.week_52_high,
                        week_52_low = EXCLUDED.week_52_low,
                        percent_from_52w_high = EXCLUDED.percent_from_52w_high,
                        percent_from_52w_low = EXCLUDED.percent_from_52w_low,
                        ma_150_trend_20d = EXCLUDED.ma_150_trend_20d,
                        ma_200_trend_20d = EXCLUDED.ma_200_trend_20d,
                        relative_strength = EXCLUDED.relative_strength,
                        stage = EXCLUDED.stage,
                        passes_minervini = EXCLUDED.passes_minervini,
                        criteria_passed = EXCLUDED.criteria_passed,
                        criteria_failed = EXCLUDED.criteria_failed,
                        vcp_detected = EXCLUDED.vcp_detected,
                        vcp_score = EXCLUDED.vcp_score,
                        contraction_count = EXCLUDED.contraction_count,
                        latest_contraction_pct = EXCLUDED.latest_contraction_pct,
                        volume_contraction = EXCLUDED.volume_contraction,
                        pivot_price = EXCLUDED.pivot_price,
                        last_contraction_low = EXCLUDED.last_contraction_low,
                        ema_10 = EXCLUDED.ema_10,
                        ema_21 = EXCLUDED.ema_21,
                        swing_low = EXCLUDED.swing_low,
                        avg_dollar_volume = EXCLUDED.avg_dollar_volume,
                        volume_ratio = EXCLUDED.volume_ratio,
                        return_1m = EXCLUDED.return_1m,
                        return_3m = EXCLUDED.return_3m,
                        return_6m = EXCLUDED.return_6m,
                        return_12m = EXCLUDED.return_12m,
                        atr_14 = EXCLUDED.atr_14,
                        atr_percent = EXCLUDED.atr_percent,
                        is_52w_high = EXCLUDED.is_52w_high,
                        days_since_52w_high = EXCLUDED.days_since_52w_high,
                        industry_rs = EXCLUDED.industry_rs,
                        eps_growth_yoy = EXCLUDED.eps_growth_yoy,
                        eps_growth_qoq = EXCLUDED.eps_growth_qoq,
                        revenue_growth_yoy = EXCLUDED.revenue_growth_yoy,
                        earnings_acceleration = EXCLUDED.earnings_acceleration,
                        avg_eps_surprise = EXCLUDED.avg_eps_surprise,
                        earnings_beat_rate = EXCLUDED.earnings_beat_rate,
                        has_upcoming_earnings = EXCLUDED.has_upcoming_earnings,
                        days_until_earnings = EXCLUDED.days_until_earnings,
                        earnings_quality_score = EXCLUDED.earnings_quality_score,
                        passes_earnings = EXCLUDED.passes_earnings,
                        is_new_issue = EXCLUDED.is_new_issue,
                        has_primary_base = EXCLUDED.has_primary_base,
                        primary_base_weeks = EXCLUDED.primary_base_weeks,
                        primary_base_correction_pct = EXCLUDED.primary_base_correction_pct,
                        primary_base_status = EXCLUDED.primary_base_status,
                        days_since_ipo = EXCLUDED.days_since_ipo,
                        signal = EXCLUDED.signal,
                        signal_reasons = EXCLUDED.signal_reasons,
                        entry_low = EXCLUDED.entry_low,
                        entry_high = EXCLUDED.entry_high,
                        stop_loss = EXCLUDED.stop_loss,
                        sell_target_conservative = EXCLUDED.sell_target_conservative,
                        sell_target_primary = EXCLUDED.sell_target_primary,
                        sell_target_aggressive = EXCLUDED.sell_target_aggressive,
                        partial_profit_at = EXCLUDED.partial_profit_at,
                        risk_reward_ratio = EXCLUDED.risk_reward_ratio,
                        risk_percent = EXCLUDED.risk_percent,
                        holder_signal = EXCLUDED.holder_signal,
                        holder_signal_reasons = EXCLUDED.holder_signal_reasons,
                        holder_stop_initial = EXCLUDED.holder_stop_initial,
                        holder_stop_trailing = EXCLUDED.holder_stop_trailing,
                        holder_trailing_method = EXCLUDED.holder_trailing_method,
                        cup_detected = EXCLUDED.cup_detected,
                        cup_depth_pct = EXCLUDED.cup_depth_pct,
                        cup_duration_weeks = EXCLUDED.cup_duration_weeks,
                        handle_detected = EXCLUDED.handle_detected,
                        handle_depth_pct = EXCLUDED.handle_depth_pct,
                        handle_duration_weeks = EXCLUDED.handle_duration_weeks,
                        handle_has_vcp = EXCLUDED.handle_has_vcp,
                        pattern_type = EXCLUDED.pattern_type
                """, (
                    metrics['symbol'],
                    metrics['date'],
                    metrics['close_price'],
                    metrics['ma_50'],
                    metrics['ma_150'],
                    metrics['ma_200'],
                    metrics['week_52_high'],
                    metrics['week_52_low'],
                    metrics['percent_from_52w_high'],
                    metrics['percent_from_52w_low'],
                    metrics['ma_150_trend_20d'],
                    metrics['ma_200_trend_20d'],
                    metrics['relative_strength'],
                    metrics['stage'],
                    metrics['passes_minervini'],
                    metrics['criteria_passed'],
                    metrics['criteria_failed'],
                    metrics['vcp_detected'],
                    metrics['vcp_score'],
                    metrics['contraction_count'],
                    metrics['latest_contraction_pct'],
                    metrics['volume_contraction'],
                    metrics['pivot_price'],
                    metrics['last_contraction_low'],
                    metrics['ema_10'],
                    metrics['ema_21'],
                    metrics['swing_low'],
                    metrics['avg_dollar_volume'],
                    metrics['volume_ratio'],
                    metrics['return_1m'],
                    metrics['return_3m'],
                    metrics['return_6m'],
                    metrics['return_12m'],
                    metrics['atr_14'],
                    metrics['atr_percent'],
                    metrics['is_52w_high'],
                    metrics['days_since_52w_high'],
                    metrics['industry_rs'],
                    metrics['eps_growth_yoy'],
                    metrics['eps_growth_qoq'],
                    metrics['revenue_growth_yoy'],
                    metrics['earnings_acceleration'],
                    metrics['avg_eps_surprise'],
                    metrics['earnings_beat_rate'],
                    metrics['has_upcoming_earnings'],
                    metrics['days_until_earnings'],
                    metrics['earnings_quality_score'],
                    metrics['passes_earnings'],
                    metrics['is_new_issue'],
                    metrics['has_primary_base'],
                    metrics['primary_base_weeks'],
                    metrics['primary_base_correction_pct'],
                    metrics['primary_base_status'],
                    metrics['days_since_ipo'],
                    metrics['signal'],
                    metrics['signal_reasons'],
                    metrics['entry_low'],
                    metrics['entry_high'],
                    metrics['stop_loss'],
                    metrics['sell_target_conservative'],
                    metrics['sell_target_primary'],
                    metrics['sell_target_aggressive'],
                    metrics['partial_profit_at'],
                    metrics['risk_reward_ratio'],
                    metrics['risk_percent'],
                    metrics['holder_signal'],
                    metrics['holder_signal_reasons'],
                    metrics['holder_stop_initial'],
                    metrics['holder_stop_trailing'],
                    metrics['holder_trailing_method'],
                    metrics['cup_detected'],
                    metrics['cup_depth_pct'],
                    metrics['cup_duration_weeks'],
                    metrics['handle_detected'],
                    metrics['handle_depth_pct'],
                    metrics['handle_duration_weeks'],
                    metrics['handle_has_vcp'],
                    metrics['pattern_type']
                ))

            self.conn.commit()
            print(f"✓ Saved {len(metrics_list)} stock metrics to database")
        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cursor.close()

    # ------------------------------------------------------------------
    # Watchlist helpers
    # ------------------------------------------------------------------

    def get_watchlist_symbols(self):
        """Return list of symbols from the watchlist table."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT symbol FROM watchlist ORDER BY symbol")
        symbols = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return symbols

    def get_previous_metrics(self, symbol, before_date):
        """
        Get the most recent minervini_metrics row for *symbol* strictly
        before *before_date*.  Returns a dict keyed by column name, or
        None if no prior row exists.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT symbol, date, signal, stage, relative_strength,
                   vcp_detected, vcp_score, passes_minervini, passes_earnings,
                   criteria_passed, close_price, entry_low, entry_high,
                   stop_loss, sell_target_primary, signal_reasons
            FROM minervini_metrics
            WHERE symbol = %s AND date < %s
            ORDER BY date DESC
            LIMIT 1
        """, (symbol, before_date))

        row = cursor.fetchone()
        cursor.close()

        if not row:
            return None

        return {
            'symbol': row[0],
            'date': row[1],
            'signal': row[2],
            'stage': row[3],
            'relative_strength': row[4],
            'vcp_detected': row[5],
            'vcp_score': row[6],
            'passes_minervini': row[7],
            'passes_earnings': row[8],
            'criteria_passed': row[9],
            'close_price': row[10],
            'entry_low': row[11],
            'entry_high': row[12],
            'stop_loss': row[13],
            'sell_target_primary': row[14],
            'signal_reasons': row[15],
        }

    # ------------------------------------------------------------------
    # Notification helpers
    # ------------------------------------------------------------------

    def save_notification(self, symbol, date, notification_type, title, message, metadata=None):
        """Insert a single notification row."""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO notifications (symbol, date, notification_type, title, message, metadata)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (symbol, date, notification_type, title, message,
              Json(metadata) if metadata else None))
        self.conn.commit()
        cursor.close()

    def save_notifications_batch(self, notifications):
        """
        Bulk-insert a list of notification dicts.

        Each dict must have keys: symbol, date, notification_type, title,
        message, and optionally metadata.
        """
        if not notifications:
            return

        cursor = self.conn.cursor()
        try:
            for n in notifications:
                cursor.execute("""
                    INSERT INTO notifications
                        (symbol, date, notification_type, title, message, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    n['symbol'], n['date'], n['notification_type'],
                    n['title'], n['message'],
                    Json(n.get('metadata')) if n.get('metadata') else None
                ))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cursor.close()

    def get_notifications(self, date, unread_only=False):
        """
        Retrieve notifications for a given date.
        Optionally filter to unread only.
        """
        cursor = self.conn.cursor()
        query = """
            SELECT id, symbol, date, notification_type, title, message,
                   metadata, created_at, is_read
            FROM notifications
            WHERE date = %s
        """
        params = [date]

        if unread_only:
            query += " AND is_read = FALSE"

        query += " ORDER BY notification_type, symbol"

        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()

        return [{
            'id': r[0], 'symbol': r[1], 'date': r[2],
            'notification_type': r[3], 'title': r[4], 'message': r[5],
            'metadata': r[6], 'created_at': r[7], 'is_read': r[8],
        } for r in rows]

    def close(self):
        """Close database connection."""
        self.conn.close()
