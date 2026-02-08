"""
Trading signal generation for Minervini analysis.

Pure logic module (no database dependency) that determines market stage,
calculates entry/stop/target price levels, and generates Buy/Wait/Pass signals.
"""


class SignalGenerator:
    """Generates trading signals and price levels based on Minervini's SEPA methodology."""

    def determine_stage(self, close_price, ma_50, ma_150, ma_200, ma_200_trend,
                        percent_from_52w_high, relative_strength):
        """
        Determine the market stage (1-4) based on Minervini's stage analysis:

        Stage 1 - Basing/Accumulation: Consolidating after decline, preparing for advance
        Stage 2 - Advancing/Markup: Strong uptrend (ideal buy zone)
        Stage 3 - Topping/Distribution: Peaked and losing momentum
        Stage 4 - Declining/Markdown: Downtrend

        Args:
            close_price: Current closing price
            ma_50, ma_150, ma_200: Moving averages
            ma_200_trend: Trend of 200-day MA
            percent_from_52w_high: Distance from 52-week high
            relative_strength: RS score (0-100)

        Returns:
            int: Stage number (1, 2, 3, or 4)
        """

        # Check MA alignment
        price_above_200 = close_price > ma_200
        price_above_150 = close_price > ma_150
        price_above_50 = close_price > ma_50
        ma_50_above_150 = ma_50 > ma_150
        ma_150_above_200 = ma_150 > ma_200

        # Stage 2: Strong uptrend (Advancing)
        # Price above all MAs, MAs in correct order, strong momentum
        if (price_above_200 and price_above_150 and price_above_50 and
            ma_50_above_150 and ma_150_above_200 and
            ma_200_trend is not None and ma_200_trend > 0 and
            percent_from_52w_high >= -25 and
            relative_strength is not None and relative_strength >= 60):
            return 2

        # Stage 4: Downtrend (Declining)
        # Price below 200 MA, MAs in wrong order, weak RS
        if (not price_above_200 and
            ma_200_trend is not None and ma_200_trend < -0.5 and
            percent_from_52w_high < -30 and
            relative_strength is not None and relative_strength < 40):
            return 4

        # Stage 3: Topping/Distribution
        # Price still elevated but losing momentum, MAs flattening
        if (price_above_200 and
            (not ma_150_above_200 or not ma_50_above_150 or
             (ma_200_trend is not None and ma_200_trend < 0.5) or
             percent_from_52w_high < -15 or
             (relative_strength is not None and relative_strength < 60))):
            return 3

        # Stage 1: Basing/Accumulation (default for everything else)
        # Consolidating, MAs converging, preparing for next move
        return 1

    def calculate_entry_range(self, close_price, pivot_price, ema_10, ema_21):
        """
        Determine suggested entry price range based on Minervini principles.

        Two entry strategies:
        A) Pivot breakout: Buy as price clears pivot on volume
           Range: [pivot, pivot * 1.05] -- max 5% chase above pivot
        B) Pullback entry: Buy on pullback to 10-day or 21-day EMA
           (when already extended above pivot)

        Args:
            close_price: Current closing price
            pivot_price: VCP pivot / breakout price
            ema_10: 10-day Exponential Moving Average
            ema_21: 21-day Exponential Moving Average

        Returns:
            tuple: (entry_low, entry_high) price range
        """
        if not pivot_price:
            return None, None

        distance_from_pivot = ((close_price - pivot_price) / pivot_price) * 100

        if distance_from_pivot > 5:
            # Already extended above pivot -- suggest pullback entry to EMAs
            if ema_10 and ema_21:
                entry_low = min(ema_10, ema_21)
                entry_high = max(ema_10, ema_21)
            elif ema_21:
                entry_low = ema_21 * 0.99
                entry_high = ema_21 * 1.01
            elif ema_10:
                entry_low = ema_10 * 0.99
                entry_high = ema_10 * 1.01
            else:
                entry_low = pivot_price
                entry_high = pivot_price * 1.05
        else:
            # At or near pivot -- breakout entry
            entry_low = pivot_price
            entry_high = pivot_price * 1.05

        return round(entry_low, 2), round(entry_high, 2)

    def calculate_stop_loss(self, entry_price, atr_14, ema_21, last_contraction_low, swing_low):
        """
        Calculate stop loss using the tightest reasonable level from multiple methods.

        Minervini Stop Loss Methods:
        1. Maximum loss rule: 7-8% below entry (absolute hard floor)
        2. ATR-based: Entry - 2x ATR (volatility-adjusted)
        3. 21-EMA based: Just below 21-day EMA
        4. VCP contraction low: Below last contraction's low
        5. Swing low: Below most recent swing low

        Uses the HIGHEST (tightest) stop, clamped between 3% and 8%.

        Args:
            entry_price: Expected entry price
            atr_14: 14-day Average True Range
            ema_21: 21-day EMA
            last_contraction_low: Low of VCP's last contraction
            swing_low: Most recent swing low

        Returns:
            float: Suggested stop loss price
        """
        if not entry_price:
            return None

        # Method 1: Maximum loss rule (hard floor -- Minervini's absolute max)
        max_loss_stop = entry_price * 0.92  # 8% max loss

        stops = [max_loss_stop]

        # Method 2: ATR-based (2x ATR below entry)
        if atr_14:
            atr_stop = entry_price - (2.0 * atr_14)
            if atr_stop > 0:
                stops.append(atr_stop)

        # Method 3: Below 21-day EMA
        if ema_21 and ema_21 < entry_price:
            ema_stop = ema_21 * 0.99  # 1% below 21-EMA
            stops.append(ema_stop)

        # Method 4: VCP contraction low
        if last_contraction_low and last_contraction_low < entry_price:
            stops.append(last_contraction_low * 0.99)  # 1% below contraction low

        # Method 5: Swing low
        if swing_low and swing_low < entry_price:
            stops.append(swing_low * 0.99)  # 1% below swing low

        # Use the HIGHEST stop (tightest risk)
        stop = max(stops)

        # Clamp: don't tighter than 3% (avoid whipsaw from noise)
        min_stop = entry_price * 0.97
        if stop > min_stop:
            stop = min_stop

        # Clamp: never more than 8% loss (Minervini's absolute max)
        if stop < max_loss_stop:
            stop = max_loss_stop

        return round(stop, 2)

    def calculate_sell_targets(self, entry_price, stop_loss, ma_200):
        """
        Calculate sell/profit targets using Minervini's selling rules.

        Minervini Profit-Taking Rules:
        1. Risk-based: Target = Entry + N * Risk (R-multiples)
           - 2:1 minimum acceptable, 3:1 standard target
        2. Percentage: Take partial profits at +20-25%
        3. Climax warning: Price 70%+ above 200-day MA = extremely extended

        Args:
            entry_price: Expected entry price
            stop_loss: Calculated stop loss price
            ma_200: 200-day moving average

        Returns:
            dict: Sell target prices and risk/reward info, or None
        """
        if not entry_price or not stop_loss:
            return None

        risk = entry_price - stop_loss  # Dollar risk per share

        if risk <= 0:
            return None

        # R-multiple targets
        target_2r = entry_price + (2.0 * risk)  # 2:1 reward/risk (minimum)
        target_3r = entry_price + (3.0 * risk)  # 3:1 (standard Minervini)

        # Percentage-based targets (Minervini's 20-25% rule)
        target_20pct = entry_price * 1.20  # Partial profit at +20%
        target_25pct = entry_price * 1.25  # More at +25%

        # Primary target: lower of 3R and 25% (conservative approach)
        primary_target = min(target_3r, target_25pct)

        # Climax/overextension price (warning level)
        climax_price = ma_200 * 1.70 if ma_200 else None

        # Risk/reward ratio for primary target
        risk_reward = round((primary_target - entry_price) / risk, 1) if risk > 0 else None

        # Risk percentage
        risk_pct = round((risk / entry_price) * 100, 1)

        return {
            'target_conservative': round(target_2r, 2),
            'target_primary': round(primary_target, 2),
            'target_aggressive': round(target_25pct, 2),
            'partial_profit_at': round(target_20pct, 2),
            'climax_warning_price': round(climax_price, 2) if climax_price else None,
            'risk_reward_ratio': risk_reward,
            'risk_percent': risk_pct
        }

    def generate_signal(self, metrics):
        """
        Generate Buy/Wait/Pass signal with entry range, stop loss, and sell targets
        based on Mark Minervini's SEPA methodology.

        BUY:  All conditions favorable for immediate entry.
              Price at/near pivot, VCP confirmed, strong RS/earnings, no imminent earnings.
        WAIT: Setup forming but not yet actionable.
              Monitor for breakout, pullback entry, or earnings resolution.
        PASS: Avoid -- unfavorable technical or fundamental conditions.

        Args:
            metrics: Dict of all calculated metrics for a stock

        Returns:
            dict: Signal, reasons, entry range, stop loss, sell targets
        """
        close = metrics['close_price']
        pivot = metrics.get('pivot_price')
        stage = metrics['stage']
        rs = metrics.get('relative_strength')
        vcp = metrics.get('vcp_detected', False)
        vcp_score = metrics.get('vcp_score', 0) or 0
        vol_ratio = metrics.get('volume_ratio')
        passes = metrics['passes_minervini']
        criteria_count = metrics['criteria_passed']
        upcoming_earnings = metrics.get('has_upcoming_earnings', False)
        days_until = metrics.get('days_until_earnings')
        passes_earnings = metrics.get('passes_earnings')
        industry_rs = metrics.get('industry_rs')
        ema_10 = metrics.get('ema_10')
        ema_21 = metrics.get('ema_21')
        atr_14 = metrics.get('atr_14')
        ma_200 = metrics.get('ma_200')
        last_contraction_low = metrics.get('last_contraction_low')
        swing_low = metrics.get('swing_low')

        # Primary Base (IPO/new issue) context
        is_new_issue = metrics.get('is_new_issue', False)
        has_primary_base = metrics.get('has_primary_base')
        primary_base_status = metrics.get('primary_base_status', 'N/A')
        primary_base_weeks = metrics.get('primary_base_weeks')
        primary_base_correction = metrics.get('primary_base_correction_pct')

        signal = 'PASS'
        reasons = []

        # Calculate distance from pivot
        distance_from_pivot = None
        if pivot and pivot > 0:
            distance_from_pivot = ((close - pivot) / pivot) * 100

        # === BUY CONDITIONS ===
        # All technical criteria pass, proper setup confirmed, actionable entry
        # New issues must have a completed primary base before buying
        is_buy = (
            passes and
            stage == 2 and
            vcp and vcp_score >= 50 and
            rs is not None and rs >= 80 and
            distance_from_pivot is not None and -2 <= distance_from_pivot <= 5 and
            not (upcoming_earnings and days_until is not None and days_until <= 14) and
            passes_earnings is not False and
            (not is_new_issue or has_primary_base is True)
        )

        if is_buy:
            signal = 'BUY'
            reasons.append('All 9 trend criteria pass')
            reasons.append('Stage 2 uptrend confirmed')
            reasons.append(f'VCP pattern (score {vcp_score:.0f})')
            if vol_ratio and vol_ratio >= 1.5:
                reasons.append(f'Above-avg volume ({vol_ratio:.1f}x)')
            if metrics.get('earnings_acceleration'):
                reasons.append('Earnings accelerating')
            if industry_rs and industry_rs >= 60:
                reasons.append(f'Strong sector (RS {industry_rs:.0f})')

        # === WAIT CONDITIONS ===
        # Setup forming or conditions almost met -- monitor for entry
        elif stage in (1, 2) and criteria_count >= 7:
            signal = 'WAIT'

            if not passes:
                failed = metrics.get('criteria_failed', '')
                reasons.append(f'{criteria_count}/9 criteria met ({failed})')

            if vcp_score and 30 <= vcp_score < 50:
                reasons.append(f'VCP forming (score {vcp_score:.0f})')
            elif not vcp:
                reasons.append('No VCP pattern yet')

            if distance_from_pivot is not None:
                if distance_from_pivot < -5:
                    reasons.append(f'Price {abs(distance_from_pivot):.1f}% below pivot -- wait for breakout')
                elif distance_from_pivot > 5:
                    reasons.append(f'Extended {distance_from_pivot:.1f}% above pivot -- wait for pullback')

            if upcoming_earnings and days_until is not None and days_until <= 14:
                reasons.append(f'Earnings in {days_until} days -- wait')

            if rs is not None and 70 <= rs < 80:
                reasons.append(f'RS adequate ({rs:.0f}) but below preferred 80+')

            if passes_earnings is False:
                reasons.append('Earnings criteria not met -- watch for improvement')

            # Primary base context for new issues
            if is_new_issue and primary_base_status == 'FORMING':
                weeks_str = f'{primary_base_weeks:.0f}' if primary_base_weeks else '?'
                corr_str = f'{primary_base_correction:.0f}%' if primary_base_correction else '?'
                reasons.append(f'IPO primary base forming ({weeks_str}wk, {corr_str} correction)')

            if not reasons:
                reasons.append('Setup forming -- monitor for confirmation')

        # === PASS CONDITIONS ===
        else:
            if stage in (3, 4):
                reasons.append(f'Stage {stage} ({"topping/distribution" if stage == 3 else "declining"})')
            if criteria_count < 7:
                reasons.append(f'Only {criteria_count}/9 trend criteria met')
            if rs is not None and rs < 70:
                reasons.append(f'Weak relative strength ({rs:.0f})')
            if passes_earnings is False:
                reasons.append('Fails earnings criteria')

            # Primary base context for new issues
            if is_new_issue:
                if primary_base_status == 'TOO_EARLY':
                    reasons.append('New issue -- insufficient trading history for primary base')
                elif primary_base_status == 'FAILED':
                    corr_str = f'{primary_base_correction:.0f}%' if primary_base_correction else '?'
                    reasons.append(f'New issue -- correction too deep ({corr_str}) for primary base')

            if not reasons:
                reasons.append('Does not meet Minervini setup requirements')

        # === CALCULATE PRICE LEVELS for BUY and WAIT ===
        entry_low = entry_high = stop_loss = None
        sell_targets = None

        if signal in ('BUY', 'WAIT') and pivot:
            entry_low, entry_high = self.calculate_entry_range(
                close, pivot, ema_10, ema_21
            )

            # Use entry_low as reference for stop/target calculations
            reference_entry = entry_low if entry_low else pivot

            stop_loss = self.calculate_stop_loss(
                reference_entry, atr_14, ema_21,
                last_contraction_low, swing_low
            )

            if stop_loss:
                sell_targets = self.calculate_sell_targets(
                    reference_entry, stop_loss, ma_200
                )

        return {
            'signal': signal,
            'reasons': reasons,
            'entry_low': entry_low,
            'entry_high': entry_high,
            'stop_loss': stop_loss,
            'sell_targets': sell_targets,
        }
