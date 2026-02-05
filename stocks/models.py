from django.db import models
from django.urls import reverse
from django.utils import timezone


class StockPrice(models.Model):
    """Model for daily stock price data (maps to existing stock_prices table)"""
    
    symbol = models.CharField(max_length=20)
    date = models.DateField()
    open = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    high = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    low = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    close = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    volume = models.BigIntegerField(null=True)
    
    class Meta:
        db_table = 'stock_prices'
        managed = False  # Don't let Django manage this table
        ordering = ['-date']
        unique_together = [['symbol', 'date']]
    
    def __str__(self):
        return f"{self.symbol} - {self.date}: ${self.close}"


class MinerviniMetrics(models.Model):
    """Model for Minervini analysis metrics (maps to existing minervini_metrics table)"""
    
    symbol = models.CharField(max_length=20)
    date = models.DateField()
    close_price = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    ma_50 = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    ma_150 = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    ma_200 = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    week_52_high = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    week_52_low = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    percent_from_52w_high = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    ma_200_trend_20d = models.DecimalField(max_digits=10, decimal_places=4, null=True)
    relative_strength = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    stage = models.IntegerField(null=True)
    passes_minervini = models.BooleanField(default=False)
    criteria_passed = models.IntegerField(null=True)
    criteria_failed = models.TextField(null=True, blank=True)
    
    # VCP fields
    vcp_detected = models.BooleanField(default=False)
    vcp_score = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    contraction_count = models.IntegerField(null=True)
    latest_contraction_pct = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    volume_contraction = models.BooleanField(default=False)
    pivot_price = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    
    # Enhanced metrics
    avg_dollar_volume = models.BigIntegerField(null=True)
    volume_ratio = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    return_1m = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    return_3m = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    return_6m = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    return_12m = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    atr_14 = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    atr_percent = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    is_52w_high = models.BooleanField(default=False)
    days_since_52w_high = models.IntegerField(null=True)
    industry_rs = models.DecimalField(max_digits=10, decimal_places=2, null=True)

    class Meta:
        db_table = 'minervini_metrics'
        managed = False  # Don't let Django manage this table
        ordering = ['-relative_strength', '-vcp_score']
        verbose_name = 'Minervini Metric'
        verbose_name_plural = 'Minervini Metrics'
        unique_together = [['symbol', 'date']]
    
    def __str__(self):
        return f"{self.symbol} - {self.date}"
    
    def get_absolute_url(self):
        """Return URL for stock detail page"""
        return reverse('stocks:stock_detail', kwargs={'symbol': self.symbol})
    
    @property
    def stage_name(self):
        """Human-readable stage name"""
        stage_names = {
            1: "Basing",
            2: "Advancing", 
            3: "Topping",
            4: "Declining"
        }
        return stage_names.get(self.stage, "Unknown")
    
    @property
    def rs_rating(self):
        """Relative strength rating category"""
        if self.relative_strength is None:
            return "N/A"
        rs = float(self.relative_strength)
        if rs >= 80:
            return "Excellent"
        elif rs >= 70:
            return "Strong"
        elif rs >= 60:
            return "Good"
        elif rs >= 50:
            return "Average"
        else:
            return "Weak"
    
    @property
    def avg_dollar_volume_formatted(self):
        """Format average dollar volume in millions"""
        if not self.avg_dollar_volume:
            return "N/A"
        return f"${self.avg_dollar_volume / 1_000_000:.1f}M"
    
    @property
    def volume_status(self):
        """Volume ratio status indicator"""
        if not self.volume_ratio:
            return "N/A"
        ratio = float(self.volume_ratio)
        if ratio >= 1.5:
            return "High"
        elif ratio >= 1.0:
            return "Above Avg"
        elif ratio >= 0.7:
            return "Normal"
        else:
            return "Low"
    
    @property
    def momentum_trend(self):
        """Overall momentum trend based on multi-timeframe returns"""
        returns = [self.return_1m, self.return_3m, self.return_6m, self.return_12m]
        valid_returns = [float(r) for r in returns if r is not None]
        
        if not valid_returns:
            return "N/A"
        
        positive_count = sum(1 for r in valid_returns if r > 0)
        
        if positive_count == len(valid_returns):
            return "Strong Up"
        elif positive_count >= len(valid_returns) * 0.75:
            return "Up"
        elif positive_count >= len(valid_returns) * 0.5:
            return "Mixed"
        else:
            return "Down"
    
    @property
    def volatility_rating(self):
        """Volatility rating based on ATR percentage"""
        if not self.atr_percent:
            return "N/A"
        atr = float(self.atr_percent)
        if atr >= 5:
            return "High"
        elif atr >= 3:
            return "Medium"
        else:
            return "Low"
    
    @property
    def industry_rs_rating(self):
        """Industry relative strength rating"""
        if not self.industry_rs:
            return "N/A"
        ind_rs = float(self.industry_rs)
        if ind_rs >= 70:
            return "Leading"
        elif ind_rs >= 50:
            return "Average"
        else:
            return "Lagging"


class AIAnalysis(models.Model):
    """Model for storing AI-generated stock analyses"""
    
    symbol = models.CharField(max_length=20)
    analysis_text = models.TextField()
    model_used = models.CharField(max_length=100)
    generated_at = models.DateTimeField(default=timezone.now)
    data_date = models.DateField()
    
    class Meta:
        db_table = 'ai_analyses'
        managed = False  # Don't let Django manage this table
        ordering = ['-generated_at']
        verbose_name = 'AI Analysis'
        verbose_name_plural = 'AI Analyses'
        unique_together = [['symbol', 'data_date', 'model_used']]
    
    def __str__(self):
        return f"{self.symbol} - {self.model_used} - {self.generated_at.strftime('%Y-%m-%d %H:%M')}"
    
    @property
    def age_hours(self):
        """How many hours old is this analysis"""
        # Ensure generated_at is timezone-aware
        generated_time = self.generated_at
        if timezone.is_naive(generated_time):
            generated_time = timezone.make_aware(generated_time)
        
        delta = timezone.now() - generated_time
        return delta.total_seconds() / 3600
    
    @property
    def is_stale(self):
        """Is this analysis more than 24 hours old"""
        return self.age_hours > 24


class Watchlist(models.Model):
    """Model for user's stock watchlist"""
    
    symbol = models.CharField(max_length=20, unique=True)
    added_at = models.DateTimeField(default=timezone.now)
    notes = models.TextField(blank=True, null=True)
    
    class Meta:
        db_table = 'watchlist'
        managed = False  # Don't let Django manage this table
        ordering = ['-added_at']
        verbose_name = 'Watchlist Item'
        verbose_name_plural = 'Watchlist'
    
    def __str__(self):
        return f"{self.symbol} (added {self.added_at.strftime('%Y-%m-%d')})"


class SectorPerformance(models.Model):
    """Model for sector/industry performance tracking"""
    
    date = models.DateField()
    sic_code = models.CharField(max_length=10)
    sic_description = models.CharField(max_length=255, null=True, blank=True)
    sector_return_90d = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    sector_rs = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    stock_count = models.IntegerField(null=True)
    
    class Meta:
        db_table = 'sector_performance'
        managed = False
        ordering = ['-sector_rs']
        verbose_name = 'Sector Performance'
        verbose_name_plural = 'Sector Performance'
        unique_together = [['date', 'sic_code']]
    
    def __str__(self):
        return f"{self.sic_description or self.sic_code} - {self.date}"
    
    @property
    def sector_strength(self):
        """Sector strength rating"""
        if not self.sector_rs:
            return "N/A"
        rs = float(self.sector_rs)
        if rs >= 70:
            return "Leading"
        elif rs >= 50:
            return "Average"
        else:
            return "Lagging"


class TickerDetails(models.Model):
    """Model for detailed ticker information from Massive API"""
    
    symbol = models.CharField(max_length=20, primary_key=True)
    name = models.CharField(max_length=255, null=True, blank=True)
    description = models.TextField(null=True, blank=True)
    market_cap = models.BigIntegerField(null=True, blank=True)
    homepage_url = models.CharField(max_length=500, null=True, blank=True)
    logo_url = models.CharField(max_length=500, null=True, blank=True)
    icon_url = models.CharField(max_length=500, null=True, blank=True)
    primary_exchange = models.CharField(max_length=20, null=True, blank=True)
    locale = models.CharField(max_length=10, null=True, blank=True)
    market = models.CharField(max_length=50, null=True, blank=True)
    currency_name = models.CharField(max_length=10, null=True, blank=True)
    active = models.BooleanField(null=True, blank=True)
    list_date = models.DateField(null=True, blank=True)
    sic_code = models.CharField(max_length=10, null=True, blank=True)
    sic_description = models.CharField(max_length=255, null=True, blank=True)
    total_employees = models.IntegerField(null=True, blank=True)
    share_class_shares_outstanding = models.BigIntegerField(null=True, blank=True)
    weighted_shares_outstanding = models.BigIntegerField(null=True, blank=True)
    cik = models.CharField(max_length=20, null=True, blank=True)
    composite_figi = models.CharField(max_length=20, null=True, blank=True)
    share_class_figi = models.CharField(max_length=20, null=True, blank=True)
    phone_number = models.CharField(max_length=50, null=True, blank=True)
    ticker_type = models.CharField(max_length=10, null=True, blank=True, db_column='ticker_type')
    round_lot = models.IntegerField(null=True, blank=True)
    address_line1 = models.CharField(max_length=255, null=True, blank=True)
    address_city = models.CharField(max_length=100, null=True, blank=True)
    address_state = models.CharField(max_length=50, null=True, blank=True)
    address_postal_code = models.CharField(max_length=20, null=True, blank=True)
    updated_at = models.DateTimeField(default=timezone.now)
    
    class Meta:
        db_table = 'ticker_details'
        managed = False  # Don't let Django manage this table
        ordering = ['-market_cap']
        verbose_name = 'Ticker Detail'
        verbose_name_plural = 'Ticker Details'
    
    def __str__(self):
        return f"{self.symbol} - {self.name or 'Unknown'}"
    
    @property
    def market_cap_formatted(self):
        """Format market cap in billions or millions"""
        if not self.market_cap:
            return "N/A"
        
        if self.market_cap >= 1_000_000_000:
            return f"${self.market_cap / 1_000_000_000:.2f}B"
        elif self.market_cap >= 1_000_000:
            return f"${self.market_cap / 1_000_000:.2f}M"
        else:
            return f"${self.market_cap:,.0f}"
