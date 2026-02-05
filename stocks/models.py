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
