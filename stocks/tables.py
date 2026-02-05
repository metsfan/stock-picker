import django_tables2 as tables
from django_tables2.utils import A
from django.utils.safestring import mark_safe
from .models import MinerviniMetrics


class StockTable(tables.Table):
    """Django table for displaying stock metrics"""
    
    watchlist = tables.Column(empty_values=(), orderable=False, verbose_name='')
    symbol = tables.Column(linkify=True, attrs={'td': {'class': 'font-weight-bold'}})
    close_price = tables.Column(verbose_name='Price')
    relative_strength = tables.Column(verbose_name='RS')
    industry_rs = tables.Column(verbose_name='Ind RS')
    stage = tables.Column(verbose_name='Stage')
    return_3m = tables.Column(verbose_name='3M %')
    avg_dollar_volume = tables.Column(verbose_name='$Vol')
    volume_ratio = tables.Column(verbose_name='Vol Ratio')
    atr_percent = tables.Column(verbose_name='ATR%')
    is_52w_high = tables.BooleanColumn(verbose_name='New High', yesno='★,')
    vcp_score = tables.Column(verbose_name='VCP Score')
    vcp_detected = tables.BooleanColumn(verbose_name='VCP', yesno='✓,')
    passes_minervini = tables.BooleanColumn(verbose_name='Passes', yesno='✓,✗')
    criteria_passed = tables.Column(verbose_name='Score')
    pivot_price = tables.Column(verbose_name='Pivot')
    
    class Meta:
        model = MinerviniMetrics
        template_name = 'django_tables2/bootstrap5.html'
        fields = (
            'watchlist',
            'symbol', 
            'close_price', 
            'relative_strength',
            'industry_rs',
            'stage',
            'return_3m',
            'avg_dollar_volume',
            'volume_ratio',
            'atr_percent',
            'is_52w_high',
            'vcp_detected',
            'vcp_score',
            'pivot_price',
            'passes_minervini',
            'criteria_passed',
            'percent_from_52w_high',
        )
        attrs = {
            'class': 'table table-striped table-hover',
            'id': 'stock-table'
        }
        per_page = 50
    
    def render_watchlist(self, record):
        """Render watchlist star icon"""
        return mark_safe(
            f'<button class="btn btn-sm btn-outline-warning watchlist-toggle" '
            f'data-symbol="{record.symbol}" title="Add to Watchlist">'
            f'<span class="watchlist-icon">☆</span>'
            f'</button>'
        )
    
    def render_close_price(self, value):
        if value is not None:
            return f'${value:,.2f}'
        return '-'
    
    def render_pivot_price(self, value):
        if value is not None:
            return f'${value:,.2f}'
        return '-'
    
    def render_relative_strength(self, value):
        if value is not None:
            rs = float(value)
            if rs >= 80:
                return mark_safe(f'<span class="badge bg-success">{rs:.0f}</span>')
            elif rs >= 70:
                return mark_safe(f'<span class="badge bg-info">{rs:.0f}</span>')
            elif rs >= 60:
                return mark_safe(f'<span class="badge bg-warning">{rs:.0f}</span>')
            else:
                return mark_safe(f'<span class="badge bg-secondary">{rs:.0f}</span>')
        return '-'
    
    def render_stage(self, value, record):
        if value is not None:
            stage_colors = {1: 'secondary', 2: 'success', 3: 'warning', 4: 'danger'}
            stage_names = {1: 'Basing', 2: 'Advancing', 3: 'Topping', 4: 'Declining'}
            color = stage_colors.get(value, 'secondary')
            name = stage_names.get(value, 'Unknown')
            return mark_safe(f'<span class="badge bg-{color}">{value} - {name}</span>')
        return '-'
    
    def render_vcp_score(self, value):
        if value is not None:
            score = float(value)
            if score >= 70:
                return mark_safe(f'<span class="badge bg-success">{score:.0f}</span>')
            elif score >= 60:
                return mark_safe(f'<span class="badge bg-info">{score:.0f}</span>')
            elif score >= 50:
                return mark_safe(f'<span class="badge bg-warning">{score:.0f}</span>')
            else:
                return mark_safe(f'<span class="badge bg-secondary">{score:.0f}</span>')
        return '-'
    
    def render_percent_from_52w_high(self, value):
        if value is not None:
            pct = float(value)
            color = 'success' if pct >= -10 else 'warning' if pct >= -25 else 'danger'
            return mark_safe(f'<span class="text-{color}">{pct:+.1f}%</span>')
        return '-'
    
    def render_criteria_passed(self, value):
        if value is not None:
            return f'{value}/9'
        return '-'
    
    def render_industry_rs(self, value):
        """Render industry relative strength with color coding"""
        if value is not None:
            ind_rs = float(value)
            if ind_rs >= 70:
                return mark_safe(f'<span class="badge bg-success">{ind_rs:.0f}</span>')
            elif ind_rs >= 50:
                return mark_safe(f'<span class="badge bg-info">{ind_rs:.0f}</span>')
            else:
                return mark_safe(f'<span class="badge bg-secondary">{ind_rs:.0f}</span>')
        return '-'
    
    def render_return_3m(self, value):
        """Render 3-month return with color coding"""
        if value is not None:
            ret = float(value)
            if ret > 0:
                return mark_safe(f'<span class="text-success">{ret:+.1f}%</span>')
            else:
                return mark_safe(f'<span class="text-danger">{ret:+.1f}%</span>')
        return '-'
    
    def render_avg_dollar_volume(self, value):
        """Render average dollar volume in millions"""
        if value is not None:
            vol_m = value / 1_000_000
            if vol_m >= 50:
                return mark_safe(f'<span class="text-success">${vol_m:.1f}M</span>')
            elif vol_m >= 20:
                return mark_safe(f'<span class="text-info">${vol_m:.1f}M</span>')
            else:
                return mark_safe(f'<span class="text-warning">${vol_m:.1f}M</span>')
        return '-'
    
    def render_volume_ratio(self, value):
        """Render volume ratio with color coding"""
        if value is not None:
            ratio = float(value)
            if ratio >= 1.5:
                return mark_safe(f'<span class="badge bg-success">{ratio:.1f}x</span>')
            elif ratio >= 1.0:
                return mark_safe(f'<span class="badge bg-info">{ratio:.1f}x</span>')
            elif ratio >= 0.7:
                return mark_safe(f'{ratio:.1f}x')
            else:
                return mark_safe(f'<span class="text-muted">{ratio:.1f}x</span>')
        return '-'
    
    def render_atr_percent(self, value):
        """Render ATR percentage (volatility)"""
        if value is not None:
            atr = float(value)
            if atr >= 5:
                return mark_safe(f'<span class="text-danger">{atr:.1f}%</span>')
            elif atr >= 3:
                return mark_safe(f'<span class="text-warning">{atr:.1f}%</span>')
            else:
                return mark_safe(f'<span class="text-success">{atr:.1f}%</span>')
        return '-'
