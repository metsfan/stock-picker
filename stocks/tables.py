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
    stage = tables.Column(verbose_name='Stage')
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
            'stage',
            'vcp_detected',
            'vcp_score',
            'pivot_price',
            'passes_minervini',
            'criteria_passed',
            'ma_50',
            'ma_150', 
            'ma_200',
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
