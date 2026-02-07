import django_tables2 as tables
from django_tables2.utils import A
from django.utils.safestring import mark_safe
from .models import MinerviniMetrics


class StockTable(tables.Table):
    """Django table for displaying stock metrics with signal data"""
    
    watchlist = tables.Column(empty_values=(), orderable=False, verbose_name='')
    signal = tables.Column(verbose_name='Signal', order_by=('signal',))
    symbol = tables.Column(linkify=True, attrs={'td': {'class': 'font-weight-bold'}})
    close_price = tables.Column(verbose_name='Price')
    relative_strength = tables.Column(verbose_name='RS')
    stage = tables.Column(verbose_name='Stage')
    entry_range = tables.Column(empty_values=(), orderable=False, verbose_name='Entry Range')
    stop_loss = tables.Column(verbose_name='Stop')
    sell_target_primary = tables.Column(verbose_name='Target')
    risk_reward_ratio = tables.Column(verbose_name='R:R')
    risk_percent = tables.Column(verbose_name='Risk%')
    vcp_detected = tables.BooleanColumn(verbose_name='VCP', yesno='✓,')
    vcp_score = tables.Column(verbose_name='VCP Score')
    pivot_price = tables.Column(verbose_name='Pivot')
    industry_rs = tables.Column(verbose_name='Ind RS')
    return_3m = tables.Column(verbose_name='3M %')
    passes_minervini = tables.BooleanColumn(verbose_name='Passes', yesno='✓,✗')
    criteria_passed = tables.Column(verbose_name='Score')
    is_52w_high = tables.BooleanColumn(verbose_name='New High', yesno='★,')
    avg_dollar_volume = tables.Column(verbose_name='$Vol')
    percent_from_52w_high = tables.Column(verbose_name='From High')
    
    class Meta:
        model = MinerviniMetrics
        template_name = 'django_tables2/bootstrap5.html'
        fields = (
            'watchlist',
            'signal',
            'symbol', 
            'close_price', 
            'relative_strength',
            'stage',
            'entry_range',
            'stop_loss',
            'sell_target_primary',
            'risk_reward_ratio',
            'risk_percent',
            'vcp_detected',
            'vcp_score',
            'pivot_price',
            'industry_rs',
            'return_3m',
            'passes_minervini',
            'criteria_passed',
            'is_52w_high',
            'avg_dollar_volume',
            'percent_from_52w_high',
        )
        attrs = {
            'class': 'table table-striped table-hover',
            'id': 'stock-table'
        }
        per_page = 50
        order_by = '-relative_strength'
    
    def render_watchlist(self, record):
        """Render watchlist star icon"""
        return mark_safe(
            f'<button class="btn btn-sm btn-outline-warning watchlist-toggle" '
            f'data-symbol="{record.symbol}" title="Add to Watchlist">'
            f'<span class="watchlist-icon">☆</span>'
            f'</button>'
        )
    
    def render_signal(self, value):
        """Render signal as a colored badge"""
        if value:
            colors = {
                'BUY': 'success',
                'WAIT': 'warning',
                'PASS': 'danger',
            }
            icons = {
                'BUY': '🟢',
                'WAIT': '🟡',
                'PASS': '🔴',
            }
            color = colors.get(value, 'secondary')
            icon = icons.get(value, '')
            text_class = ' text-dark' if value == 'WAIT' else ''
            return mark_safe(
                f'<span class="badge bg-{color}{text_class}" '
                f'style="font-size:0.85em;letter-spacing:0.5px">'
                f'{icon} {value}</span>'
            )
        return mark_safe('<span class="text-muted">-</span>')
    
    def render_close_price(self, value):
        if value is not None:
            return f'${value:,.2f}'
        return '-'
    
    def render_entry_range(self, record):
        """Render combined entry range from entry_low and entry_high"""
        if record.entry_low is not None and record.entry_high is not None:
            return mark_safe(
                f'<span class="text-nowrap">'
                f'${record.entry_low:,.2f}'
                f'<span class="text-muted"> – </span>'
                f'${record.entry_high:,.2f}'
                f'</span>'
            )
        return mark_safe('<span class="text-muted">-</span>')
    
    def render_stop_loss(self, value):
        if value is not None:
            return mark_safe(f'<span class="text-danger">${value:,.2f}</span>')
        return mark_safe('<span class="text-muted">-</span>')
    
    def render_sell_target_primary(self, value):
        if value is not None:
            return mark_safe(f'<span class="text-success">${value:,.2f}</span>')
        return mark_safe('<span class="text-muted">-</span>')
    
    def render_risk_reward_ratio(self, value):
        if value is not None:
            rr = float(value)
            if rr >= 3.0:
                color = 'success'
            elif rr >= 2.0:
                color = 'info'
            else:
                color = 'warning'
            return mark_safe(f'<span class="badge bg-{color}">{rr:.1f}:1</span>')
        return mark_safe('<span class="text-muted">-</span>')
    
    def render_risk_percent(self, value):
        if value is not None:
            risk = float(value)
            if risk <= 4:
                color = 'success'
            elif risk <= 6:
                color = 'warning'
            else:
                color = 'danger'
            return mark_safe(f'<span class="text-{color}">{risk:.1f}%</span>')
        return mark_safe('<span class="text-muted">-</span>')
    
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