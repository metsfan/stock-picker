from django.contrib import admin
from .models import MinerviniMetrics


@admin.register(MinerviniMetrics)
class MinerviniMetricsAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'date', 'close_price', 'relative_strength', 'stage', 
                    'passes_minervini', 'vcp_detected', 'vcp_score')
    list_filter = ('date', 'passes_minervini', 'vcp_detected', 'stage')
    search_fields = ('symbol',)
    ordering = ('-date', '-relative_strength')
    
    def has_add_permission(self, request):
        return False  # Read-only
    
    def has_delete_permission(self, request, obj=None):
        return False  # Read-only
