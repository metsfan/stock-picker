from django.urls import path
from . import views

app_name = 'stocks'

urlpatterns = [
    path('', views.dashboard_view, name='dashboard'),
    path('stocks/', views.StockListView.as_view(), name='stock_list'),
    path('stocks/<str:symbol>/', views.stock_detail_view, name='stock_detail'),
    path('sectors/', views.hot_sectors_view, name='hot_sectors'),
    path('sectors/<str:sic_code>/', views.sector_detail_view, name='sector_detail'),
    path('watchlist/', views.watchlist_view, name='watchlist'),
    path('notifications/', views.notifications_view, name='notifications'),
    path('glossary/', views.glossary_view, name='glossary'),
    path('api/search/', views.search_stocks, name='search_stocks'),
    path('api/proxy-image/', views.proxy_company_image, name='proxy_company_image'),
    path('api/analyze/<str:symbol>/', views.analyze_stock_ai, name='analyze_stock_ai'),
    path('api/agent/<str:symbol>/', views.ask_ai_agent, name='ask_ai_agent'),
    path('api/watchlist/add/<str:symbol>/', views.add_to_watchlist, name='add_to_watchlist'),
    path('api/watchlist/remove/<str:symbol>/', views.remove_from_watchlist, name='remove_from_watchlist'),
    path('api/watchlist/check/<str:symbol>/', views.check_watchlist_status, name='check_watchlist_status'),
    path('api/notifications/<int:notification_id>/read/', views.mark_notification_read, name='mark_notification_read'),
    path('api/notifications/read-all/', views.mark_all_notifications_read, name='mark_all_notifications_read'),
]
