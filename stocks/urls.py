from django.urls import path
from . import views

app_name = 'stocks'

urlpatterns = [
    path('', views.dashboard_view, name='dashboard'),
    path('stocks/', views.StockListView.as_view(), name='stock_list'),
    path('stocks/<str:symbol>/', views.stock_detail_view, name='stock_detail'),
    path('watchlist/', views.watchlist_view, name='watchlist'),
    path('api/search/', views.search_stocks, name='search_stocks'),
    path('api/analyze/<str:symbol>/', views.analyze_stock_ai, name='analyze_stock_ai'),
    path('api/watchlist/add/<str:symbol>/', views.add_to_watchlist, name='add_to_watchlist'),
    path('api/watchlist/remove/<str:symbol>/', views.remove_from_watchlist, name='remove_from_watchlist'),
    path('api/watchlist/check/<str:symbol>/', views.check_watchlist_status, name='check_watchlist_status'),
]
