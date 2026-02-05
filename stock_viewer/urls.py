"""
URL configuration for stock_viewer project.
"""
from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('stocks.urls')),
]
