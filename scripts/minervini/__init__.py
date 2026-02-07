"""
Minervini Stock Analyzer Package

Analyzes stocks based on Mark Minervini's Trend Template Criteria.
(Reference: "Trade Like a Stock Market Wizard" by Mark Minervini)
"""

from .analyzer import MinerviniAnalyzer
from .notifications import NotificationManager

__all__ = ['MinerviniAnalyzer', 'NotificationManager']
