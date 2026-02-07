#!/usr/bin/env python3
"""
Minervini Stock Analyzer - Entry Point

Backward-compatible wrapper that delegates to the minervini package.
All analysis logic lives in the minervini/ package modules.

Usage:
    python minervini.py
"""

from minervini import MinerviniAnalyzer
from minervini.analyzer import main

# Re-export MinerviniAnalyzer at module level for importlib compatibility
# (used by run_analysis.py)

if __name__ == "__main__":
    main()
