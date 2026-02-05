"""
Example configuration file for Stock Picker application.
Copy this file to config.py and update with your actual credentials.
"""

# Claude AI API Configuration
CLAUDE_API_KEY = "your-api-key-here"  # Get from https://console.anthropic.com/
CLAUDE_MODEL = "claude-opus-4-20250514"  # Claude Opus 4.5

# Available models:
# "claude-opus-4-20250514" - Opus 4.5 (most capable, best for comprehensive analysis with news/events)
# "claude-sonnet-4-20250514" - Sonnet 4 (balanced speed and capability)
# "claude-3-5-sonnet-20241022" - Sonnet 3.5 (fast and very capable)
# "claude-3-opus-20240229" - Opus 3 (previous generation)

# Analysis Features:
# The AI analysis will consider:
# - Technical metrics (Minervini criteria, VCP patterns)
# - Recent news and current events
# - Earnings reports and financial announcements
# - Industry trends and competitive landscape
# - Macroeconomic factors
# - Market sentiment
