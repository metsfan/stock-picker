# Earnings & Financial Data Scripts

Scripts for fetching and managing earnings and financial statement data from Massive/Polygon APIs.

## Overview

Two complementary data sources:

1. **Benzinga Earnings API** - Analyst estimates, surprises, upcoming dates
2. **Income Statements API** - Actual financial results from SEC filings

Together, these provide complete fundamental analysis capabilities.

### When to Use Which?

| Need | Use Benzinga | Use Income Statements | Best Approach |
|------|-------------|----------------------|---------------|
| Upcoming earnings dates | ✅ | ❌ | Benzinga only |
| Analyst estimates | ✅ | ❌ | Benzinga only |
| Beat/miss patterns | ✅ | ❌ | Benzinga only |
| Actual EPS values | ⚠️ (delayed) | ✅ | Income Statements |
| Actual revenue | ⚠️ (sometimes) | ✅ | Income Statements |
| QoQ/YoY growth | ❌ | ✅ | Income Statements |
| Profit margins | ❌ | ✅ | Income Statements |
| Operating expenses | ❌ | ✅ | Income Statements |
| Earnings surprise % | ✅ | ❌ | Benzinga only |
| **Minervini Screening** | ✅ + ✅ | ✅ + ✅ | **Use BOTH!** |

**Best Practice:** Use Income Statements for growth calculations, Benzinga for surprise patterns and upcoming dates.

## Prerequisites

1. **Database Setup**
   ```bash
   psql -h localhost -U postgres -d stocks -f setup_database.sql
   ```

2. **API Key**
   - Ensure your Massive API key is stored in `~/.massive-api/api_key.txt`
   - This should already be set up from `daily_tickers.py`

## Scripts

### Part 1: Benzinga Earnings Data

#### 1. `fetch_earnings.py` - Historical Earnings Fetcher

Fetches **3 years of historical earnings** for all stocks in your database.

**Run this ONCE initially**, then periodically (monthly) to catch up on new earnings.

```bash
cd scripts
python fetch_earnings.py
```

**What it does:**
- Fetches last 12 quarters of earnings for each ticker
- Stores: Actual EPS, Estimated EPS, Revenue, Surprises
- Takes ~30-60 minutes for 5000+ tickers
- Shows progress every 100 tickers

**Output:**
```
Benzinga Earnings Data Fetcher
================================
✓ Database connected
✓ Found 5234 tickers
Fetching earnings data (2023-02-05 to 2026-02-05)...

[1/5234] ✓ AAPL: 12 earnings records
[2/5234] ✓ MSFT: 12 earnings records
...
```

#### 2. `fetch_upcoming_earnings.py` - Upcoming Earnings Calendar

Fetches **upcoming earnings for the next 60 days**.

**Run this DAILY** or weekly to stay current on earnings dates.

```bash
cd scripts
python fetch_upcoming_earnings.py
```

**What it does:**
- Fetches next 60 days of earnings announcements
- Updates estimates as they change
- Highlights stocks in your portfolio with upcoming earnings
- Shows earnings calendar

**Output:**
```
Upcoming Earnings Fetcher (Next 60 Days)
========================================
✓ Found 847 upcoming earnings announcements

Upcoming Earnings Calendar:
  2026-02-06: 23 companies
  2026-02-07: 45 companies
  2026-02-08: 38 companies
  ...

⚠️  WARNING: Stocks Passing Minervini with Earnings in Next 14 Days:
  NVDA: 2026-02-12 at 16:00 (confirmed) - Est EPS: $0.75
  META: 2026-02-14 at 16:00 (confirmed) - Est EPS: $5.23
  
  → Consider exiting or tightening stops before earnings!
```

### Part 2: Income Statements (SEC Filings)

#### 3. `fetch_income_statements.py` - Financial Statements Fetcher

Fetches **quarterly and annual financial statements from SEC filings** for the last 5 years.

**Run this ONCE initially**, then monthly to update.

```bash
cd scripts
python fetch_income_statements.py
```

**What it does:**
- Fetches last 20 quarters (~5 years) per ticker
- **Processes 10 tickers concurrently** for faster execution
- Stores actual financial results (EPS, revenue, expenses)
- Calculates YoY growth automatically
- Shows top growth stocks
- ~20-30 minutes for 3000+ tickers (faster than sequential)

**What you get:**
- Actual EPS (basic & diluted)
- Revenue & revenue growth
- Operating income, gross profit
- Operating expenses (R&D, SG&A)
- Profit margins
- Shares outstanding

**Output:**
```
Income Statements Fetcher (SEC Filings)
========================================
✓ Database connected
✓ Found 3,847 tickers with CIK

[1/3847] ✓ AAPL: 20 quarters | YoY EPS Growth: +12.3%
[2/3847] ✓ NVDA: 20 quarters | YoY EPS Growth: +168.4%
[3/3847] ✓ MSFT: 20 quarters | YoY EPS Growth: +15.7%
...

Growth Statistics:
  Stocks with YoY data: 2,134
  Average EPS growth: 18.3%
  Average revenue growth: 14.2%
  Stocks with 25%+ EPS growth: 487

Top 10 EPS Growth Stocks (YoY):
  Ticker    Latest EPS    Year Ago     Growth
  NVDA           $5.16       $3.08     +67.5%
  AMD            $0.92       $0.46    +100.0%
  ...
```

## Database Schema

### `earnings` table (Benzinga API)

Stores earnings announcements with analyst estimates:

| Field | Description |
|-------|-------------|
| `ticker` | Stock symbol |
| `date` | Earnings announcement date |
| `fiscal_period` | Q1, Q2, Q3, Q4, FY |
| `fiscal_year` | Calendar year |
| `actual_eps` | Reported EPS |
| `estimated_eps` | Analyst consensus |
| `eps_surprise_percent` | % beat/miss |
| `actual_revenue` | Reported revenue |
| `estimated_revenue` | Analyst estimate |
| `revenue_surprise_percent` | % beat/miss |
| `previous_eps` | Prior period EPS |
| `date_status` | confirmed/projected |
| `importance` | 0-5 (event importance) |

### `income_statements` table (SEC Filings)

Stores actual financial results from quarterly/annual reports:

| Field | Description |
|-------|-------------|
| `ticker` | Stock symbol |
| `period_end` | Reporting period end date |
| `fiscal_quarter` | 1, 2, 3, or 4 |
| `fiscal_year` | Fiscal year |
| `timeframe` | quarterly, annual, trailing_twelve_months |
| `basic_earnings_per_share` | Actual EPS reported |
| `diluted_earnings_per_share` | Diluted EPS |
| `revenue` | Total revenue |
| `gross_profit` | Revenue - COGS |
| `operating_income` | Operating profit |
| `net_income_loss_attributable_common_shareholders` | Net income |
| `basic_shares_outstanding` | Shares outstanding |
| `selling_general_administrative` | SG&A expenses |
| `research_development` | R&D expenses |
| `ebitda` | EBITDA |

## Usage Workflow

### Initial Setup (One-time)
```bash
# 1. Update database schema
psql -h localhost -U postgres -d stocks -f setup_database.sql

# 2. Fetch historical earnings announcements (~30-60 min)
python scripts/fetch_earnings.py

# 3. Fetch income statements from SEC filings (~30-45 min)
python scripts/fetch_income_statements.py
```

### Regular Maintenance

**Daily:**
```bash
python scripts/fetch_upcoming_earnings.py  # Update earnings calendar
```

**Monthly:**
```bash
python scripts/fetch_earnings.py           # Update historical earnings
python scripts/fetch_income_statements.py  # Update financial statements
```

## Integration with Minervini Analysis

Once both earnings and income statement data is loaded, you can:

### From Benzinga Earnings:
1. **Filter by Beat History** - Find consistent estimate beaters
2. **Avoid Pre-Earnings Trades** - Check upcoming dates  
3. **Earnings Surprise Patterns** - Identify momentum stocks

### From Income Statements:
1. **Calculate EPS Growth** - QoQ and YoY growth rates
2. **Calculate Revenue Growth** - Top-line growth analysis
3. **Identify Acceleration** - Earnings getting better each quarter
4. **Profit Margin Analysis** - Operational efficiency trends
5. **Compare to Estimates** - Cross-reference with Benzinga data

### Combined Power:
```sql
-- Find Minervini-quality earnings growth stocks
SELECT 
    i.ticker,
    i.basic_earnings_per_share as latest_eps,
    LAG(i.basic_earnings_per_share, 4) OVER (
        PARTITION BY i.ticker ORDER BY i.period_end
    ) as eps_year_ago,
    e.eps_surprise_percent as latest_surprise
FROM income_statements i
LEFT JOIN earnings e ON i.ticker = e.ticker 
    AND i.period_end = e.date
WHERE i.timeframe = 'quarterly'
AND i.basic_earnings_per_share > 0
ORDER BY i.period_end DESC;
```

See `minervini.py` integration (coming soon) for automatic earnings-based filtering.

## Troubleshooting

### "No earnings data" for many tickers
- This is normal - not all tickers report earnings (ETFs, bonds, etc.)
- Newly listed companies may have limited history
- Success rate ~40-60% is expected

### API Rate Limits
- Scripts include automatic rate limiting
- Batch processing with checkpoints
- Resume-friendly (can Ctrl+C and restart)

### Database Errors
- Ensure `setup_database.sql` was run
- Check PostgreSQL is running
- Verify credentials in scripts

## Data Freshness

- **Historical earnings**: Update monthly
- **Upcoming earnings**: Update daily (estimates change frequently)
- **After earnings release**: Run historical fetch to get latest actuals

## Cost Considerations

- Benzinga API is included in your Stocks Advanced plan
- No additional cost per request
- Historical fetch is one-time cost
- Daily updates are lightweight (~50-100 upcoming earnings)

## Next Steps

After loading earnings data:

1. Run `minervini.py` to see enhanced analysis with earnings filters
2. Check Django web interface for earnings display
3. Set up cron jobs for daily updates:
   ```bash
   # Add to crontab -e
   0 8 * * * cd /path/to/stock-picker && python scripts/fetch_upcoming_earnings.py
   ```
