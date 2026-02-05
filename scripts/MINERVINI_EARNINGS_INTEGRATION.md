# Minervini Earnings Integration

## Overview

The `minervini.py` script has been enhanced to integrate earnings and income statement data from the `earnings` and `income_statements` tables. This adds fundamental analysis capabilities based on Mark Minervini's SEPA (Specific Entry Point Analysis) criteria.

## New Features

### 1. Earnings Growth Analysis

Calculates key earnings metrics from income statements:

- **YoY EPS Growth**: Year-over-year earnings per share growth percentage
- **QoQ EPS Growth**: Quarter-over-quarter growth (acceleration indicator)
- **Revenue Growth**: Year-over-year revenue growth
- **Earnings Acceleration**: Boolean flag when EPS growth is accelerating (getting better each quarter)

**Minervini Requirement**: 25%+ quarterly EPS growth, with preference for accelerating earnings.

### 2. Earnings Surprise History

Analyzes the last 4 quarterly earnings reports:

- **Average EPS Surprise**: Mean percentage beat/miss vs estimates
- **Beat Rate**: Percentage of quarters where actual EPS beat estimates
- **Consistency**: Flags stocks that consistently beat estimates

**Minervini Preference**: Stocks that consistently beat analyst estimates demonstrate strong, reliable fundamentals.

### 3. Upcoming Earnings Detection

Checks for earnings announcements within the next 14 days:

- **Warning System**: Flags stocks with upcoming earnings
- **Days Until**: Shows countdown to next earnings date
- **Risk Management**: Minervini typically avoids buying within 2 weeks of earnings

### 4. Earnings Quality Score

Overall score (0-100) combining all earnings factors:

- 30 points: YoY EPS growth (25%+ = max points)
- 20 points: QoQ growth acceleration
- 20 points: Earnings acceleration pattern
- 20 points: Consistent estimate beats
- 10 points: Revenue growth

**Passes Earnings Criteria**: Boolean flag for stocks meeting Minervini's earnings requirements.

## Database Schema Changes

New columns added to `minervini_metrics` table:

```sql
-- Earnings/Fundamental metrics
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS eps_growth_yoy NUMERIC(10, 2);
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS eps_growth_qoq NUMERIC(10, 2);
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS revenue_growth_yoy NUMERIC(10, 2);
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS earnings_acceleration BOOLEAN;
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS avg_eps_surprise NUMERIC(10, 2);
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS earnings_beat_rate NUMERIC(5, 2);
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS has_upcoming_earnings BOOLEAN;
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS days_until_earnings INTEGER;
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS earnings_quality_score INTEGER;
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS passes_earnings BOOLEAN;
```

## How It Works

### Analysis Flow

1. **Technical Screening**: First applies Minervini's technical criteria (moving averages, RS, etc.)
2. **Earnings Evaluation**: For passing stocks, evaluates earnings quality
3. **Combined Filter**: Stocks must pass BOTH technical and fundamental criteria
4. **Prioritization**: Stocks are ranked by:
   - Earnings quality (passes_earnings)
   - Earnings quality score
   - Relative strength
   - Industry RS

### Example Output

```
✓ AAPL: PASSED - Stage 2 (9/9 criteria) [VCP Score: 85] [Large Cap⭐] [EPS: +32%🚀]
✓ NVDA: PASSED - Stage 2 (9/9 criteria) [Mid Cap⭐] [EPS: +78%🚀] ⚠️ Earnings in 5d
```

**Symbols**:
- 🚀 = Earnings accelerating (each quarter better than last)
- ⚠️ = Upcoming earnings within 14 days

### Top Stocks Table

The enhanced output table now includes:

```
Symbol   Price    RS  IndRS  EPSyy  EPSqq  Beat%  ErnQual   3M%   $Vol(M)  VCP  Earn
NVDA     $850.25  95    88   +78%  +42%🚀   100%   95/100  +45.2%    5420.3   92    ✓
AAPL     $175.50  92    85   +32%  +18%     75%   82/100  +22.1%    8932.1   -    ✓
```

**Columns**:
- **EPSyy**: EPS Year-over-Year growth
- **EPSqq**: EPS Quarter-over-Quarter growth (with 🚀 for acceleration)
- **Beat%**: Earnings beat rate (last 4 quarters)
- **ErnQual**: Earnings quality score (0-100)
- **Earn**: Earnings status (✓=passes, ✗=fails, days=upcoming)

## Setup & Usage

### 1. Update Database Schema

```bash
psql -U postgres -d stocks -f setup_database.sql
```

This adds the new earnings columns to the `minervini_metrics` table.

### 2. Fetch Earnings Data

Make sure you have earnings and income statement data populated:

```bash
# Fetch upcoming earnings (Benzinga API)
python scripts/fetch_upcoming_earnings.py

# Fetch historical earnings (optional, for surprises)
python scripts/fetch_earnings.py

# Fetch income statements (for EPS/revenue growth)
python scripts/fetch_income_statements.py
```

### 3. Run Minervini Analysis

```bash
python scripts/minervini.py
```

The script will automatically:
- Calculate earnings metrics for all stocks
- Apply earnings filters to passing stocks
- Display earnings data in output
- Store earnings metrics in database

## Filtering Behavior

### With Earnings Data

If a stock has earnings data available:
- **Must pass technical criteria** (Minervini's 9 criteria)
- **Must pass earnings criteria** (25%+ growth, positive trends)
- Stocks with weak earnings but strong technicals are **demoted** (not fully disqualified)

### Without Earnings Data

If a stock has no earnings data:
- **Technical criteria only** (original Minervini screening)
- No earnings penalty (we can't evaluate what we don't have)
- These stocks still appear in results but without earnings metrics

### Upcoming Earnings Warning

- Stocks with earnings in 7 days or less are **flagged** but not disqualified
- Helps with risk management (avoid buying right before earnings)
- Minervini's rule: Don't buy within 2 weeks of earnings

## Querying Earnings Metrics

### Find stocks with strong earnings

```sql
SELECT symbol, eps_growth_yoy, eps_growth_qoq, earnings_quality_score
FROM minervini_metrics
WHERE date = '2026-02-03'
AND passes_minervini = true
AND passes_earnings = true
AND eps_growth_yoy >= 25
ORDER BY earnings_quality_score DESC;
```

### Find accelerating earnings

```sql
SELECT symbol, eps_growth_qoq, earnings_acceleration
FROM minervini_metrics
WHERE date = '2026-02-03'
AND earnings_acceleration = true
AND passes_minervini = true
ORDER BY eps_growth_qoq DESC;
```

### Check upcoming earnings

```sql
SELECT symbol, has_upcoming_earnings, days_until_earnings
FROM minervini_metrics
WHERE date = '2026-02-03'
AND passes_minervini = true
AND has_upcoming_earnings = true
ORDER BY days_until_earnings ASC;
```

## API Methods

### `get_earnings_growth(symbol)`

Returns earnings growth metrics:
```python
{
    'latest_eps': 2.45,
    'latest_revenue': 85000000000,
    'qoq_eps_growth': 18.5,
    'yoy_eps_growth': 32.1,
    'yoy_revenue_growth': 12.4,
    'earnings_acceleration': True,
    'quarters_available': 5
}
```

### `get_earnings_surprises(symbol, num_quarters=4)`

Returns earnings surprise history:
```python
{
    'total_earnings': 4,
    'beats': 3,
    'misses': 1,
    'avg_eps_surprise': 5.2,
    'avg_revenue_surprise': 2.1,
    'all_beats': False,
    'mostly_beats': True
}
```

### `check_upcoming_earnings(symbol, date, days_ahead=14)`

Returns upcoming earnings info:
```python
{
    'has_upcoming_earnings': True,
    'earnings_date': '2026-02-10',
    'days_until': 5,
    'estimated_eps': 2.15,
    'date_status': 'confirmed',
    'importance': 5
}
```

### `evaluate_earnings_quality(symbol, date)`

Returns comprehensive earnings evaluation:
```python
{
    'has_earnings_data': True,
    'passes_earnings_criteria': True,
    'earnings_score': 85,
    'issues': [],
    'growth': {...},
    'surprises': {...},
    'upcoming': {...}
}
```

## Minervini's SEPA Criteria

The earnings integration follows Mark Minervini's Specific Entry Point Analysis (SEPA):

1. **Fundamental Requirements**:
   - 25%+ quarterly EPS growth (year-over-year)
   - Consistent revenue growth
   - Earnings acceleration preferred

2. **Quality Signals**:
   - Consistent earnings beats
   - Positive earnings surprises
   - Accelerating growth rates

3. **Risk Management**:
   - Avoid buying before earnings (2-week buffer)
   - Prefer stocks with reliable earnings patterns
   - Higher bar for micro-caps (need higher RS)

## Performance Impact

- **Minimal**: Earnings queries are lightweight (indexed tables)
- **Cached**: Earnings metrics stored in `minervini_metrics` table
- **Optional**: Stocks without earnings data are not penalized
- **Fast**: Concurrent processing unchanged (~20-30 min for 3000+ tickers)

## Troubleshooting

### No earnings data showing

Check if income statements are populated:
```sql
SELECT COUNT(*) FROM income_statements;
```

If count is 0, run:
```bash
python scripts/fetch_income_statements.py
```

### Earnings quality score is 0

This means:
- No earnings data available for the stock, OR
- Stock has negative/declining earnings

Check raw data:
```sql
SELECT * FROM income_statements WHERE ticker = 'AAPL' ORDER BY period_end DESC LIMIT 5;
```

### All stocks failing earnings criteria

This is normal if:
- Market is in a downturn (earnings declining)
- You're analyzing value stocks (steady but not high-growth)
- Income statement data is incomplete

Adjust the filter in code if needed (e.g., lower the 25% threshold).

## References

- **Mark Minervini's Books**: "Trade Like a Stock Market Wizard", "Think & Trade Like a Champion"
- **SEPA**: Specific Entry Point Analysis (fundamental + technical screening)
- **VCP**: Volatility Contraction Pattern (price pattern Minervini looks for)
