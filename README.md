# Stock Picker - Minervini Trend Template Analyzer

This project downloads historical stock data and analyzes stocks using Mark Minervini's trend template criteria to identify strong momentum stocks.

## Setup

### Prerequisites

1. **WSL (Windows Subsystem for Linux)** must be installed
2. **AWS CLI** must be configured in WSL
3. **Python 3.7+** installed on Windows
4. **PostgreSQL** installed and running locally

### PostgreSQL Setup

The application requires a PostgreSQL database with the following configuration:
- Host: `localhost`
- Port: `5432`
- Database: `stocks`
- Username: `postgres`
- Password: `adamesk`

Create the database and set up tables:
```bash
# Create the database
createdb stocks

# Run the setup script to create tables and indices
psql -h localhost -U postgres -d stocks -f setup_database.sql
```

The `setup_database.sql` file contains all table definitions and schema changes. All future database modifications should be added to the bottom of this file.

### Installation

Install required Python packages:
```bash
pip install -r requirements.txt
```

## Usage

### Download Stock Data

Run the script to download data for the last 2 years:

```bash
cd scripts
python download_stock_data.py
```

The script will:
- Generate dates for the last 2 years (ending 2 days before today due to source data delay)
- Skip files that already exist locally (no re-downloading!)
- Download 10 files concurrently for efficiency
- **Load stock prices directly into PostgreSQL database**
- Check if date already exists in DB before loading
- Save files to the `day_aggs/` directory
- Show progress and success/failure for each date
- Display a summary at the end

**Note:** The source has a 24+ hour delay, so data stops 2 days before the current date.

### Output

Files are saved as uncompressed CSV files in the `day_aggs/` directory:
```
day_aggs/
  ├── 2024-02-03.csv     # Saturday - empty file (non-trading day)
  ├── 2024-02-04.csv     # Sunday - empty file (non-trading day)
  ├── 2024-02-05.csv     # Monday - contains stock data
  ├── 2024-02-06.csv     # Tuesday - contains stock data
  └── ...
```

**Note**: Empty (0 byte) CSV files indicate non-trading days (weekends, holidays, market closures). These are created automatically when the server returns a 404 error, preventing unnecessary retry attempts.

---

## Minervini Stock Analyzer

### What is Minervini's Trend Template?

The analyzer evaluates stocks based on Mark Minervini's trend template criteria, which identifies stocks in strong uptrends:

1. **Price above moving averages**: Stock > 150-day MA and 200-day MA
2. **MA alignment**: 150-day MA > 200-day MA
3. **200-day MA trending up**: Upward trend for at least 20 days
4. **50-day MA strong**: 50-day MA > 150-day MA and 200-day MA
5. **Price above 50-day MA**: Recent momentum
6. **Near 52-week high**: Within 25% of 52-week high
7. **Relative strength**: RS rating ≥ 70 (outperforming market)

### Stage Analysis

Each stock is classified into one of four market stages:

- **Stage 1 (Basing/Accumulation)**: Consolidating after a decline, preparing for advance. MAs converging, low volatility.
- **Stage 2 (Advancing/Markup)**: Strong uptrend - THE IDEAL BUY ZONE! This is where Minervini criteria shine. Price above all MAs, proper MA alignment, strong RS.
- **Stage 3 (Topping/Distribution)**: Peaked and losing momentum. Price may be elevated but momentum fading, MAs flattening.
- **Stage 4 (Declining/Markdown)**: Downtrend. Price below MAs, wrong MA alignment, weak RS, far from 52-week high.

### Relative Strength Calculation

Relative Strength (RS) compares each stock's 90-day performance against a **weighted average** of major market indices:

| Index | Weight | Description |
|-------|--------|-------------|
| **SPY** | 35% | S&P 500 ETF - Broad US large cap (most important) |
| **DIA** | 15% | Dow Jones Industrial Average ETF - Blue chip stocks |
| **QQQ** | 25% | Nasdaq-100 ETF - Tech-heavy index |
| **VTI** | 20% | Total US Stock Market ETF - Comprehensive coverage |
| **VT** | 5% | Total World Stock ETF - Global diversification |

The RS score (0-100) indicates how a stock performs relative to the weighted market average. A score of 70+ means the stock significantly outperforms the market. The market performance is calculated once per date and cached for efficiency across all stocks analyzed that day.

### Usage

Run the analyzer on the latest available data (at 8pm daily in production):

```bash
cd scripts
python minervini.py
```

By default, analyzes data from 2 days ago (latest available due to source delay).

The script will:
- Read stock price data from PostgreSQL database (loaded by download script)
- Calculate moving averages (50, 150, 200-day)
- Evaluate each stock against Minervini criteria
- Save analysis metrics to PostgreSQL database (`stocks`)
- Display top stocks passing all criteria

**Note:** The download script must be run first to populate the `stock_prices` table with price data.

### Database Schema

**PostgreSQL Tables:**

**stock_prices**: Raw price data
- symbol (VARCHAR), date (DATE), open, high, low, close (NUMERIC), volume (BIGINT)

**minervini_metrics**: Analysis results
- symbol (VARCHAR), date (DATE)
- ma_50, ma_150, ma_200 (NUMERIC) - moving averages
- week_52_high, week_52_low (NUMERIC) - 52-week ranges
- percent_from_52w_high, ma_200_trend_20d (NUMERIC) - trend metrics
- relative_strength (NUMERIC) - RS score (0-100)
- stage (INTEGER) - Market stage (1-4)
- passes_minervini (BOOLEAN) - Pass/fail status
- criteria_passed (INTEGER), criteria_failed (TEXT) - Criteria breakdown

### Query Examples

Connect to the database to run custom queries:

```bash
psql -h localhost -U postgres -d stocks
```

Find all stocks passing Minervini criteria:
```sql
SELECT symbol, close_price, relative_strength, stage, criteria_passed
FROM minervini_metrics
WHERE passes_minervini = 1 AND date = '2026-02-04'
ORDER BY relative_strength DESC;
```

Find Stage 2 stocks (ideal buy candidates):
```sql
SELECT symbol, close_price, relative_strength, criteria_passed
FROM minervini_metrics
WHERE stage = 2 AND date = '2026-02-04'
ORDER BY relative_strength DESC
LIMIT 50;
```

Track a specific stock over time:
```sql
SELECT date, close_price, ma_50, ma_150, ma_200, stage, passes_minervini
FROM minervini_metrics
WHERE symbol = 'AAPL'
ORDER BY date DESC
LIMIT 30;
```

### Expected CSV Format

The script expects daily CSV files with columns:
- `ticker`: Stock ticker symbol
- `open`, `high`, `low`, `close`: Price data
- `volume`: Trading volume
- `window_start`: Unix timestamp in nanoseconds (optional - date extracted from filename)
- `transactions`: Number of transactions (optional)

The date is automatically extracted from the filename (e.g., `2026-02-03.csv` → date = `2026-02-03`)

---

## Quick Query Tool

Use `query_stocks.py` for easy database queries:

### Show stocks passing Minervini criteria:
```bash
cd scripts
python query_stocks.py passing 20
```

### Show Stage 2 stocks (ideal buy zone):
```bash
cd scripts
python query_stocks.py stage 2 50
```

### View history for a specific stock:
```bash
cd scripts
python query_stocks.py stock AAPL 60
```

### Show overall statistics (includes stage distribution):
```bash
cd scripts
python query_stocks.py stats
```

---

## Typical Workflow

### Daily Workflow (run at 8pm):

1. **First Time Setup** (one-time only):
   ```bash
   psql -h localhost -U postgres -d stocks -f setup_database.sql
   ```

2. **Download & Load Data** (downloads files AND loads into database):
   ```bash
   cd scripts
   python download_stock_data.py
   ```
   This script:
   - Downloads new CSV files from S3
   - Automatically loads stock prices into PostgreSQL
   - Skips dates already in the database

3. **Analyze Stocks** (reads from database, calculates Minervini metrics):
   ```bash
   cd scripts
   python minervini.py
   ```
   - Truncates `minervini_metrics` table
   - Recalculates all metrics from `stock_prices`
   - Saves new analysis results

3. **Query Results**:
   ```bash
   cd scripts
   python query_stocks.py passing 50        # All passing stocks
   python query_stocks.py stage 2 100       # Stage 2 stocks (ideal buys)
   python query_stocks.py stock TSLA 30     # Track specific stock
   python query_stocks.py stats             # Overall statistics
   ```

### Backfilling Historical Data:

```bash
cd scripts
# Download last 30 days of data (loads to DB automatically)
# Note: Will stop 2 days before today due to source delay
python download_stock_data.py

# Analyze all loaded dates
python run_analysis.py last 30
```

### Important Notes:

- **Data Delay:** The source has a 24+ hour delay. Scripts automatically stop at "today minus 2 days" to ensure data availability.
- **"Today" means:** Latest available data (2 days ago), not the actual current date.
- When running at 8pm daily, you'll get data from 2 days prior.

4. **Build web app** (coming soon) to visualize the results!

---

## Database Management

### Initial Setup:
```bash
# Create database and tables
createdb stocks
psql -h localhost -U postgres -d stocks -f setup_database.sql
```

### View all tables:
```bash
psql -h localhost -U postgres -d stocks -c "\dt"
```

### Clear data (if needed):
```bash
# Clear only analysis results (keeps price data)
psql -h localhost -U postgres -d stocks -c "TRUNCATE TABLE minervini_metrics CASCADE"

# Clear all data including price data
psql -h localhost -U postgres -d stocks -c "TRUNCATE TABLE minervini_metrics, stock_prices CASCADE"
```

### Schema Updates:
When making schema changes, add them to the bottom of `setup_database.sql` with a comment and date. This maintains a migration history.

### Notes

- **Resume Support**: Run the script multiple times - it automatically skips files that already exist
- **Non-Trading Days**: When a file returns 404 (weekends, holidays, market closures), the script creates an empty dummy file to mark it as checked. This prevents retrying non-trading days on subsequent runs.
- Each file download has a 5-minute timeout
- Progress is displayed in real-time with status indicators:
  - ✓ = Successfully downloaded
  - ⊙ = Skipped (already exists or non-trading day)
  - ✗ = Failed (error other than 404)
- Uses ThreadPoolExecutor for concurrent downloads (10 workers)
