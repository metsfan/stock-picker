# Stock Picker Data Pipeline

Automated pipeline to fetch all stock data and run Minervini analysis.

## Scripts Execution Order

The pipeline runs these scripts in sequence:

1. **`fetch_stock_prices.py`** - Fetch split-adjusted OHLCV data (15 months)
2. **`fetch_earnings.py`** - Fetch historical earnings data
3. **`fetch_upcoming_earnings.py`** - Fetch upcoming earnings calendar
4. **`fetch_income_statements.py`** - Fetch financial statements
5. **`minervini.py`** - Run Minervini trend template analysis

If any step fails, the entire pipeline stops immediately.

## Usage

### Linux/Mac/WSL

```bash
cd scripts
./run_pipeline.sh
```

Or from project root:
```bash
./scripts/run_pipeline.sh
```

Make executable if needed:
```bash
chmod +x scripts/run_pipeline.sh
```

### Windows

```cmd
cd scripts
run_pipeline.bat
```

Or from project root:
```cmd
scripts\run_pipeline.bat
```

## Docker Usage

To run the pipeline in Docker:

```bash
docker-compose run --rm web python scripts/run_pipeline.sh
```

Or run individual steps:
```bash
docker-compose run --rm web python scripts/fetch_stock_prices.py
docker-compose run --rm web python scripts/minervini.py
```

## Prerequisites

- PostgreSQL database running and accessible
- API key configured in `~/.massive-api/api_key.txt`
- Python dependencies installed (`pip install -r requirements.txt`)

## Environment Variables

The scripts use these database connection settings (with defaults):

- `DB_HOST` (default: `localhost`)
- `DB_PORT` (default: `5432`)
- `DB_NAME` (default: `stocks`)
- `DB_USER` (default: `postgres`)
- `DB_PASSWORD` (default: `adamesk`)

## Expected Runtime

Full pipeline runtime (approximate, with 10 concurrent requests):

- **fetch_stock_prices.py**: 2-4 hours (10,000+ tickers × 15 months)
- **fetch_earnings.py**: 1-2 hours (historical data)
- **fetch_upcoming_earnings.py**: < 5 minutes (single bulk fetch)
- **fetch_income_statements.py**: 30-60 minutes
- **minervini.py**: 10-15 minutes (analysis only)

**Total: ~4-7 hours** for complete refresh

## Daily Updates

For daily incremental updates, run the pipeline script once per day. Each script is designed to be idempotent:

- `fetch_stock_prices.py` - Full refresh (could be optimized for daily incremental)
- `fetch_earnings.py` - Only fetches tickers with stale data (>90 days old)
- `fetch_upcoming_earnings.py` - Full refresh of next 60 days
- `fetch_income_statements.py` - Only fetches tickers with missing/stale data
- `minervini.py` - Analyzes latest available date

## Scheduling

### Linux/Mac (cron)

Run daily at 6 PM:
```bash
0 18 * * * cd /path/to/stock-picker/scripts && ./run_pipeline.sh >> /var/log/stock-picker.log 2>&1
```

### Windows (Task Scheduler)

1. Open Task Scheduler
2. Create Basic Task
3. Set trigger (e.g., daily at 6 PM)
4. Action: Start a program
5. Program: `cmd.exe`
6. Arguments: `/c "cd C:\path\to\stock-picker\scripts && run_pipeline.bat"`

## Troubleshooting

**Pipeline fails at Step 1 (fetch_stock_prices.py):**
- Check API key: `cat ~/.massive-api/api_key.txt`
- Verify API quota/limits
- Check network connectivity

**Pipeline fails at Step 5 (minervini.py):**
- Ensure previous steps completed successfully
- Check if database has sufficient data (>252 trading days for 52-week high/low)

**Database connection errors:**
- Verify PostgreSQL is running: `pg_isready`
- Check connection settings in scripts
- Ensure database `stocks` exists

## Logs

Both scripts output colored console logs. To save logs:

**Linux/Mac:**
```bash
./run_pipeline.sh 2>&1 | tee pipeline.log
```

**Windows:**
```cmd
run_pipeline.bat > pipeline.log 2>&1
```
