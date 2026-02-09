-- Stock Picker Database Schema
-- Run this script to set up the PostgreSQL database
-- 
-- Usage: psql -h localhost -U postgres -d stocks -f setup_database.sql

-- ============================================================================
-- INITIAL SCHEMA
-- ============================================================================

-- Table: stock_prices
-- Stores daily OHLCV data for all stocks
CREATE TABLE IF NOT EXISTS stock_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    open NUMERIC(10, 2),
    high NUMERIC(10, 2),
    low NUMERIC(10, 2),
    close NUMERIC(10, 2),
    volume BIGINT,
    UNIQUE(symbol, date)
);

-- Table: minervini_metrics
-- Stores calculated Minervini analysis metrics
CREATE TABLE IF NOT EXISTS minervini_metrics (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    close_price NUMERIC(10, 2),
    ma_50 NUMERIC(10, 2),
    ma_150 NUMERIC(10, 2),
    ma_200 NUMERIC(10, 2),
    week_52_high NUMERIC(10, 2),
    week_52_low NUMERIC(10, 2),
    percent_from_52w_high NUMERIC(10, 2),
    ma_200_trend_20d NUMERIC(10, 4),
    relative_strength NUMERIC(10, 2),
    stage INTEGER,
    passes_minervini BOOLEAN,
    criteria_passed INTEGER,
    criteria_failed TEXT,
    UNIQUE(symbol, date)
);

-- ============================================================================
-- INDICES
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_symbol_date ON stock_prices(symbol, date);
CREATE INDEX IF NOT EXISTS idx_metrics_symbol_date ON minervini_metrics(symbol, date);
CREATE INDEX IF NOT EXISTS idx_passes_minervini ON minervini_metrics(passes_minervini);
CREATE INDEX IF NOT EXISTS idx_stage ON minervini_metrics(stage);

-- ============================================================================
-- SCHEMA UPDATES
-- Add future schema changes below this line with comments and dates
-- ============================================================================

-- 2026-02-05: Add VCP (Volatility Contraction Pattern) columns
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS vcp_detected BOOLEAN DEFAULT FALSE;
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS vcp_score NUMERIC(5, 2);
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS contraction_count INTEGER;
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS latest_contraction_pct NUMERIC(5, 2);
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS volume_contraction BOOLEAN DEFAULT FALSE;
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS pivot_price NUMERIC(10, 2);

-- Index for VCP queries
CREATE INDEX IF NOT EXISTS idx_vcp_detected ON minervini_metrics(vcp_detected);

-- 2026-02-05: Add AI Analysis storage table
CREATE TABLE IF NOT EXISTS ai_analyses (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    analysis_text TEXT NOT NULL,
    model_used VARCHAR(100) NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    data_date DATE NOT NULL,
    UNIQUE(symbol, data_date, model_used)
);

-- Indices for AI analysis queries
CREATE INDEX IF NOT EXISTS idx_ai_analyses_symbol ON ai_analyses(symbol);
CREATE INDEX IF NOT EXISTS idx_ai_analyses_generated_at ON ai_analyses(generated_at DESC);

-- 2026-02-05: Add Watchlist table
CREATE TABLE IF NOT EXISTS watchlist (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL UNIQUE,
    added_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    notes TEXT
);

-- Index for watchlist queries
CREATE INDEX IF NOT EXISTS idx_watchlist_symbol ON watchlist(symbol);
CREATE INDEX IF NOT EXISTS idx_watchlist_added_at ON watchlist(added_at DESC);

-- 2026-02-05: Add Ticker Details table (from Massive API)
CREATE TABLE IF NOT EXISTS ticker_details (
    symbol VARCHAR(20) PRIMARY KEY,
    name VARCHAR(255),
    description TEXT,
    market_cap BIGINT,
    homepage_url VARCHAR(500),
    logo_url VARCHAR(500),
    icon_url VARCHAR(500),
    primary_exchange VARCHAR(20),
    locale VARCHAR(10),
    market VARCHAR(50),
    currency_name VARCHAR(10),
    active BOOLEAN,
    list_date DATE,
    sic_code VARCHAR(10),
    sic_description VARCHAR(255),
    total_employees INTEGER,
    share_class_shares_outstanding BIGINT,
    weighted_shares_outstanding BIGINT,
    cik VARCHAR(20),
    composite_figi VARCHAR(20),
    share_class_figi VARCHAR(20),
    phone_number VARCHAR(50),
    ticker_type VARCHAR(10),
    round_lot INTEGER,
    address_line1 VARCHAR(255),
    address_city VARCHAR(100),
    address_state VARCHAR(50),
    address_postal_code VARCHAR(20),
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Index for ticker details queries
CREATE INDEX IF NOT EXISTS idx_ticker_details_symbol ON ticker_details(symbol);
CREATE INDEX IF NOT EXISTS idx_ticker_details_market_cap ON ticker_details(market_cap DESC);
CREATE INDEX IF NOT EXISTS idx_ticker_details_active ON ticker_details(active);
CREATE INDEX IF NOT EXISTS idx_ticker_details_updated_at ON ticker_details(updated_at DESC);

-- ============================================================================
-- 2026-02-05: Add Earnings Data table (Benzinga API)
-- Stores historical and upcoming earnings announcements
-- ============================================================================

CREATE TABLE IF NOT EXISTS earnings (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    time TIME,
    fiscal_year INTEGER,
    fiscal_period VARCHAR(10),
    currency VARCHAR(10),
    actual_eps NUMERIC,
    estimated_eps NUMERIC,
    eps_surprise NUMERIC,
    eps_surprise_percent NUMERIC,
    eps_method VARCHAR(10),
    actual_revenue BIGINT,
    estimated_revenue BIGINT,
    revenue_surprise BIGINT,
    revenue_surprise_percent NUMERIC,
    revenue_method VARCHAR(10),
    previous_eps NUMERIC,
    previous_revenue BIGINT,
    company_name VARCHAR(255),
    importance INTEGER,
    date_status VARCHAR(20),
    notes TEXT,
    benzinga_id VARCHAR(50),
    last_updated TIMESTAMPTZ,
    fetched_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, date, fiscal_period, fiscal_year)
);

-- Indices for earnings queries
CREATE INDEX IF NOT EXISTS idx_earnings_ticker ON earnings(ticker);
CREATE INDEX IF NOT EXISTS idx_earnings_date ON earnings(date DESC);
CREATE INDEX IF NOT EXISTS idx_earnings_ticker_date ON earnings(ticker, date DESC);
CREATE INDEX IF NOT EXISTS idx_earnings_fiscal ON earnings(fiscal_year DESC, fiscal_period);
CREATE INDEX IF NOT EXISTS idx_earnings_surprise ON earnings(eps_surprise_percent DESC) WHERE eps_surprise_percent IS NOT NULL;

-- ============================================================================
-- 2026-02-05: Add Income Statements table (Financials API)
-- Stores quarterly/annual financial statements from SEC filings
-- ============================================================================

CREATE TABLE IF NOT EXISTS income_statements (
    id SERIAL PRIMARY KEY,
    cik VARCHAR(20) NOT NULL,
    tickers TEXT[],  -- Array of ticker symbols
    ticker VARCHAR(20),  -- Primary ticker (extracted from array)
    period_end DATE NOT NULL,
    filing_date DATE,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    timeframe VARCHAR(30),  -- quarterly, annual, trailing_twelve_months
    
    -- Revenue & Profitability
    revenue BIGINT,
    cost_of_revenue BIGINT,
    gross_profit BIGINT,
    operating_income BIGINT,
    net_income_loss_attributable_common_shareholders BIGINT,
    consolidated_net_income_loss BIGINT,
    
    -- Earnings Per Share
    basic_earnings_per_share NUMERIC,
    diluted_earnings_per_share NUMERIC,
    basic_shares_outstanding BIGINT,
    diluted_shares_outstanding BIGINT,
    
    -- Operating Expenses
    total_operating_expenses BIGINT,
    selling_general_administrative BIGINT,
    research_development BIGINT,
    depreciation_depletion_amortization BIGINT,
    other_operating_expenses BIGINT,
    
    -- Other Income/Expenses
    total_other_income_expense BIGINT,
    interest_income BIGINT,
    interest_expense BIGINT,
    other_income_expense BIGINT,
    
    -- Tax & Special Items
    income_before_income_taxes BIGINT,
    income_taxes BIGINT,
    discontinued_operations BIGINT,
    extraordinary_items BIGINT,
    
    -- Calculated Metrics
    ebitda BIGINT,
    
    -- Other
    equity_in_affiliates BIGINT,
    noncontrolling_interest BIGINT,
    preferred_stock_dividends_declared BIGINT,
    
    fetched_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(cik, period_end, timeframe)
);

-- Indices for income statement queries
CREATE INDEX IF NOT EXISTS idx_income_stmt_ticker ON income_statements(ticker);
CREATE INDEX IF NOT EXISTS idx_income_stmt_period ON income_statements(period_end DESC);
CREATE INDEX IF NOT EXISTS idx_income_stmt_ticker_period ON income_statements(ticker, period_end DESC);
CREATE INDEX IF NOT EXISTS idx_income_stmt_fiscal ON income_statements(fiscal_year DESC, fiscal_quarter);
CREATE INDEX IF NOT EXISTS idx_income_stmt_timeframe ON income_statements(timeframe);
CREATE INDEX IF NOT EXISTS idx_income_stmt_cik ON income_statements(cik);

-- ============================================================================
-- 2026-02-05: Enhanced Minervini Metrics
-- Additional technical indicators calculated from existing price/volume data
-- ============================================================================

-- Liquidity metrics
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS avg_dollar_volume BIGINT;  -- 50-day avg dollar volume
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS volume_ratio NUMERIC(5, 2);  -- Today's volume vs 50-day avg

-- Multi-timeframe momentum (percentage returns)
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS return_1m NUMERIC(10, 2);   -- 1-month return %
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS return_3m NUMERIC(10, 2);   -- 3-month return %
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS return_6m NUMERIC(10, 2);   -- 6-month return %
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS return_12m NUMERIC(10, 2);  -- 12-month return %

-- Volatility metrics
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS atr_14 NUMERIC(10, 2);      -- 14-day Average True Range
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS atr_percent NUMERIC(5, 2);  -- ATR as % of price (volatility)

-- New high tracking
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS is_52w_high BOOLEAN DEFAULT FALSE;
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS days_since_52w_high INTEGER;

-- Industry/Sector relative strength
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS industry_rs NUMERIC(10, 2);  -- Industry group RS score

-- Earnings/Fundamental metrics (integrated from earnings and income_statements tables)
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS eps_growth_yoy NUMERIC(10, 2);      -- Year-over-year EPS growth %
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS eps_growth_qoq NUMERIC(10, 2);      -- Quarter-over-quarter EPS growth %
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS revenue_growth_yoy NUMERIC(10, 2);  -- Year-over-year revenue growth %
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS earnings_acceleration BOOLEAN;      -- Earnings accelerating
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS avg_eps_surprise NUMERIC(10, 2);    -- Avg EPS surprise % (last 4 qtrs)
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS earnings_beat_rate NUMERIC(5, 2);   -- % of earnings beats (last 4 qtrs)
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS has_upcoming_earnings BOOLEAN;      -- Earnings within 14 days
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS days_until_earnings INTEGER;        -- Days until next earnings
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS earnings_quality_score INTEGER;     -- Overall earnings quality (0-100)
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS passes_earnings BOOLEAN;            -- Passes Minervini earnings criteria

-- Indices for new columns
CREATE INDEX IF NOT EXISTS idx_avg_dollar_volume ON minervini_metrics(avg_dollar_volume DESC);
CREATE INDEX IF NOT EXISTS idx_industry_rs ON minervini_metrics(industry_rs DESC);
CREATE INDEX IF NOT EXISTS idx_is_52w_high ON minervini_metrics(is_52w_high);
CREATE INDEX IF NOT EXISTS idx_eps_growth_yoy ON minervini_metrics(eps_growth_yoy DESC);
CREATE INDEX IF NOT EXISTS idx_earnings_quality ON minervini_metrics(earnings_quality_score DESC);
CREATE INDEX IF NOT EXISTS idx_passes_earnings ON minervini_metrics(passes_earnings);
CREATE INDEX IF NOT EXISTS idx_upcoming_earnings ON minervini_metrics(has_upcoming_earnings);

-- Sector performance tracking table (aggregated from individual stocks)
CREATE TABLE IF NOT EXISTS sector_performance (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    sic_code VARCHAR(10) NOT NULL,
    sic_description VARCHAR(255),
    sector_return_90d NUMERIC(10, 2),  -- 90-day sector return
    sector_rs NUMERIC(10, 2),          -- Sector relative strength vs market
    stock_count INTEGER,               -- Number of stocks in sector
    UNIQUE (date, sic_code)
);

CREATE INDEX IF NOT EXISTS idx_sector_perf_date ON sector_performance(date);
CREATE INDEX IF NOT EXISTS idx_sector_perf_rs ON sector_performance(sector_rs DESC);

-- ============================================================================
-- 2026-02-05: Minervini Criteria Corrections
-- Added missing criteria columns for proper 8-criterion trend template
-- ============================================================================

-- 150-day MA trend (slope) - Minervini criterion #3
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS ma_150_trend_20d NUMERIC(10, 4);

-- Percent from 52-week low - Minervini criterion #7 (must be 30%+ above)
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS percent_from_52w_low NUMERIC(10, 2);

-- Index for the new columns
CREATE INDEX IF NOT EXISTS idx_ma_150_trend ON minervini_metrics(ma_150_trend_20d);
CREATE INDEX IF NOT EXISTS idx_pct_from_52w_low ON minervini_metrics(percent_from_52w_low DESC);

-- ============================================================================
-- 2026-02-06: Buy/Wait/Pass Signal System
-- Adds Minervini signal generation with entry ranges, stop losses, and targets
-- ============================================================================

-- Short-term EMAs for entry/stop calculations
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS ema_10 NUMERIC(10, 2);       -- 10-day EMA (tight trailing stop)
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS ema_21 NUMERIC(10, 2);       -- 21-day EMA (pullback entry level)

-- VCP pattern stop loss anchor
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS last_contraction_low NUMERIC(10, 2);  -- Low of VCP's tightest contraction

-- Swing low for pattern-based stop loss
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS swing_low NUMERIC(10, 2);    -- Most recent swing low

-- Signal: BUY, WAIT, or PASS
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS signal VARCHAR(10);           -- BUY / WAIT / PASS
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS signal_reasons TEXT;           -- Explanation for the signal

-- Entry price range (suggested buy zone)
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS entry_low NUMERIC(10, 2);     -- Lower bound of entry range
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS entry_high NUMERIC(10, 2);    -- Upper bound of entry range

-- Stop loss
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS stop_loss NUMERIC(10, 2);     -- Suggested stop loss price

-- Sell targets (profit-taking levels)
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS sell_target_conservative NUMERIC(10, 2);  -- 2:1 R/R target
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS sell_target_primary NUMERIC(10, 2);       -- Min(3:1 R/R, +25%)
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS sell_target_aggressive NUMERIC(10, 2);    -- +25% target
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS partial_profit_at NUMERIC(10, 2);         -- +20% partial profit level

-- Risk metrics
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS risk_reward_ratio NUMERIC(5, 1);  -- R:R ratio for primary target
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS risk_percent NUMERIC(5, 1);       -- Downside risk as % of entry

-- Indices for signal queries
CREATE INDEX IF NOT EXISTS idx_signal ON minervini_metrics(signal);
CREATE INDEX IF NOT EXISTS idx_signal_date ON minervini_metrics(date, signal);
CREATE INDEX IF NOT EXISTS idx_buy_signals ON minervini_metrics(date, signal) WHERE signal = 'BUY';
CREATE INDEX IF NOT EXISTS idx_wait_signals ON minervini_metrics(date, signal) WHERE signal = 'WAIT';

-- ============================================================
-- Notifications table
-- Stores actionable alerts generated during Minervini analysis
-- for stocks on the watchlist.
-- ============================================================
CREATE TABLE IF NOT EXISTS notifications (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    notification_type VARCHAR(30) NOT NULL,   -- 'WAIT_TO_BUY', 'METRIC_CHANGE', 'EARNINGS_SURPRISE'
    title VARCHAR(200) NOT NULL,
    message TEXT NOT NULL,
    metadata JSONB,                           -- old/new values, contextual data
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    is_read BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_notifications_symbol ON notifications(symbol);
CREATE INDEX IF NOT EXISTS idx_notifications_date ON notifications(date);
CREATE INDEX IF NOT EXISTS idx_notifications_type ON notifications(notification_type);
CREATE INDEX IF NOT EXISTS idx_notifications_unread ON notifications(is_read) WHERE is_read = FALSE;

-- ============================================================================
-- 2026-02-08: Primary Base detection for IPOs/new issues (Minervini)
-- Tracks whether recent new issues have formed a proper primary base before
-- they are considered buyable. Tiered correction rules based on base duration.
-- ============================================================================

ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS is_new_issue BOOLEAN;
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS has_primary_base BOOLEAN;
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS primary_base_weeks NUMERIC(5, 1);
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS primary_base_correction_pct NUMERIC(5, 1);
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS primary_base_status VARCHAR(20);  -- N/A, TOO_EARLY, FORMING, COMPLETE, FAILED
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS days_since_ipo INTEGER;

CREATE INDEX IF NOT EXISTS idx_is_new_issue ON minervini_metrics(is_new_issue);
CREATE INDEX IF NOT EXISTS idx_primary_base_status ON minervini_metrics(primary_base_status);

-- ============================================================================
-- 2026-02-09: Holder Signal system (HOLD/SELL) for existing stockholders
-- Parallel to the buyer signal (BUY/WAIT/PASS). Provides two stop-loss tiers:
-- an initial stop for new positions and a trailing stop for existing holders.
-- ============================================================================

ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS holder_signal VARCHAR(10);           -- HOLD / SELL
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS holder_signal_reasons TEXT;           -- Explanation for the holder signal
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS holder_stop_initial NUMERIC(10, 2);  -- Tight stop for new positions (3-8% below price)
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS holder_stop_trailing NUMERIC(10, 2); -- Trailing stop based on rising MAs
ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS holder_trailing_method VARCHAR(20);  -- Which MA is used (10-EMA, 21-EMA, 50-MA, 200-MA)

CREATE INDEX IF NOT EXISTS idx_holder_signal ON minervini_metrics(holder_signal);
CREATE INDEX IF NOT EXISTS idx_holder_signal_date ON minervini_metrics(date, holder_signal);
