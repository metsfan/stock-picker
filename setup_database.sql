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

-- Indices for new columns
CREATE INDEX IF NOT EXISTS idx_avg_dollar_volume ON minervini_metrics(avg_dollar_volume DESC);
CREATE INDEX IF NOT EXISTS idx_industry_rs ON minervini_metrics(industry_rs DESC);
CREATE INDEX IF NOT EXISTS idx_is_52w_high ON minervini_metrics(is_52w_high);

-- Sector performance tracking table (aggregated from individual stocks)
CREATE TABLE IF NOT EXISTS sector_performance (
    date DATE NOT NULL,
    sic_code VARCHAR(10) NOT NULL,
    sic_description VARCHAR(255),
    sector_return_90d NUMERIC(10, 2),  -- 90-day sector return
    sector_rs NUMERIC(10, 2),          -- Sector relative strength vs market
    stock_count INTEGER,               -- Number of stocks in sector
    PRIMARY KEY (date, sic_code)
);

CREATE INDEX IF NOT EXISTS idx_sector_perf_date ON sector_performance(date);
CREATE INDEX IF NOT EXISTS idx_sector_perf_rs ON sector_performance(sector_rs DESC);
