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

-- Example:
-- 2026-02-04: Add new column for volume analysis
-- ALTER TABLE minervini_metrics ADD COLUMN IF NOT EXISTS volume_analysis NUMERIC(10, 2);
