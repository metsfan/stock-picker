@echo off
REM Stock Picker Data Pipeline (Windows)
REM Runs all data fetching and analysis scripts in sequence.
REM Exits immediately if any step fails.
REM
REM Usage:
REM   cd scripts
REM   run_pipeline.bat
REM
REM Or from project root:
REM   scripts\run_pipeline.bat

setlocal enabledelayedexpansion

echo ========================================
echo Stock Picker Data Pipeline
echo ========================================
echo.
echo Working directory: %cd%
echo Start time: %date% %time%
echo.

REM Change to script directory
cd /d "%~dp0"

REM Step 1: Fetch Stock Prices
echo.
echo ========================================
echo Step 1: Fetch Stock Prices (Split-Adjusted)
echo ========================================
echo Running: python fetch_stock_prices.py
echo.
python fetch_stock_prices.py
if errorlevel 1 (
    echo.
    echo [ERROR] Step 1 FAILED
    echo Pipeline aborted.
    exit /b 1
)
echo.
echo [OK] Step 1 completed successfully
echo.

REM Step 2: Fetch Historical Earnings
echo.
echo ========================================
echo Step 2: Fetch Historical Earnings
echo ========================================
echo Running: python fetch_earnings.py
echo.
python fetch_earnings.py
if errorlevel 1 (
    echo.
    echo [ERROR] Step 2 FAILED
    echo Pipeline aborted.
    exit /b 1
)
echo.
echo [OK] Step 2 completed successfully
echo.

REM Step 3: Fetch Upcoming Earnings
echo.
echo ========================================
echo Step 3: Fetch Upcoming Earnings
echo ========================================
echo Running: python fetch_upcoming_earnings.py
echo.
python fetch_upcoming_earnings.py
if errorlevel 1 (
    echo.
    echo [ERROR] Step 3 FAILED
    echo Pipeline aborted.
    exit /b 1
)
echo.
echo [OK] Step 3 completed successfully
echo.

REM Step 4: Fetch Income Statements
echo.
echo ========================================
echo Step 4: Fetch Income Statements
echo ========================================
echo Running: python fetch_income_statements.py
echo.
python fetch_income_statements.py
if errorlevel 1 (
    echo.
    echo [ERROR] Step 4 FAILED
    echo Pipeline aborted.
    exit /b 1
)
echo.
echo [OK] Step 4 completed successfully
echo.

REM Step 5: Run Minervini Analysis
echo.
echo ========================================
echo Step 5: Run Minervini Analysis
echo ========================================
echo Running: python minervini.py
echo.
python minervini.py
if errorlevel 1 (
    echo.
    echo [ERROR] Step 5 FAILED
    echo Pipeline aborted.
    exit /b 1
)
echo.
echo [OK] Step 5 completed successfully
echo.

REM Success
echo.
echo ========================================
echo Pipeline completed successfully!
echo ========================================
echo End time: %date% %time%
echo.

exit /b 0
