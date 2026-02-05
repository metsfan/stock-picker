#!/usr/bin/env python3
"""
Download stock data files from S3 for the last 2 years.
Downloads 10 files concurrently for efficiency.
"""

import subprocess
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from pathlib import Path
import csv
import psycopg2


def get_db_connection():
    """Get PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='stocks',
            user='postgres',
            password='adamesk'
        )
        return conn
    except psycopg2.Error as e:
        print(f"Warning: Could not connect to PostgreSQL: {e}")
        return None


def date_exists_in_db(conn, date_str):
    """Check if data for a given date already exists in the database."""
    if conn is None:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM stock_prices WHERE date = %s
        """, (date_str,))
        count = cursor.fetchone()[0]
        cursor.close()
        return count > 0
    except Exception as e:
        print(f"  Error checking date in DB: {e}")
        return False


def load_csv_to_db(conn, csv_file_path, date_str):
    """Load stock data from CSV file into the database."""
    if conn is None:
        return 0
    
    if not Path(csv_file_path).exists():
        return 0
    
    # Skip empty files (non-trading days)
    if Path(csv_file_path).stat().st_size == 0:
        return 0
    
    cursor = conn.cursor()
    records_loaded = 0
    
    try:
        with open(csv_file_path, 'r') as f:
            reader = csv.DictReader(f)
            
            batch = []
            for row in reader:
                try:
                    ticker = row.get('ticker', '').upper()
                    if not ticker:
                        continue
                    
                    batch.append((
                        ticker,
                        date_str,
                        float(row.get('open', 0) or 0),
                        float(row.get('high', 0) or 0),
                        float(row.get('low', 0) or 0),
                        float(row.get('close', 0) or 0),
                        int(row.get('volume', 0) or 0)
                    ))
                    
                    # Batch insert every 1000 records for performance
                    if len(batch) >= 1000:
                        cursor.executemany("""
                            INSERT INTO stock_prices 
                            (symbol, date, open, high, low, close, volume)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (symbol, date) DO NOTHING
                        """, batch)
                        records_loaded += len(batch)
                        batch = []
                        
                except Exception as e:
                    continue
            
            # Insert remaining records
            if batch:
                cursor.executemany("""
                    INSERT INTO stock_prices 
                    (symbol, date, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, date) DO NOTHING
                """, batch)
                records_loaded += len(batch)
        
        conn.commit()
        cursor.close()
        return records_loaded
        
    except Exception as e:
        print(f"  Error loading CSV to DB: {e}")
        conn.rollback()
        return 0


def generate_date_range(start_date, end_date):
    """Generate all dates between start_date and end_date (inclusive)."""
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date)
        current_date += timedelta(days=1)
    return dates


def download_file_for_date(date, db_conn):
    """
    Download and extract the stock data file for a specific date.
    Also loads the data into the database.
    
    Args:
        date: datetime object representing the date to download
        db_conn: PostgreSQL database connection
        
    Returns:
        tuple: (date_str, status, message) where status is 'success', 'skipped', or 'failed'
    """
    date_str = date.strftime("%Y-%m-%d")
    year = date.strftime("%Y")
    month = date.strftime("%m")
    
    # Check if the uncompressed CSV file already exists
    csv_path = f"../day_aggs/{date_str}.csv"
    file_already_exists = os.path.exists(csv_path)
    
    if file_already_exists:
        # Check if it's a dummy file (non-trading day marker)
        file_size = os.path.getsize(csv_path)
        if file_size == 0:
            return (date_str, "skipped", "Non-trading day")
        
        # File exists, but we still need to check if it's in the database
        if date_exists_in_db(db_conn, date_str):
            return (date_str, "skipped", "Already in DB")
        else:
            # File exists but not in DB - load it
            records = load_csv_to_db(db_conn, csv_path, date_str)
            if records > 0:
                return (date_str, "success", f"Loaded to DB ({records} records)")
            else:
                return (date_str, "skipped", "Already exists")
    
    # Construct the S3 path and local filename
    s3_path = f"s3://flatfiles/us_stocks_sip/day_aggs_v1/{year}/{month}/{date_str}.csv.gz"
    local_path = f"../day_aggs/{date_str}.csv.gz"
    
    # Full command to run through WSL
    command = (
        f'wsl bash -c "aws s3 cp {s3_path} {local_path} '
        f'--endpoint-url https://files.massive.com && gunzip {local_path}"'
    )
    
    try:
        # Run the command
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout per file
        )
        
        if result.returncode == 0:
            # Successfully downloaded - now load to database
            records = load_csv_to_db(db_conn, csv_path, date_str)
            if records > 0:
                return (date_str, "success", f"Downloaded & loaded ({records} records)")
            else:
                return (date_str, "success", "Downloaded")
        else:
            # Check if it's a 404 error (file not found - likely non-trading day)
            error_msg = result.stderr.strip() if result.stderr else result.stdout.strip()
            
            # Check for common 404/not found indicators
            is_not_found = any(indicator in error_msg.lower() for indicator in [
                '404', 'not found', 'nosuchkey', 'does not exist', 'no such key'
            ])
            
            if is_not_found:
                # Create a dummy file to mark this as a non-trading day
                Path(csv_path).touch()
                return (date_str, "skipped", "Non-trading day (404)")
            else:
                # Some other error occurred
                return (date_str, "failed", f"Error: {error_msg[:100]}")
            
    except subprocess.TimeoutExpired:
        return (date_str, "failed", "Timeout")
    except Exception as e:
        return (date_str, "failed", f"Exception: {str(e)[:100]}")


def main():
    """Main function to orchestrate the download process."""
    # Calculate date range (last 2 years, ending 2 days ago due to data delay)
    today = datetime.now()
    end_date = today - timedelta(days=2)  # Data has 24hr+ delay, stop 2 days before today
    start_date = end_date - timedelta(days=2*365)  # Approximately 2 years
    
    print(f"Downloading stock data from {start_date.date()} to {end_date.date()}")
    print(f"(Stopping 2 days before today due to source data delay)")
    print(f"Total days: {(end_date - start_date).days + 1}")
    
    # Ensure the output directory exists
    os.makedirs("../day_aggs", exist_ok=True)
    
    # Connect to database
    print("\nConnecting to PostgreSQL database...")
    db_conn = get_db_connection()
    if db_conn is None:
        print("Warning: Could not connect to database. Files will be downloaded but not loaded to DB.")
    else:
        print("✓ Database connection established")
    
    # Generate all dates
    dates = generate_date_range(start_date, end_date)
    total_dates = len(dates)
    
    print(f"\nStarting downloads with 10 concurrent workers...")
    print("-" * 80)
    
    # Track statistics
    successful = 0
    skipped = 0
    failed = 0
    completed = 0
    
    try:
        # Use ThreadPoolExecutor to download files concurrently
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Submit all download jobs
            future_to_date = {executor.submit(download_file_for_date, date, db_conn): date for date in dates}
            
            # Process completed downloads as they finish
            for future in as_completed(future_to_date):
                date_str, status, message = future.result()
                completed += 1
                
                if status == "success":
                    successful += 1
                    status_icon = "✓"
                elif status == "skipped":
                    skipped += 1
                    status_icon = "⊙"
                else:  # failed
                    failed += 1
                    status_icon = "✗"
                
                # Print progress
                progress = (completed / total_dates) * 100
                print(f"[{progress:5.1f}%] {status_icon} {date_str} - {message}")
    finally:
        # Close database connection
        if db_conn:
            db_conn.close()
            print("\n✓ Database connection closed")
    
    # Print summary
    print("-" * 80)
    print(f"\nDownload Summary:")
    print(f"  Total dates processed: {total_dates}")
    print(f"  Successful downloads: {successful}")
    print(f"  Skipped (already exist): {skipped}")
    print(f"  Failed downloads: {failed}")
    print(f"  Download rate: {(successful/total_dates)*100:.1f}%")
    
    print(f"\nFiles saved to: {Path('../day_aggs').absolute()}")
    print(f"Database: PostgreSQL @ localhost:5432/stocks")


if __name__ == "__main__":
    main()
