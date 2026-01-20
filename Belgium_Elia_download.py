import pandas as pd
import numpy as np
import requests
import os
from datetime import datetime, timedelta
import time

os.system('clear')

# Elia Belgium Cross-Border Physical Flows Data Download Script
print("Elia Belgium Cross-Border Physical Flows Data Download Script")
print("Dataset: ODS026 - Physical flow by border (15-minute resolution)")
print("=" * 80)

# Elia API Configuration
ELIA_API_BASE = "https://opendata.elia.be/api/explore/v2.1/catalog/datasets/ods026/records"
CONTROL_AREA = "Netherlands"  # Netherlands control area = Belgium-Netherlands border

# Define the output directory (parent folder: GUI_NET_CROSS_BORDER_PHYSICAL_FLOWS/)
output_dir = os.path.join(os.path.dirname(__file__), '..')
output_dir = os.path.abspath(output_dir)
print(f"\nOutput directory: {output_dir}")

# Data available from 2015 onwards, but 15-minute data quality improves over time
# Start from 2018 to match the other datasets in the project
default_start = pd.Timestamp('20180101', tz='Europe/Brussels')
end = pd.Timestamp.now(tz='Europe/Brussels').floor('15min')  # Retrieve data up to last 15-minute interval

# Check for existing CSV files
print("\nChecking for existing Belgium data files...")
existing_files = [f for f in os.listdir(output_dir)
                  if f.startswith('Belgium_NL_crossborder_') and f.endswith('.csv')]

historical_data = None
start = default_start

if existing_files:
    existing_files.sort(reverse=True)
    latest_file = existing_files[0]
    print(f"  ✓ Found existing file: {latest_file}")

    try:
        latest_file_path = os.path.join(output_dir, latest_file)
        historical_data = pd.read_csv(latest_file_path)

        # Convert datetime column
        historical_data['datetime'] = pd.to_datetime(historical_data['datetime'], utc=True)
        historical_data['datetime'] = historical_data['datetime'].dt.tz_convert('Europe/Brussels')
        historical_data = historical_data.set_index('datetime')

        last_date = historical_data.index.max()
        print(f"  Existing data: {len(historical_data)} rows")
        print(f"    Date range: {historical_data.index.min().strftime('%Y-%m-%d %H:%M')} to {last_date.strftime('%Y-%m-%d %H:%M')}")

        # Start fetching from next 15-minute interval
        start = last_date + pd.Timedelta(minutes=15)
        print(f"\n  → Will fetch NEW data from {start.strftime('%Y-%m-%d %H:%M')} onwards")

        if start >= end:
            print(f"\n✓ Data is already up to date! No new data to fetch.")
            print(f"  Latest data: {last_date.strftime('%Y-%m-%d %H:%M')}")
            print(f"  Current time: {end.strftime('%Y-%m-%d %H:%M')}")
            exit(0)

    except Exception as e:
        print(f"  ⚠️  Error reading existing file: {e}")
        print(f"  → Will fetch all data from {default_start.strftime('%Y-%m-%d')}")
        historical_data = None
        start = default_start
else:
    print(f"  ℹ️  No existing files found")
    print(f"  → Will fetch all data from {start.strftime('%Y-%m-%d')}")

print(f"\n{'='*80}")
print(f"Fetching data from {start.strftime('%Y-%m-%d %H:%M')} to {end.strftime('%Y-%m-%d %H:%M')}")
print(f"Total days: {(end - start).days}")
print(f"{'='*80}\n")


def fetch_elia_data(start_dt, end_dt, limit=100, max_retries=3):
    """
    Fetch cross-border flow data from Elia API with pagination

    Parameters:
    -----------
    start_dt : pd.Timestamp
        Start datetime (Europe/Brussels timezone)
    end_dt : pd.Timestamp
        End datetime (Europe/Brussels timezone)
    limit : int
        Number of records per API call (max 100)
    max_retries : int
        Number of retries for failed requests

    Returns:
    --------
    pd.DataFrame
        DataFrame with datetime index and flow values
    """
    all_records = []
    offset = 0

    # Convert to ISO format for API
    start_str = start_dt.strftime('%Y-%m-%dT%H:%M:%S')
    end_str = end_dt.strftime('%Y-%m-%dT%H:%M:%S')

    # Build query filter (API requires uppercase AND)
    where_clause = f'controlarea="{CONTROL_AREA}" AND datetime >= "{start_str}" AND datetime < "{end_str}"'

    print(f"\nFetching Belgium-Netherlands cross-border flows...")
    print(f"  Control Area: {CONTROL_AREA}")
    print(f"  Period: {start_str} to {end_str}")

    page = 1
    while True:
        params = {
            'where': where_clause,
            'order_by': 'datetime',
            'limit': limit,
            'offset': offset,
            'timezone': 'Europe/Brussels'
        }

        retry_count = 0
        while retry_count < max_retries:
            try:
                response = requests.get(ELIA_API_BASE, params=params, timeout=30)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"\n  ❌ Failed after {max_retries} retries: {e}")
                    raise
                print(f"  ⚠️  Retry {retry_count}/{max_retries} after error: {e}")
                time.sleep(2 ** retry_count)  # Exponential backoff: 2s, 4s, 8s

        data = response.json()
        records = data.get('results', [])  # API v2.1 uses 'results' not 'records'

        if not records:
            break

        all_records.extend(records)
        print(f"  Page {page}: Retrieved {len(records)} records (total: {len(all_records)})")

        # Check if we've retrieved all available records
        total_count = data.get('total_count', 0)
        if len(all_records) >= total_count:
            print(f"  ✓ Retrieved all {total_count} available records")
            break

        offset += limit
        page += 1

        # Be nice to the API - minimal delay
        time.sleep(0.01)

    if not all_records:
        print("  ⚠️  No records found for this period")
        return pd.DataFrame()

    # Process records into DataFrame
    processed_data = []
    for record in all_records:
        # Elia API v2.1 structure: each result contains fields directly
        processed_data.append({
            'datetime': record.get('datetime'),
            # physicalflowatborder: Positive = Belgium EXPORTS to NL (= NL imports from BE)
            #                       Negative = Belgium IMPORTS from NL (= NL exports to BE)
            'flow_MW': record.get('physicalflowatborder'),
            'control_area': record.get('controlarea')
        })

    df = pd.DataFrame(processed_data)

    # Convert datetime to timezone-aware (API returns UTC timestamps)
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True).dt.tz_convert('Europe/Brussels')

    df = df.set_index('datetime').sort_index()

    # Remove duplicates if any
    df = df[~df.index.duplicated(keep='first')]

    return df


# Fetch data in monthly chunks to handle large date ranges
print("\n" + "="*80)
print("Starting data retrieval...")
print("="*80)

all_data = []
current_start = start

# Calculate total months for progress tracking
total_months = ((end.year - start.year) * 12 + end.month - start.month) + 1
current_month = 0

# Track timing for ETA
loop_start_time = time.time()
first_chunk_completed = False

# Calculate monthly chunks
try:
    while current_start < end:
        current_month += 1
        # Fetch one month at a time
        chunk_end = min(current_start + pd.DateOffset(months=1), end)

        progress_pct = (current_month / total_months) * 100
        print(f"\n📅 Chunk {current_month}/{total_months} ({progress_pct:.1f}%): {current_start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}")

        try:
            chunk_data = fetch_elia_data(current_start, chunk_end, limit=100)
            print(chunk_data)  # Debug: show chunk data structure

            if not chunk_data.empty:
                all_data.append(chunk_data)
                print(f"  ✓ Retrieved {len(chunk_data)} records")
                
                # Estimate remaining time after first chunk
                if not first_chunk_completed:
                    first_chunk_completed = True
                    elapsed_first = time.time() - loop_start_time
                    remaining_chunks = total_months - current_month
                    estimated_total_secs = elapsed_first * total_months
                    estimated_remaining_secs = estimated_total_secs - elapsed_first
                    hours, remainder = divmod(estimated_remaining_secs, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    print(f"  ⏱️  First chunk took {elapsed_first:.1f}s; estimating ~{int(hours)}h {int(minutes)}m {int(seconds)}s remaining")
            else:
                print(f"  ℹ️  No data available for this period")

        except Exception as e:
            print(f"  ❌ Error fetching chunk: {e}")
            print(f"  → Skipping this chunk and continuing...")

        current_start = chunk_end
        time.sleep(0.01)  # Rate limiting between chunks

except KeyboardInterrupt:
    print(f"\n\n⚠️  Script interrupted by user!")
    print(f"Saving partial data collected so far...")
except Exception as e:
    print(f"\n\n❌ Critical error occurred: {e}")
    print(f"Saving partial data collected so far...")

# Combine all chunks
if all_data:
    new_data = pd.concat(all_data)
    new_data = new_data[~new_data.index.duplicated(keep='first')].sort_index()

    print(f"\n{'='*80}")
    print(f"✓ Successfully retrieved {len(new_data)} new records")
    print(f"  Date range: {new_data.index.min().strftime('%Y-%m-%d %H:%M')} to {new_data.index.max().strftime('%Y-%m-%d %H:%M')}")

    # Convert flow_MW to import_BE_NL_MW BEFORE combining with historical data
    #
    # SIGN CONVENTION (from Netherlands perspective):
    # ================================================
    # Positive value (+) = Belgium EXPORTS to NL  →  NL IMPORTS from Belgium
    # Negative value (-) = Belgium IMPORTS from NL →  NL EXPORTS to Belgium
    #
    # Column name 'import_BE_NL_MW' means: Import from Belgium to Netherlands
    # This matches ENTSO-E convention where positive = import to NL
    new_data['import_BE_NL_MW'] = new_data['flow_MW']

    # Combine with historical data if available
    if historical_data is not None:
        print(f"\nCombining with historical data ({len(historical_data)} rows)...")
        # Only select import_BE_NL_MW column for concatenation
        combined_data = pd.concat([historical_data[['import_BE_NL_MW']], new_data[['import_BE_NL_MW']]])
        combined_data = combined_data[~combined_data.index.duplicated(keep='last')].sort_index()
        print(f"  ✓ Total combined records: {len(combined_data)}")
    else:
        combined_data = new_data[['import_BE_NL_MW']].copy()

    # Validate 15-minute frequency
    print("\nValidating data frequency...")
    expected_index = pd.date_range(
        start=combined_data.index.min(),
        end=combined_data.index.max(),
        freq='15min',
        tz='Europe/Brussels'
    )

    missing_timestamps = expected_index.difference(combined_data.index)
    extra_timestamps = combined_data.index.difference(expected_index)

    if len(missing_timestamps) > 0:
        print(f"  ⚠️  Missing {len(missing_timestamps)} timestamps (gaps in data)")
    if len(extra_timestamps) > 0:
        print(f"  ⚠️  Found {len(extra_timestamps)} extra timestamps")

    if len(missing_timestamps) == 0 and len(extra_timestamps) == 0:
        print(f"  ✓ Perfect 15-minute frequency: {len(combined_data)} records")

    # Prepare output (import_BE_NL_MW already created earlier)
    output_data = combined_data.copy()
    output_data = output_data.reset_index()

    # Convert timezone to Amsterdam for consistency with other data
    output_data['datetime'] = output_data['datetime'].dt.tz_convert('Europe/Amsterdam')

    # Handle NaN and non-finite values properly
    output_data['import_BE_NL_MW'] = pd.to_numeric(output_data['import_BE_NL_MW'], errors='coerce')
    # Replace infinite values with NA
    output_data['import_BE_NL_MW'] = output_data['import_BE_NL_MW'].replace([np.inf, -np.inf], pd.NA)

    # Count and report missing values
    n_missing = int(output_data['import_BE_NL_MW'].isna().sum())
    if n_missing > 0:
        print(f"  ⚠️  {n_missing} missing or non-finite flow values will remain as NA in the output")

    # Round non-NA values and convert to nullable Int64 (which preserves NA values)
    output_data['import_BE_NL_MW'] = output_data['import_BE_NL_MW'].round(0).astype('Int64')

    # Save to CSV
    output_start = output_data['datetime'].min()
    output_end = output_data['datetime'].max()
    output_filename = f'Belgium_NL_crossborder_{output_start.strftime("%Y%m%d")}_{output_end.strftime("%Y%m%d")}_MW_quarterly.csv'
    output_file_path = os.path.join(output_dir, output_filename)

    print(f"\n{'='*80}")
    print(f"Saving data to CSV...")
    print(f"  SIGN CONVENTION: Positive (+) = Belgium exports to NL (NL imports from BE)")
    print(f"                   Negative (-) = Belgium imports from NL (NL exports to BE)")
    output_data.to_csv(output_file_path, index=False)
    print(f"  ✓ Saved to: {output_filename}")
    print(f"  Total rows: {len(output_data)}")
    print(f"  Date range: {output_start.strftime('%Y-%m-%d %H:%M')} to {output_end.strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*80}\n")

    # Display sample statistics
    print("Sample statistics:")
    # Cast to float for aggregation to handle nullable integers (pd.NA)
    series_float = output_data['import_BE_NL_MW'].astype(float)
    print(f"  Mean flow: {series_float.mean():.0f} MW")
    print(f"  Max import (to NL): {series_float.max():.0f} MW")
    print(f"  Max export (from NL): {series_float.min():.0f} MW")
    print(f"  Std deviation: {series_float.std():.0f} MW")

    print("\n✓ Script completed successfully!")

else:
    print(f"\n❌ No data retrieved. Please check:")
    print(f"  1. Date range is valid (Oct 1, 2025 onwards)")
    print(f"  2. API endpoint is accessible")
    print(f"  3. Internet connection is stable")
