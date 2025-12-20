import pandas as pd
import numpy as np
from datetime import date, timedelta
from entsoe import EntsoePandasClient
import os
os.system('clear')

# Entsoe Cross-Border Physical Flows Data Download Script
print("Entsoe Cross-Border Physical Flows Data Download Script (using entsoe-py)")

# Load API key from .env file
print("Loading API key from .env file...")
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('ENTSOE_API_KEY')
if not api_key:
    api_key = os.getenv('ENTSOE_API_BIRDVIEW')

if not api_key:
    raise ValueError("No API key found. Please set ENTSOE_API_KEY or ENTSOE_API_BIRDVIEW in .env file")

# Initialize the ENTSO-E client
client = EntsoePandasClient(api_key=api_key)
country_code = 'NL'  # Netherlands

# Define the output directory (parent folder: GUI_NET_CROSS_BORDER_PHYSICAL_FLOWS/)
output_dir = os.path.join(os.path.dirname(__file__), '..')
output_dir = os.path.abspath(output_dir)  # Convert to absolute path
print(f"\nOutput directory: {output_dir}")

# Check for existing CSV files and determine date range to fetch
print("\nChecking for existing data files...")
existing_files = [f for f in os.listdir(output_dir) if f.startswith(f'netCrossBorderExchangeNL_') and f.endswith('.csv')]
existing_detailed_files = [f for f in os.listdir(output_dir) if f.startswith(f'CrossBorder_detailed_') and f.endswith('.csv')]

historical_data = None
historical_detailed_data = None
start = pd.Timestamp('20180101', tz='Europe/Amsterdam')  # Default start date
end = pd.Timestamp.now(tz='Europe/Amsterdam').floor('h')  # Round down to current hour

if existing_files:
    # Sort files by name to get the most recent one
    existing_files.sort(reverse=True)
    latest_file = existing_files[0]
    print(f"  ✓ Found existing file: {latest_file}")

    try:
        # Load existing data (with full path)
        latest_file_path = os.path.join(output_dir, latest_file)
        # Read CSV without parsing dates first, then handle timezone properly
        historical_data = pd.read_csv(latest_file_path)

        # Convert datetime column
        historical_data['datetime'] = pd.to_datetime(historical_data['datetime'], utc=True)
        historical_data['datetime'] = historical_data['datetime'].dt.tz_convert('Europe/Amsterdam')

        # Set datetime as index
        historical_data = historical_data.set_index('datetime')

        last_date = historical_data.index.max()
        print(f"  Existing data: {len(historical_data)} rows")
        print(f"    Date range: {historical_data.index.min().strftime('%Y-%m-%d %H:%M')} to {last_date.strftime('%Y-%m-%d %H:%M')}")

        # Start fetching from the next hour after the last existing data
        start = last_date + pd.Timedelta(hours=1)
        print(f"\n  → Will fetch NEW data from {start.strftime('%Y-%m-%d %H:%M')} onwards")

        # If the existing data is already up to date, skip fetching
        if start >= end:
            print(f"\n✓ Data is already up to date! No new data to fetch.")
            print(f"  Latest data: {last_date.strftime('%Y-%m-%d %H:%M')}")
            print(f"  Current time: {end.strftime('%Y-%m-%d %H:%M')}")
            exit(0)

    except Exception as e:
        print(f"  ⚠️  Error reading existing file: {e}")
        print(f"  → Will fetch all data from {start.strftime('%Y-%m-%d')}")
        historical_data = None
else:
    print(f"  ℹ️  No existing files found")
    print(f"  → Will fetch all data from {start.strftime('%Y-%m-%d')}")

# Also check for detailed historical file
if existing_detailed_files:
    existing_detailed_files.sort(reverse=True)
    latest_detailed_file = existing_detailed_files[0]
    print(f"\n  ✓ Found existing detailed file: {latest_detailed_file}")

    try:
        # Load existing detailed data (with full path)
        latest_detailed_file_path = os.path.join(output_dir, latest_detailed_file)
        historical_detailed_data = pd.read_csv(latest_detailed_file_path)

        # Convert datetime column
        historical_detailed_data['datetime'] = pd.to_datetime(historical_detailed_data['datetime'], utc=True)
        historical_detailed_data['datetime'] = historical_detailed_data['datetime'].dt.tz_convert('Europe/Amsterdam')

        # Set datetime as index
        historical_detailed_data = historical_detailed_data.set_index('datetime')

        print(f"  Detailed historical data: {len(historical_detailed_data)} rows")
        print(f"    Columns: {list(historical_detailed_data.columns)}")

    except Exception as e:
        print(f"  ⚠️  Error reading existing detailed file: {e}")
        historical_detailed_data = None

# Define parameters for data retrieval
print("\nData Retrieval Parameters:")
print(f'Country Code: {country_code}')
print(f'Start date: {start}')
print(f'End date:   {end}')

# Define neighboring countries for Netherlands cross-border flows
neighboring_countries = {
    'BE': 'Belgium',
    'DE': 'Germany',
    'DK_1': 'Denmark (DK1)',
    'GB': 'Great Britain',
    'NO_2': 'Norway (NO2)'
}

# Fetch Cross-Border Physical Flows data from ENTSO-E in chunks
import threading, itertools, sys, time

def _spinner(stop_event):
    for ch in itertools.cycle('|/-\\'):
        if stop_event.is_set():
            break
        sys.stdout.write(f'\rFetching cross-border flows data from ENTSO-E... {ch}')
        sys.stdout.flush()
        time.sleep(0.1)
    sys.stdout.write('\rFetching cross-border flows data from ENTSO-E... done\n')
    sys.stdout.flush()

def api_call_with_timeout(func, timeout_seconds, *args, **kwargs):
    """Execute an API call with a timeout using threading (cross-platform)"""
    import queue

    result_queue = queue.Queue()
    exception_queue = queue.Queue()

    def worker():
        try:
            result = func(*args, **kwargs)
            result_queue.put(result)
        except Exception as e:
            exception_queue.put(e)

    worker_thread = threading.Thread(target=worker, daemon=True)
    worker_thread.start()
    worker_thread.join(timeout=timeout_seconds)

    if worker_thread.is_alive():
        # Thread is still running - timeout occurred
        raise TimeoutError(f"API call timed out after {timeout_seconds} seconds")

    # Check for exceptions
    if not exception_queue.empty():
        raise exception_queue.get()

    # Get result
    if not result_queue.empty():
        return result_queue.get()

    return None

def fetch_crossborder_flows_in_chunks(country_from, country_to, start, end):
    """Fetch cross-border flows for a specific border in chunks"""
    all_data = []
    current_start = start
    chunk_count = 0
    max_retries = 3
    base_delay = 5
    api_timeout = 60  # 60 second timeout for API calls
    request_delay = 1.5  # Delay between successful API requests (seconds)

    while current_start < end:
        chunk_count += 1
        # Use smaller chunks (3 months) to respect API limits
        requested_end = min(current_start + pd.DateOffset(months=3), end)

        # Ensure both start and end have the same UTC offset to avoid DST issues
        # Convert to UTC, then back to Europe/Amsterdam to normalize
        current_start_utc = current_start.tz_convert('UTC')
        requested_end_utc = requested_end.tz_convert('UTC')

        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
            try:
                # Query cross-border flows using entsoe-py with timeout (use UTC timestamps)
                chunk_data = api_call_with_timeout(
                    client.query_crossborder_flows,
                    api_timeout,
                    country_code_from=country_from,
                    country_code_to=country_to,
                    start=current_start_utc,
                    end=requested_end_utc
                )

                if chunk_data is None or len(chunk_data) == 0:
                    current_start = requested_end
                    success = True
                    time.sleep(0.2)
                    continue

                # Convert to DataFrame if it's a Series
                if isinstance(chunk_data, pd.Series):
                    chunk_data = chunk_data.to_frame(f'{country_from}_to_{country_to}')

                # Ensure data is in Europe/Amsterdam timezone
                if chunk_data.index.tz is None:
                    chunk_data.index = chunk_data.index.tz_localize('Europe/Amsterdam')
                elif chunk_data.index.tz != pd.Timestamp.now(tz='Europe/Amsterdam').tz:
                    chunk_data.index = chunk_data.index.tz_convert('Europe/Amsterdam')

                all_data.append(chunk_data)
                success = True

                # Continue from the next timestamp after the last received data
                actual_end = chunk_data.index.max()
                # Convert back to Europe/Amsterdam for comparison
                if actual_end.tz != pd.Timestamp.now(tz='Europe/Amsterdam').tz:
                    actual_end = actual_end.tz_convert('Europe/Amsterdam')

                # Show progress
                print(f"      Chunk {chunk_count}: {len(chunk_data)} rows ({current_start.strftime('%Y-%m-%d')} to {actual_end.strftime('%Y-%m-%d')})")

                current_start = actual_end + pd.Timedelta(hours=1)

                # Rate limiting: delay before next request
                time.sleep(request_delay)

                # Check if we've reached the target end date
                if actual_end >= end:
                    break

            except TimeoutError as e:
                print(f"  ⚠️  Timeout fetching {country_from}->{country_to}: {e}")
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"  ✗ Max retries reached for {country_from}->{country_to} after timeouts")
                    success = True  # Skip this border
                    break
                else:
                    delay = base_delay * (2 ** (retry_count - 1))
                    print(f"  ⚠️  Retrying {country_from}->{country_to} in {delay}s...")
                    time.sleep(delay)

            except Exception as e:
                error_msg = str(e) if str(e) else repr(e)
                status_code = None
                response = getattr(e, "response", None)
                if response is not None:
                    status_code = getattr(response, "status_code", None)

                # Only retry on specific error codes (503 = Service Unavailable, timeouts)
                if status_code not in (503,):
                    print(f"  ✗ Non-retryable error fetching {country_from}->{country_to}: {error_msg}")
                    success = True  # Skip this border
                    break

                retry_count += 1
                if retry_count >= max_retries:
                    print(f"  ✗ Max retries reached for {country_from}->{country_to}: {error_msg}")
                    success = True  # Skip this border
                    break
                else:
                    delay = base_delay * (2 ** (retry_count - 1))
                    print(f"  ⚠️  Retrying {country_from}->{country_to} in {delay}s... (Error: {error_msg})")
                    time.sleep(delay)

        if success:
            time.sleep(0.2)

    # Concatenate all chunks
    if all_data:
        combined_data = pd.concat(all_data)
        combined_data = combined_data[~combined_data.index.duplicated(keep='first')]
        combined_data = combined_data.sort_index()

        # Check for frequency transitions within the data
        time_diffs = combined_data.index.to_series().diff()
        has_hourly = (time_diffs == pd.Timedelta(hours=1)).any()
        has_15min = (time_diffs == pd.Timedelta(minutes=15)).any()

        if has_hourly and has_15min:
            print(f"      ⚠️  Detected frequency transition (hourly→15min) for {country_from}->{country_to}")
            print(f"      Converting entire series to 15-minute with forward fill...")

            # Create complete 15-minute index
            complete_index = pd.date_range(
                start=combined_data.index.min(),
                end=combined_data.index.max(),
                freq='15min',
                tz='Europe/Amsterdam'
            )

            # Reindex and forward fill
            combined_data = combined_data.reindex(complete_index, method='ffill')
            print(f"      ✓ Converted to 15-minute: {len(combined_data)} rows")

        return combined_data
    else:
        return None

_stop_event = threading.Event()
_spinner_thread = threading.Thread(target=_spinner, args=(_stop_event,), daemon=True)
_spinner_thread.start()

# Fetch flows for all borders
print(f"\nFetching cross-border flows for {len(neighboring_countries)} borders...")
all_flows = {}
country_row_counts = {}  # Track row counts to detect hourly vs 15-minute data
api_call_counter = 0  # Global API call counter for rate limiting

try:
    border_count = 0
    total_borders = len(neighboring_countries) * 2  # 2 directions per country

    for country_to, country_name in neighboring_countries.items():
        border_count += 1
        print(f"\n  [{border_count}/{total_borders}] Fetching NL -> {country_name}...")
        flow_export = fetch_crossborder_flows_in_chunks('NL', country_to, start, end)
        if flow_export is not None:
            all_flows[f'exp_{country_to}'] = flow_export
            export_rows = len(flow_export)
            print(f"    ✓ Received {export_rows} rows")
            if country_to not in country_row_counts:
                country_row_counts[country_to] = []
            country_row_counts[country_to].append(export_rows)

        # Small delay between directions
        time.sleep(0.1)

        border_count += 1
        print(f"  [{border_count}/{total_borders}] Fetching {country_name} -> NL...")
        flow_import = fetch_crossborder_flows_in_chunks(country_to, 'NL', start, end)
        if flow_import is not None:
            all_flows[f'imp_{country_to}'] = flow_import
            import_rows = len(flow_import)
            print(f"    ✓ Received {import_rows} rows")
            if country_to not in country_row_counts:
                country_row_counts[country_to] = []
            country_row_counts[country_to].append(import_rows)

        # Delay between country pairs to respect API limits
        if border_count < total_borders:
            print(f"    Waiting 0.1s before next country...")
            time.sleep(0.1)

except Exception as e:
    _stop_event.set()
    _spinner_thread.join()
    print(f"\nError fetching data: {e}")
    raise
else:
    _stop_event.set()
    _spinner_thread.join()

print(f"\n{'='*60}")
print(f"Fetch complete: {len(all_flows)} flow series retrieved")
print(f"{'='*60}\n")

# Detect which countries provide hourly vs 15-minute data based on row counts
hourly_countries = []
if country_row_counts:
    print("Detecting data frequency per country:")

    # Calculate expected row counts based on time range
    days_requested = (end - start).days
    expected_hourly = days_requested * 24
    expected_quarterly = days_requested * 24 * 4  # 15-minute = 4 per hour

    print(f"  Time range: {days_requested} days ({start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')})")
    print(f"  Expected rows: {expected_hourly} (hourly) or {expected_quarterly} (15-minute)")
    print()

    for country, counts in country_row_counts.items():
        avg_count = sum(counts) / len(counts) if counts else 0

        # Calculate distance to expected values
        dist_to_hourly = abs(avg_count - expected_hourly)
        dist_to_quarterly = abs(avg_count - expected_quarterly)

        # Determine which is closer
        if dist_to_hourly < dist_to_quarterly:
            hourly_countries.append(country)
            print(f"  {country}: {int(avg_count)} rows → HOURLY data detected (expected ~{expected_hourly})")
        else:
            print(f"  {country}: {int(avg_count)} rows → 15-minute data (expected ~{expected_quarterly})")

    if hourly_countries:
        print(f"\n  ℹ️  Countries with hourly data: {', '.join(hourly_countries)}")
        print(f"     These will be replicated to 15-minute resolution")
print()

# Combine all flows into a single DataFrame
if all_flows:
    print("\nCombining all cross-border flows...")

    # Merge all dataframes on datetime index
    combined_df = pd.DataFrame()

    for flow_name, flow_data in all_flows.items():
        if combined_df.empty:
            combined_df = flow_data.copy()
            combined_df.columns = [flow_name]
        else:
            flow_data_copy = flow_data.copy()
            flow_data_copy.columns = [flow_name]
            combined_df = combined_df.join(flow_data_copy, how='outer')

    # Convert to Europe/Amsterdam timezone
    combined_df.index = combined_df.index.tz_convert('Europe/Amsterdam')

    # ============================================================================
    # HANDLE MIXED FREQUENCY DATA (hourly + 15-minute)
    # ============================================================================
    print("\nProcessing mixed frequency data...")
    print(f"  Initial data: {len(combined_df)} rows")

    # Check if we have sub-hourly data (15-minute intervals)
    time_diffs = combined_df.index.to_series().diff()
    has_15min_data = (time_diffs == pd.Timedelta(minutes=15)).any()
    has_hourly_data = (time_diffs == pd.Timedelta(hours=1)).any()

    if has_15min_data and has_hourly_data:
        print(f"  ⚠️  Detected mixed frequency data (hourly + 15-minute)")
        print(f"  Creating complete 15-minute time series and forward filling...")

        # Create a complete 15-minute frequency index
        complete_index = pd.date_range(
            start=combined_df.index.min(),
            end=combined_df.index.max(),
            freq='15min',
            tz='Europe/Amsterdam'
        )

        # Reindex to 15-minute frequency, forward filling hourly values
        combined_df = combined_df.reindex(complete_index, method='ffill')
        print(f"  After filling: {len(combined_df)} rows (15-minute resolution)")

        # Special handling for hourly countries: replicate hourly values to all quarters
        if hourly_countries:
            hourly_country_cols = [col for col in combined_df.columns
                                   if any(country in col for country in hourly_countries)]
            if hourly_country_cols:
                print(f"\n  ℹ️  Replicating hourly values to all quarters for: {', '.join(hourly_countries)}...")
                for col in hourly_country_cols:
                    # Get hourly values (first value of each hour)
                    hourly_values = combined_df[col].resample('h').first()
                    # Replicate to all 15-minute intervals within each hour
                    combined_df[col] = hourly_values.reindex(combined_df.index, method='ffill')
                print(f"  ✓ Updated {len(hourly_country_cols)} columns: {hourly_country_cols}")

                # Recalculate totals after replication
                print(f"\n  ℹ️  Recalculating import_NL, export_NL, and netCroBoNL after hourly replication...")
                import_cols = [col for col in combined_df.columns if col.startswith('imp_')]
                export_cols = [col for col in combined_df.columns if col.startswith('exp_')]

                combined_df['import_NL'] = combined_df[import_cols].sum(axis=1)
                combined_df['export_NL'] = combined_df[export_cols].sum(axis=1)
                combined_df['netCroBoNL'] = combined_df['import_NL'] - combined_df['export_NL']
                print(f"  ✓ Totals recalculated")
    elif has_15min_data:
        print(f"  ✓ Data is at 15-minute resolution")
    else:
        print(f"  ✓ Data is at hourly resolution")

    # Fill any remaining NaN values with 0 (no flow)
    combined_df = combined_df.fillna(0)

    # Separate import and export columns
    import_cols = [col for col in combined_df.columns if col.startswith('imp_')]
    export_cols = [col for col in combined_df.columns if col.startswith('exp_')]

    # Calculate total imports and exports
    combined_df['import_NL'] = combined_df[import_cols].sum(axis=1)
    combined_df['export_NL'] = combined_df[export_cols].sum(axis=1)

    # Calculate net cross-border exchange (positive = import, negative = export)
    combined_df['netCroBoNL'] = combined_df['import_NL'] - combined_df['export_NL']

    print(f"  ✓ Combined data: {len(combined_df)} rows")
    print(f"    Columns: {list(combined_df.columns)}")
    print(f"\nData summary:")
    print(combined_df[['import_NL', 'export_NL', 'netCroBoNL']].describe().round(1))

    # Merge detailed data with historical detailed data if available
    if historical_detailed_data is not None:
        print(f"\nMerging with existing historical detailed data...")
        print(f"  Historical detailed data: {len(historical_detailed_data)} rows")
        print(f"  New detailed data: {len(combined_df)} rows")

        # Combine datasets
        print("\n  Concatenating detailed datasets...")
        combined_detailed = pd.concat([historical_detailed_data, combined_df])

        # Remove duplicates, keeping the most recent version
        combined_detailed = combined_detailed[~combined_detailed.index.duplicated(keep='last')]

        # Sort by datetime
        combined_detailed = combined_detailed.sort_index()

        print(f"  ✓ Merged detailed data: {len(combined_detailed)} rows")
        print(f"    Final date range: {combined_detailed.index.min().strftime('%Y-%m-%d %H:%M')} to {combined_detailed.index.max().strftime('%Y-%m-%d %H:%M')}")

        # Check for mixed frequency and standardize to 15-minute
        time_diffs_detailed = combined_detailed.index.to_series().diff()
        has_hourly_detailed = (time_diffs_detailed == pd.Timedelta(hours=1)).any()

        if has_hourly_detailed:
            print(f"\n  ℹ️  Converting detailed data to 15-minute resolution...")
            print(f"  Current detailed data: {len(combined_detailed)} rows")

            # Create complete 15-minute index
            complete_15min_index = pd.date_range(
                start=combined_detailed.index.min(),
                end=combined_detailed.index.max(),
                freq='15min',
                tz='Europe/Amsterdam'
            )

            # Reindex to 15-minute and forward fill
            combined_detailed = combined_detailed.reindex(complete_15min_index, method='ffill')

            print(f"  ✓ Detailed data resampled to 15-minute with ffill: {len(combined_detailed)} rows")

        # Special handling for hourly countries: replicate hourly values to all quarters
        if hourly_countries:
            hourly_country_cols = [col for col in combined_detailed.columns
                                   if any(country in col for country in hourly_countries)]
            if hourly_country_cols:
                print(f"\n  ℹ️  Replicating hourly values for: {', '.join(hourly_countries)}...")
                for col in hourly_country_cols:
                    # Get hourly values (first value of each hour)
                    hourly_values = combined_detailed[col].resample('h').first()
                    # Replicate to all 15-minute intervals within each hour
                    combined_detailed[col] = hourly_values.reindex(combined_detailed.index, method='ffill')
                print(f"  ✓ Updated {len(hourly_country_cols)} columns: {hourly_country_cols}")

                # Recalculate totals after replication
                print(f"\n  ℹ️  Recalculating totals after hourly replication...")
                import_cols = [col for col in combined_detailed.columns if col.startswith('imp_')]
                export_cols = [col for col in combined_detailed.columns if col.startswith('exp_')]

                combined_detailed['import_NL'] = combined_detailed[import_cols].sum(axis=1)
                combined_detailed['export_NL'] = combined_detailed[export_cols].sum(axis=1)
                combined_detailed['netCroBoNL'] = combined_detailed['import_NL'] - combined_detailed['export_NL']
                print(f"  ✓ Totals recalculated")

        # Use combined data for saving
        combined_df = combined_detailed
    else:
        print(f"\n  ℹ️  No historical detailed data to merge - saving only new data")

        # Still need to ensure new detailed data is at 15-minute resolution
        time_diffs_new_detailed = combined_df.index.to_series().diff()
        has_hourly_new_detailed = (time_diffs_new_detailed == pd.Timedelta(hours=1)).any()

        if has_hourly_new_detailed:
            print(f"\n  ℹ️  Converting new detailed data to 15-minute resolution...")
            print(f"  Current detailed data: {len(combined_df)} rows")

            # Create complete 15-minute index
            complete_15min_index = pd.date_range(
                start=combined_df.index.min(),
                end=combined_df.index.max(),
                freq='15min',
                tz='Europe/Amsterdam'
            )

            # Reindex to 15-minute and forward fill
            combined_df = combined_df.reindex(complete_15min_index, method='ffill')

            print(f"  ✓ Detailed data resampled to 15-minute with ffill: {len(combined_df)} rows")

        # Special handling for hourly countries: replicate hourly values to all quarters
        if hourly_countries:
            hourly_country_cols = [col for col in combined_df.columns
                                   if any(country in col for country in hourly_countries)]
            if hourly_country_cols:
                print(f"\n  ℹ️  Replicating hourly values for: {', '.join(hourly_countries)}...")
                for col in hourly_country_cols:
                    # Get hourly values (first value of each hour)
                    hourly_values = combined_df[col].resample('h').first()
                    # Replicate to all 15-minute intervals within each hour
                    combined_df[col] = hourly_values.reindex(combined_df.index, method='ffill')
                print(f"  ✓ Updated {len(hourly_country_cols)} columns: {hourly_country_cols}")

                # Recalculate totals after replication
                print(f"\n  ℹ️  Recalculating totals after hourly replication...")
                import_cols = [col for col in combined_df.columns if col.startswith('imp_')]
                export_cols = [col for col in combined_df.columns if col.startswith('exp_')]

                combined_df['import_NL'] = combined_df[import_cols].sum(axis=1)
                combined_df['export_NL'] = combined_df[export_cols].sum(axis=1)
                combined_df['netCroBoNL'] = combined_df['import_NL'] - combined_df['export_NL']
                print(f"  ✓ Totals recalculated")

    # Create output dataframe with only netCroBoNL
    crossborder_data = combined_df[['netCroBoNL']].copy()
    crossborder_data['netCroBoNL'] = crossborder_data['netCroBoNL'].astype(int)

    # Merge with existing historical data (already loaded at the start)
    if historical_data is not None:
        print(f"\nMerging with existing historical data...")
        print(f"  Historical data: {len(historical_data)} rows")
        print(f"    Date range: {historical_data.index.min().strftime('%Y-%m-%d %H:%M')} to {historical_data.index.max().strftime('%Y-%m-%d %H:%M')}")
        print(f"  New data: {len(crossborder_data)} rows")
        print(f"    Date range: {crossborder_data.index.min().strftime('%Y-%m-%d %H:%M')} to {crossborder_data.index.max().strftime('%Y-%m-%d %H:%M')}")

        # Combine datasets
        print("\n  Concatenating datasets...")
        combined_data = pd.concat([historical_data, crossborder_data])

        # Remove duplicates, keeping the most recent version
        combined_data = combined_data[~combined_data.index.duplicated(keep='last')]

        # Sort by datetime
        combined_data = combined_data.sort_index()

        print(f"  ✓ Merged data: {len(combined_data)} rows")
        print(f"    Final date range: {combined_data.index.min().strftime('%Y-%m-%d %H:%M')} to {combined_data.index.max().strftime('%Y-%m-%d %H:%M')}")

        # Check for mixed frequency after merge and standardize to 15-minute
        time_diffs_merged = combined_data.index.to_series().diff()
        has_15min_merged = (time_diffs_merged == pd.Timedelta(minutes=15)).any()
        has_hourly_merged = (time_diffs_merged == pd.Timedelta(hours=1)).any()

        if has_hourly_merged:
            print(f"\n  ℹ️  Converting to 15-minute resolution for consistency...")
            print(f"  Current data: {len(combined_data)} rows")

            # Create complete 15-minute index
            complete_15min_index = pd.date_range(
                start=combined_data.index.min(),
                end=combined_data.index.max(),
                freq='15min',
                tz='Europe/Amsterdam'
            )

            # Reindex to 15-minute and forward fill hourly values
            combined_data = combined_data.reindex(complete_15min_index, method='ffill')

            print(f"  ✓ Resampled to 15-minute with ffill: {len(combined_data)} rows")

        # Use combined data for saving
        crossborder_data = combined_data
    else:
        print(f"\n  ℹ️  No historical data to merge - saving only new data")

        # Still need to ensure new data is at 15-minute resolution
        time_diffs_new = crossborder_data.index.to_series().diff()
        has_hourly_new = (time_diffs_new == pd.Timedelta(hours=1)).any()

        if has_hourly_new:
            print(f"\n  ℹ️  Converting new data to 15-minute resolution...")
            print(f"  Current data: {len(crossborder_data)} rows")

            # Create complete 15-minute index
            complete_15min_index = pd.date_range(
                start=crossborder_data.index.min(),
                end=crossborder_data.index.max(),
                freq='15min',
                tz='Europe/Amsterdam'
            )

            # Reindex to 15-minute and forward fill
            crossborder_data = crossborder_data.reindex(complete_15min_index, method='ffill')

            print(f"  ✓ Resampled to 15-minute with ffill: {len(crossborder_data)} rows")

    # ============================================================================
    # DATA VALIDATION CHECKS
    # ============================================================================
    print("\n" + "="*60)
    print("PERFORMING DATA VALIDATION CHECKS")
    print("="*60)

    # Detect data frequency
    data_start = crossborder_data.index.min()
    data_end = crossborder_data.index.max()
    time_diffs_check = crossborder_data.index.to_series().diff()
    most_common_freq = time_diffs_check.mode()[0]

    is_15min = most_common_freq == pd.Timedelta(minutes=15)
    is_hourly = most_common_freq == pd.Timedelta(hours=1)

    if is_15min:
        expected_intervals = int((data_end - data_start).total_seconds() / 900) + 1  # 900s = 15min
        interval_type = "15-minute intervals"
        expected_freq = pd.Timedelta(minutes=15)
    else:
        expected_intervals = int((data_end - data_start).total_seconds() / 3600) + 1  # 3600s = 1hour
        interval_type = "hourly intervals"
        expected_freq = pd.Timedelta(hours=1)

    actual_rows = len(crossborder_data)

    print(f"\n1. Row Count Validation:")
    print(f"   Date range: {data_start.strftime('%Y-%m-%d %H:%M')} to {data_end.strftime('%Y-%m-%d %H:%M')}")
    print(f"   Data frequency: {interval_type}")
    print(f"   Expected intervals: {expected_intervals:,}")
    print(f"   Actual rows: {actual_rows:,}")

    if actual_rows == expected_intervals:
        print(f"   ✓ PASS: Row count matches expected intervals")
    elif actual_rows < expected_intervals:
        missing = expected_intervals - actual_rows
        print(f"   ⚠️  WARNING: Missing {missing} intervals ({missing/expected_intervals*100:.2f}%)")
    else:
        extra = actual_rows - expected_intervals
        print(f"   ⚠️  WARNING: {extra} extra rows found")

    # Check 2: Verify frequency and find gaps
    print(f"\n2. Time Series Continuity Check:")
    print(f"   Most common frequency: {most_common_freq}")

    # Find gaps (where diff is not the expected frequency)
    gaps = time_diffs_check[time_diffs_check != expected_freq]
    gaps = gaps.dropna()  # Remove first NaN

    if len(gaps) == 0:
        print(f"   ✓ PASS: No gaps found - continuous {interval_type} sequence")
    else:
        print(f"   ⚠️  WARNING: Found {len(gaps)} gaps in time series:")
        for i, (idx, gap_size) in enumerate(gaps.items()):
            if i >= 10:
                print(f"      ... and {len(gaps) - 10} more gaps")
                break
            try:
                pos = crossborder_data.index.get_loc(idx)
                if isinstance(pos, int) and pos > 0:
                    prev_timestamp = crossborder_data.index[pos - 1]
                    if isinstance(gap_size, pd.Timedelta):
                        if is_15min:
                            intervals_missing = int(gap_size.total_seconds() / 900) - 1
                        else:
                            intervals_missing = int(gap_size.total_seconds() / 3600) - 1
                        current_time = str(idx)
                        prev_time = prev_timestamp.strftime('%Y-%m-%d %H:%M')
                        print(f"      Gap at {current_time} (after {prev_time})")
                        print(f"        Duration: {gap_size} ({intervals_missing} intervals missing)")
            except Exception as e:
                print(f"      Error processing gap at position {i}: {e}")

    # Check 3: Check for duplicate timestamps
    print(f"\n3. Duplicate Timestamp Check:")
    duplicates = crossborder_data.index.duplicated()
    num_duplicates = duplicates.sum()

    if num_duplicates == 0:
        print(f"   ✓ PASS: No duplicate timestamps")
    else:
        print(f"   ✗ FAIL: Found {num_duplicates} duplicate timestamps")

    # Check 4: Check for missing values
    print(f"\n4. Missing Values Check:")
    missing_values = crossborder_data.isnull().sum().sum()

    if missing_values == 0:
        print(f"   ✓ PASS: No missing values (NaN)")
    else:
        print(f"   ⚠️  WARNING: Found {missing_values} missing values (NaN)")

    # Check 5: Data quality checks
    print(f"\n5. Data Quality Check:")
    min_net = crossborder_data['netCroBoNL'].min()
    max_net = crossborder_data['netCroBoNL'].max()
    mean_net = crossborder_data['netCroBoNL'].mean()

    print(f"   Net Cross-Border statistics (MW):")
    print(f"      Min:  {min_net:,.0f} MW (export)")
    print(f"      Max:  {max_net:,.0f} MW (import)")
    print(f"      Mean: {mean_net:,.0f} MW")

    # Summary
    print("\n" + "="*60)
    print("VALIDATION SUMMARY")
    print("="*60)

    validation_passed = (
        actual_rows == expected_intervals and
        len(gaps) == 0 and
        num_duplicates == 0 and
        missing_values == 0
    )

    if validation_passed:
        print("✓ ALL CHECKS PASSED - Data quality is excellent")
    else:
        print("⚠️  SOME CHECKS FAILED - Review warnings above")
        if actual_rows != expected_intervals or len(gaps) > 0:
            print("   → Missing intervals detected - data may be incomplete")
        if num_duplicates > 0:
            print("   → Duplicates found - already removed but investigate source")
        if missing_values > 0:
            print("   → NaN values found - consider filling or removing")

    print("="*60 + "\n")

    # ============================================================================
    # SAVE DATA TO CSV FILE
    # ============================================================================
    print("="*60)
    print("SAVING DATA TO FILE")
    print("="*60 + "\n")

    # Determine output filename based on actual data range
    output_start = crossborder_data.index.min()
    output_end = crossborder_data.index.max()

    # ============================================================================
    # SAVE EXTENDED VERSION (15-minute resolution)
    # ============================================================================
    output_filename_extended = f'netCrossBorderExchangeNL_{output_start.strftime("%Y%m%d")}_{output_end.strftime("%Y%m%d")}_MW_positive_import_extended.csv'
    output_file_extended = os.path.join(output_dir, output_filename_extended)

    # Reset index to save datetime as column
    crossborder_data_to_save = crossborder_data.copy()
    crossborder_data_to_save.index.name = 'datetime'
    crossborder_data_to_save = crossborder_data_to_save.reset_index()

    print(f"Saving EXTENDED cross-border exchange data (15-minute)...")
    print(f"  File: {output_filename_extended}")
    print(f"  Dataset: {len(crossborder_data_to_save)} rows ({interval_type})")
    print(f"  Date range: {output_start.strftime('%Y-%m-%d %H:%M')} to {output_end.strftime('%Y-%m-%d %H:%M')}")
    crossborder_data_to_save.to_csv(output_file_extended, index=False)
    print(f"✓ Extended data saved successfully!")
    print(f"  File location: {output_file_extended}")

    # ============================================================================
    # SAVE HOURLY VERSION (resampled to hourly mean)
    # ============================================================================
    print(f"\nCreating HOURLY version (resampling to hourly mean)...")
    crossborder_hourly = crossborder_data.resample('h').mean().round(0).astype(int)

    output_filename_hourly = f'netCrossBorderExchangeNL_{output_start.strftime("%Y%m%d")}_{output_end.strftime("%Y%m%d")}_MW_positive_import_hourly.csv'
    output_file_hourly = os.path.join(output_dir, output_filename_hourly)

    crossborder_hourly_to_save = crossborder_hourly.copy()
    crossborder_hourly_to_save.index.name = 'datetime'
    crossborder_hourly_to_save = crossborder_hourly_to_save.reset_index()

    print(f"  File: {output_filename_hourly}")
    print(f"  Dataset: {len(crossborder_hourly_to_save)} rows (hourly)")
    crossborder_hourly_to_save.to_csv(output_file_hourly, index=False)
    print(f"✓ Hourly data saved successfully!")
    print(f"  File location: {output_file_hourly}")

    # ============================================================================
    # SAVE DETAILED DATA - EXTENDED VERSION (15-minute resolution)
    # ============================================================================
    print(f"\nSaving EXTENDED detailed cross-border data (15-minute)...")
    detailed_filename_extended = f'CrossBorder_detailed_{output_start.strftime("%Y%m%d")}_{output_end.strftime("%Y%m%d")}_MW_extended.csv'
    detailed_file_extended = os.path.join(output_dir, detailed_filename_extended)

    # Prepare detailed data for saving
    detailed_data_to_save = combined_df.copy()
    detailed_data_to_save.index.name = 'datetime'
    detailed_data_to_save = detailed_data_to_save.reset_index()

    # Convert all numeric columns to integers
    numeric_cols = detailed_data_to_save.select_dtypes(include=['float64', 'float32']).columns
    for col in numeric_cols:
        detailed_data_to_save[col] = detailed_data_to_save[col].astype(int)

    print(f"  File: {detailed_filename_extended}")
    print(f"  Dataset: {len(detailed_data_to_save)} rows, {len(detailed_data_to_save.columns)} columns")
    print(f"  Columns: {list(detailed_data_to_save.columns)}")
    detailed_data_to_save.to_csv(detailed_file_extended, index=False)
    print(f"✓ Extended detailed data saved successfully!")
    print(f"  File location: {detailed_file_extended}")

    # ============================================================================
    # SAVE DETAILED DATA - HOURLY VERSION (resampled to hourly mean)
    # ============================================================================
    print(f"\nCreating HOURLY detailed data (resampling to hourly mean)...")
    detailed_hourly = combined_df.resample('h').mean().round(0).astype(int)

    detailed_filename_hourly = f'CrossBorder_detailed_{output_start.strftime("%Y%m%d")}_{output_end.strftime("%Y%m%d")}_MW_hourly.csv'
    detailed_file_hourly = os.path.join(output_dir, detailed_filename_hourly)

    detailed_hourly_to_save = detailed_hourly.copy()
    detailed_hourly_to_save.index.name = 'datetime'
    detailed_hourly_to_save = detailed_hourly_to_save.reset_index()

    # Convert all numeric columns to integers
    numeric_cols_hourly = detailed_hourly_to_save.select_dtypes(include=['float64', 'float32']).columns
    for col in numeric_cols_hourly:
        detailed_hourly_to_save[col] = detailed_hourly_to_save[col].astype(int)

    print(f"  File: {detailed_filename_hourly}")
    print(f"  Dataset: {len(detailed_hourly_to_save)} rows, {len(detailed_hourly_to_save.columns)} columns")
    detailed_hourly_to_save.to_csv(detailed_file_hourly, index=False)
    print(f"✓ Hourly detailed data saved successfully!")
    print(f"  File location: {detailed_file_hourly}")

    # Print sample of data
    print("\n" + "="*60)
    print("DATA SAMPLE - Net Exchange EXTENDED (15-minute, first 7 rows)")
    print("="*60)
    print(crossborder_data_to_save.head(7))

    print("\n" + "="*60)
    print("DATA SAMPLE - Net Exchange HOURLY (first 7 rows)")
    print("="*60)
    print(crossborder_hourly_to_save.head(7))

    print("\n" + "="*60)
    print("DATA SAMPLE - Detailed EXTENDED (15-minute, first 7 rows)")
    print("="*60)
    print(detailed_data_to_save.head(7))

    print("\n" + "="*60)
    print("DATA SAMPLE - Detailed HOURLY (first 7 rows)")
    print("="*60)
    print(detailed_hourly_to_save.head(7))

    print("\n" + "="*60)
    print("FILES SAVED SUMMARY")
    print("="*60)
    print(f"✓ Net Exchange Extended: {output_filename_extended}")
    print(f"✓ Net Exchange Hourly:   {output_filename_hourly}")
    print(f"✓ Detailed Extended:     {detailed_filename_extended}")
    print(f"✓ Detailed Hourly:       {detailed_filename_hourly}")

    print("\n" + "="*60)
    print("SCRIPT COMPLETED SUCCESSFULLY")
    print("="*60)

else:
    print("\n❌ No cross-border flow data was retrieved.")
