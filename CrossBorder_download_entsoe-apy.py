import pandas as pd
import numpy as np
from datetime import date, timedelta
import os
os.system('clear')

# Entsoe Cross-Border Physical Flows Data Download Script
# Using entsoe-apy library (https://pypi.org/project/entsoe-apy/)
print("Entsoe Cross-Border Physical Flows Data Download Script (using entsoe-apy)")

# Load API key from .env file
print("Loading API key from .env file...")
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('ENTSOE_API_KEY')
if not api_key:
    api_key = os.getenv('ENTSOE_API_BIRDVIEW')
if not api_key:
    # entsoe-apy uses ENTSOE_API environment variable
    api_key = os.getenv('ENTSOE_API')

if not api_key:
    raise ValueError("No API key found. Please set ENTSOE_API_KEY, ENTSOE_API_BIRDVIEW, or ENTSOE_API in .env file")

# Set environment variable for entsoe-apy (it expects ENTSOE_API)
os.environ['ENTSOE_API'] = api_key

# Import entsoe-apy modules
try:
    from entsoe.Transmission import PhysicalFlows
    from entsoe.utils import extract_records, add_timestamps
    from entsoe.utils.mappings import mappings
except ImportError as e:
    raise ImportError(f"Failed to import entsoe-apy. Please install it with: pip install entsoe-apy\nError: {e}")

# EIC code mappings based on entsoe-apy mappings (https://entsoe-apy.berrisch.biz/mappings/)
# These are the correct EIC codes for cross-border flows
EIC_CODES = {
    'NL': '10YNL----------L',      # Netherlands
    'BE': '10YBE----------2',       # Belgium
    'DE-LU': '10Y1001A1001A82H',    # Germany-Luxembourg (DE-LU with dash, not underscore)
    'DK1': '10YDK-1--------W',      # Denmark DK1 (without underscore)
    'GB': '10YGB----------A',       # Great Britain
    'NO2': '10YNO-2--------T',      # Norway NO2 (without underscore)
}

# Reverse lookup: EIC code to area name for display
EIC_TO_NAME = {v: k for k, v in EIC_CODES.items()}

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
# Using EIC codes from entsoe-apy mappings (https://entsoe-apy.berrisch.biz/mappings/)
# IMPORTANT: Country codes based on entsoe-apy mappings:
# - Germany: Uses 'DE-LU' (with dash) → EIC: 10Y1001A1001A82H
# - Norway: Uses 'NO2' (without underscore) → EIC: 10YNO-2--------T
# - Denmark: Uses 'DK1' (without underscore) → EIC: 10YDK-1--------W
# - Great Britain: Uses 'GB' → EIC: 10YGB----------A (stopped reporting after 15 June 2021)
# - Belgium: Uses 'BE' → EIC: 10YBE----------2 (we also use Elia data for BE)
neighboring_countries = {
    'BE': ('Belgium', EIC_CODES['BE']),
    'DE-LU': ('Germany (DE-LU)', EIC_CODES['DE-LU']),  # Note: dash, not underscore
    'DK1': ('Denmark (DK1)', EIC_CODES['DK1']),  # Note: no underscore
    'GB': ('Great Britain', EIC_CODES['GB']),  # NOTE: Data discontinued after 15 June 2021 (Brexit)
    'NO2': ('Norway (NO2)', EIC_CODES['NO2'])  # Note: no underscore
}

# Mapping from API area codes to column name country codes (for backward compatibility)
# This allows us to use correct API codes (e.g., DE-LU) while maintaining column names (e.g., DE)
country_code_to_column_name = {
    'BE': 'BE',
    'DE-LU': 'DE',  # API uses DE-LU, but columns should be DE for compatibility
    'DK1': 'DK_1',  # Keep DK_1 for column names (backward compatibility)
    'GB': 'GB',
    'NO2': 'NO_2'   # Keep NO_2 for column names (backward compatibility)
}

# Country-specific start dates (for cables that went live after 2018)
country_start_dates = {
    'DK1': pd.Timestamp('20190801', tz='Europe/Amsterdam'),  # COBRAcable live Sept 2019
}

# Country-specific end dates (for countries that stopped reporting)
country_end_dates = {
    'GB': pd.Timestamp('20210615', tz='Europe/Amsterdam'),  # GB stopped reporting after this date
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

def fetch_crossborder_flows_entsoe_apy(in_domain_eic, out_domain_eic, start, end):
    """Fetch cross-border flows using entsoe-apy library"""
    try:
        # Convert timestamps to format expected by entsoe-apy (YYYYMMDDHHMM)
        period_start = start.strftime('%Y%m%d%H%M')
        period_end = end.strftime('%Y%m%d%H%M')
        
        # Query using PhysicalFlows class from entsoe-apy
        result = PhysicalFlows(
            in_domain=in_domain_eic,
            out_domain=out_domain_eic,
            period_start=int(period_start),
            period_end=int(period_end),
        ).query_api()
        
        # Extract records and convert to DataFrame
        records = extract_records(result)
        if not records:
            return None
            
        records = add_timestamps(records)
        df = pd.DataFrame(records)
        
        if df.empty:
            return None
        
        # Parse the data - entsoe-apy returns structured data
        # The exact column names depend on the API response structure
        # Common patterns: quantity, value, flow, or time_series.period.point.quantity
        
        # Try multiple strategies to find the flow values and timestamps
        flow_col = None
        time_col = None
        
        # Strategy 1: Look for common column name patterns
        for col in df.columns:
            col_lower = col.lower()
            if not flow_col and ('quantity' in col_lower or 'flow' in col_lower or 'value' in col_lower or 'amount' in col_lower):
                # Make sure it's numeric
                if pd.api.types.is_numeric_dtype(df[col]):
                    flow_col = col
            if not time_col and ('time' in col_lower or 'period' in col_lower or 'start' in col_lower or 'datetime' in col_lower):
                time_col = col
        
        # Strategy 2: If no time column found, check if index is datetime
        if not time_col and isinstance(df.index, pd.DatetimeIndex):
            time_col = 'index'
            df = df.reset_index()
        
        # Strategy 3: Use first numeric column as flow, first datetime column as time
        if not flow_col:
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                flow_col = numeric_cols[0]
        
        if not time_col:
            datetime_cols = df.select_dtypes(include=['datetime64']).columns
            if len(datetime_cols) > 0:
                time_col = datetime_cols[0]
        
        if not flow_col or not time_col:
            # Last resort: print structure for debugging
            print(f"      ⚠️  Warning: Could not parse entsoe-apy response structure")
            print(f"         Columns: {list(df.columns)}")
            print(f"         DataFrame shape: {df.shape}")
            print(f"         First few rows:\n{df.head()}")
            return None
        
        # Set time column as index
        if time_col != 'index':
            df[time_col] = pd.to_datetime(df[time_col], utc=True)
            df = df.set_index(time_col)
        else:
            df.index = pd.to_datetime(df.index, utc=True)
        
        # Ensure timezone-aware index
        if df.index.tz is None:
            df.index = df.index.tz_localize('UTC')
        df.index = df.index.tz_convert('Europe/Amsterdam')
        
        # Get flow values
        flow_series = df[flow_col].copy()
        flow_series = flow_series.sort_index()
        
        # Remove any NaN values
        flow_series = flow_series.dropna()
        
        if len(flow_series) == 0:
            return None
        
        # Convert to DataFrame with single column
        flow_df = flow_series.to_frame(f'{in_domain_eic}_to_{out_domain_eic}')
        
        return flow_df
        
    except Exception as e:
        error_msg = str(e) if str(e) else repr(e)
        # Check if it's a "no data" type error
        if 'NoMatchingData' in error_msg or 'no data' in error_msg.lower() or '404' in error_msg or 'not found' in error_msg.lower():
            return None  # No data available
        # Print error for debugging but don't fail completely
        print(f"      ⚠️  Error in fetch_crossborder_flows_entsoe_apy: {error_msg}")
        raise  # Re-raise other errors

def fetch_crossborder_flows_in_chunks(in_domain_eic, out_domain_eic, area_from_name, area_to_name, start, end):
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
                # Query cross-border flows using entsoe-apy with timeout (use UTC timestamps)
                chunk_data = api_call_with_timeout(
                    fetch_crossborder_flows_entsoe_apy,
                    api_timeout,
                    in_domain_eic=in_domain_eic,
                    out_domain_eic=out_domain_eic,
                    start=current_start_utc,
                    end=requested_end_utc
                )

                if chunk_data is None or len(chunk_data) == 0:
                    current_start = requested_end
                    success = True
                    time.sleep(0.2)
                    continue

                # Ensure data is in Europe/Amsterdam timezone
                if chunk_data.index.tz is None:
                    chunk_data.index = chunk_data.index.tz_localize('Europe/Amsterdam')
                elif chunk_data.index.tz != pd.Timestamp.now(tz='Europe/Amsterdam').tz:
                    chunk_data.index = chunk_data.index.tz_convert('Europe/Amsterdam')

                # Check if chunk contains only zeros
                numeric_cols = chunk_data.select_dtypes(include=[np.number]).columns
                if len(numeric_cols) > 0:
                    chunk_sum = chunk_data[numeric_cols].abs().sum().sum()
                    if chunk_sum == 0:
                        print(f"      ⚠️  WARNING: Chunk {chunk_count} contains only zeros - possible data issue!")

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
                print(f"  ⚠️  Timeout fetching {area_from_name}->{area_to_name}: {e}")
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"  ✗ Max retries reached for {area_from_name}->{area_to_name} after timeouts")
                    success = True  # Skip this border
                    break
                else:
                    delay = base_delay * (2 ** (retry_count - 1))
                    print(f"  ⚠️  Retrying {area_from_name}->{area_to_name} in {delay}s...")
                    time.sleep(delay)

            except Exception as e:
                error_msg = str(e) if str(e) else repr(e)
                
                # Check if it's a "no data" error
                if 'NoMatchingData' in error_msg or 'no data' in error_msg.lower() or '404' in error_msg:
                    print(f"  ℹ️  No data available for {area_from_name}->{area_to_name}")
                    success = True  # Skip this border
                    break
                
                status_code = None
                response = getattr(e, "response", None)
                if response is not None:
                    status_code = getattr(response, "status_code", None)

                # Only retry on specific error codes (503 = Service Unavailable, timeouts)
                if status_code not in (503,):
                    print(f"  ✗ Non-retryable error fetching {area_from_name}->{area_to_name}: {error_msg}")
                    success = True  # Skip this border
                    break

                retry_count += 1
                if retry_count >= max_retries:
                    print(f"  ✗ Max retries reached for {area_from_name}->{area_to_name}: {error_msg}")
                    success = True  # Skip this border
                    break
                else:
                    delay = base_delay * (2 ** (retry_count - 1))
                    print(f"  ⚠️  Retrying {area_from_name}->{area_to_name} in {delay}s... (Error: {error_msg})")
                    time.sleep(delay)

        if success:
            time.sleep(0.2)

    # Concatenate all chunks
    if all_data:
        combined_data = pd.concat(all_data)
        combined_data = combined_data[~combined_data.index.duplicated(keep='first')]
        combined_data = combined_data.sort_index()
        
        # Check if combined data contains only zeros
        numeric_cols = combined_data.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            total_sum = combined_data[numeric_cols].abs().sum().sum()
            non_zero_count = (combined_data[numeric_cols].abs() > 0).sum().sum()
            total_values = len(combined_data) * len(numeric_cols)
            
            if total_sum == 0:
                print(f"      ⚠️  WARNING: All data for {area_from_name}->{area_to_name} is ZERO!")
                print(f"         This may indicate:")
                print(f"         - Incorrect EIC code")
                print(f"         - No data available for this border/period")
                print(f"         - API returned empty/zero values")
            elif non_zero_count < total_values * 0.1:  # Less than 10% non-zero
                zero_percentage = (1 - non_zero_count / total_values) * 100
                print(f"      ⚠️  WARNING: {zero_percentage:.1f}% of values are zero ({non_zero_count}/{total_values} non-zero)")
                print(f"         This may indicate data quality issues")

        return combined_data
    else:
        return None

_stop_event = threading.Event()
_spinner_thread = threading.Thread(target=_spinner, args=(_stop_event,), daemon=True)
_spinner_thread.start()

# Fetch flows for all borders
nl_eic = EIC_CODES['NL']
print(f"\nFetching cross-border flows for {len(neighboring_countries)} borders...")
print(f"Netherlands EIC code: {nl_eic}")
all_flows = {}
country_row_counts = {}  # Track row counts to detect hourly vs 15-minute data
api_call_counter = 0  # Global API call counter for rate limiting

try:
    border_count = 0
    total_borders = len(neighboring_countries) * 2  # 2 directions per country

    for area_code, (area_name, area_eic) in neighboring_countries.items():
        # Skip Belgium - data comes from Elia CSV file, not ENTSO-E
        if area_code == 'BE':
            print(f"\n  ℹ️  {area_name}: Skipping ENTSO-E fetch (data comes from Elia CSV file)")
            border_count += 2  # Skip both directions
            continue
            
        # Use country-specific start date if available
        country_start = country_start_dates.get(area_code, start)
        # Use country-specific end date if available (e.g., GB stopped reporting after Brexit)
        country_end = country_end_dates.get(area_code, end)
        # Use the earlier of country_end or global end
        effective_end = min(country_end, end)

        if country_start != start:
            print(f"\n  ℹ️  {area_name}: Using custom start date {country_start.strftime('%Y-%m-%d')} (cable went live later)")
        
        if country_end < end:
            print(f"\n  ⚠️  {area_name}: Data only available until {country_end.strftime('%Y-%m-%d')} (stopped reporting to ENTSO-E)")

        # Skip if start is after end
        if country_start >= effective_end:
            print(f"\n  ⚠️  {area_name}: Skipping (start date {country_start.strftime('%Y-%m-%d')} is after end date {effective_end.strftime('%Y-%m-%d')})")
            border_count += 2  # Skip both directions
            continue

        border_count += 1
        print(f"\n  [{border_count}/{total_borders}] Fetching NL -> {area_name}...")
        print(f"      Using EIC codes: {nl_eic} -> {area_eic}")
        flow_export = fetch_crossborder_flows_in_chunks(nl_eic, area_eic, 'NL', area_name, country_start, effective_end)
        if flow_export is not None:
            # Check if export flow contains only zeros
            numeric_cols = flow_export.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                export_sum = flow_export[numeric_cols].abs().sum().sum()
                export_non_zero = (flow_export[numeric_cols].abs() > 0).sum().sum()
                export_total = len(flow_export) * len(numeric_cols)
                
                if export_sum == 0:
                    print(f"    ⚠️  WARNING: Export flow (NL->{area_name}) contains ONLY ZEROS!")
                    print(f"       This likely indicates a problem with EIC code '{area_eic}' or no data available")
                elif export_non_zero < export_total * 0.1:
                    zero_pct = (1 - export_non_zero / export_total) * 100
                    print(f"    ⚠️  WARNING: {zero_pct:.1f}% of export values are zero ({export_non_zero}/{export_total} non-zero)")
            
            # Use column name mapping for backward compatibility
            column_country_code = country_code_to_column_name.get(area_code, area_code)
            all_flows[f'exp_{column_country_code}'] = flow_export
            export_rows = len(flow_export)
            print(f"    ✓ Received {export_rows} rows")
            if area_code not in country_row_counts:
                country_row_counts[area_code] = []
            country_row_counts[area_code].append(export_rows)

        # Small delay between directions
        time.sleep(0.1)

        border_count += 1
        print(f"  [{border_count}/{total_borders}] Fetching {area_name} -> NL...")
        print(f"      Using EIC codes: {area_eic} -> {nl_eic}")
        flow_import = fetch_crossborder_flows_in_chunks(area_eic, nl_eic, area_name, 'NL', country_start, effective_end)
        if flow_import is not None:
            # Check if import flow contains only zeros
            numeric_cols = flow_import.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                import_sum = flow_import[numeric_cols].abs().sum().sum()
                import_non_zero = (flow_import[numeric_cols].abs() > 0).sum().sum()
                import_total = len(flow_import) * len(numeric_cols)
                
                if import_sum == 0:
                    print(f"    ⚠️  WARNING: Import flow ({area_name}->NL) contains ONLY ZEROS!")
                    print(f"       This likely indicates a problem with EIC code '{area_eic}' or no data available")
                elif import_non_zero < import_total * 0.1:
                    zero_pct = (1 - import_non_zero / import_total) * 100
                    print(f"    ⚠️  WARNING: {zero_pct:.1f}% of import values are zero ({import_non_zero}/{import_total} non-zero)")
            
            # Use column name mapping for backward compatibility
            column_country_code = country_code_to_column_name.get(area_code, area_code)
            all_flows[f'imp_{column_country_code}'] = flow_import
            import_rows = len(flow_import)
            print(f"    ✓ Received {import_rows} rows")
            if area_code not in country_row_counts:
                country_row_counts[area_code] = []
            country_row_counts[area_code].append(import_rows)

        # Delay between country pairs to respect API limits
        if border_count < total_borders:
            print(f"    Waiting 0.1s before next country...")
            time.sleep(0.1)

except Exception as e:
    _stop_event.set()
    _spinner_thread.join()
    print(f"\nError fetching data: {e}")
    import traceback
    traceback.print_exc()
    raise
else:
    _stop_event.set()
    _spinner_thread.join()

print(f"\n{'='*60}")
print(f"Fetch complete: {len(all_flows)} flow series retrieved")
print(f"{'='*60}\n")

# Check for zero-only flows and report summary
if all_flows:
    print("Checking for zero-only data...")
    zero_flows = []
    low_data_flows = []
    
    for flow_name, flow_data in all_flows.items():
        numeric_cols = flow_data.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            total_sum = flow_data[numeric_cols].abs().sum().sum()
            non_zero_count = (flow_data[numeric_cols].abs() > 0).sum().sum()
            total_values = len(flow_data) * len(numeric_cols)
            
            if total_sum == 0:
                zero_flows.append(flow_name)
            elif non_zero_count < total_values * 0.1:  # Less than 10% non-zero
                zero_pct = (1 - non_zero_count / total_values) * 100
                low_data_flows.append((flow_name, zero_pct, non_zero_count, total_values))
    
    if zero_flows:
        print(f"\n  ⚠️  CRITICAL: {len(zero_flows)} flow(s) contain ONLY ZEROS:")
        for flow in zero_flows:
            print(f"     - {flow}")
        print(f"     → This likely indicates incorrect EIC codes or no data available")
        print(f"     → Check the EIC code mappings in the script")
    
    if low_data_flows:
        print(f"\n  ⚠️  WARNING: {len(low_data_flows)} flow(s) have very low data quality (<10% non-zero):")
        for flow_name, zero_pct, non_zero, total in low_data_flows:
            print(f"     - {flow_name}: {zero_pct:.1f}% zeros ({non_zero}/{total} non-zero)")
    
    if not zero_flows and not low_data_flows:
        print("  ✓ All flows contain meaningful data (no zero-only flows detected)")
    
    print()

# NOTE: The rest of the script (data processing, combining, saving, etc.) is identical to the original
# For brevity, I'm including a note that the remaining code should be copied from the original script
# starting from the "Detect which countries provide hourly vs 15-minute data" section

# Detect which countries provide hourly vs 15-minute data based on row counts
hourly_countries = []
if country_row_counts:
    print("Detecting data frequency per country:")
    print()

    for country, counts in country_row_counts.items():
        avg_count = sum(counts) / len(counts) if counts else 0

        # Use country-specific start date for calculating expected rows
        country_start = country_start_dates.get(country, start)
        days_requested = (end - country_start).days
        expected_hourly = days_requested * 24
        expected_quarterly = days_requested * 24 * 4  # 15-minute = 4 per hour

        # Calculate distance to expected values
        dist_to_hourly = abs(avg_count - expected_hourly)
        dist_to_quarterly = abs(avg_count - expected_quarterly)

        # Determine which is closer
        if dist_to_hourly < dist_to_quarterly:
            hourly_countries.append(country)
            print(f"  {country}: {int(avg_count)} rows → HOURLY data detected (expected ~{expected_hourly} for {days_requested} days)")
        else:
            print(f"  {country}: {int(avg_count)} rows → 15-minute data (expected ~{expected_quarterly} for {days_requested} days)")

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

    # ============================================================================
    # IMPORT BELGIUM DATA FROM CSV FILE (more accurate 15-minute resolution)
    # ============================================================================
    print("\n" + "="*60)
    print("IMPORTING BELGIUM DATA FROM CSV FILE")
    print("="*60)
    
    # Find the latest Belgium CSV file
    belgium_csv_files = [f for f in os.listdir(output_dir) if f.startswith('Belgium_NL_crossborder_') and f.endswith('_MW_quarterly.csv')]
    
    if belgium_csv_files:
        # Sort files by name to get the most recent one
        belgium_csv_files.sort(reverse=True)
        latest_belgium_file = belgium_csv_files[0]
        belgium_file_path = os.path.join(output_dir, latest_belgium_file)
        
        print(f"\n  ✓ Found Belgium CSV file: {latest_belgium_file}")
        
        try:
            # Load Belgium CSV data
            belgium_data = pd.read_csv(belgium_file_path)
            
            # Convert datetime column
            belgium_data['datetime'] = pd.to_datetime(belgium_data['datetime'], utc=True)
            belgium_data['datetime'] = belgium_data['datetime'].dt.tz_convert('Europe/Amsterdam')
            
            # Set datetime as index
            belgium_data = belgium_data.set_index('datetime')
            
            print(f"  Loaded {len(belgium_data)} rows from Belgium CSV")
            print(f"    Date range: {belgium_data.index.min().strftime('%Y-%m-%d %H:%M')} to {belgium_data.index.max().strftime('%Y-%m-%d %H:%M')}")
            
            # Parse import_BE_NL_MW into separate import and export columns
            # Positive values = import into NL (from Belgium) → imp_BE
            # Negative values = export from NL (to Belgium) → exp_BE (positive values only)
            belgium_import = belgium_data['import_BE_NL_MW'].copy()
            belgium_import[belgium_import < 0] = 0  # Set negative values to 0 for import
            
            # exp_BE = export from NL into Belgium (positive values only)
            belgium_export = -belgium_data['import_BE_NL_MW'].copy()
            belgium_export[belgium_export < 0] = 0  # Set negative values to 0 for export
            
            # Ensure Belgium data is at 15-minute resolution
            time_diffs_belgium = belgium_data.index.to_series().diff()
            has_15min_belgium = (time_diffs_belgium == pd.Timedelta(minutes=15)).any()
            
            if not has_15min_belgium:
                print(f"  ⚠️  Belgium data is not at 15-minute resolution, converting...")
                # Create complete 15-minute index
                complete_15min_index = pd.date_range(
                    start=belgium_data.index.min(),
                    end=belgium_data.index.max(),
                    freq='15min',
                    tz='Europe/Amsterdam'
                )
                belgium_import = belgium_import.reindex(complete_15min_index, method='ffill')
                belgium_export = belgium_export.reindex(complete_15min_index, method='ffill')
                print(f"  ✓ Converted to 15-minute resolution: {len(belgium_import)} rows")
            
            # Align Belgium data with combined_df index
            # Find overlapping time range
            overlap_start = max(combined_df.index.min(), belgium_import.index.min())
            overlap_end = min(combined_df.index.max(), belgium_import.index.max())
            
            if overlap_start <= overlap_end:
                print(f"\n  Overwriting Belgium columns in combined_df...")
                print(f"    Overlap range: {overlap_start.strftime('%Y-%m-%d %H:%M')} to {overlap_end.strftime('%Y-%m-%d %H:%M')}")
                
                # Create aligned series for the overlap period
                # Use forward fill to align 15-minute data, then backward fill for any remaining gaps
                belgium_import_aligned = belgium_import.reindex(combined_df.index, method='ffill')
                belgium_import_aligned = belgium_import_aligned.bfill()
                belgium_export_aligned = belgium_export.reindex(combined_df.index, method='ffill')
                belgium_export_aligned = belgium_export_aligned.bfill()
                
                # Only overwrite where Belgium data exists (not NaN) and within overlap range
                overlap_mask = (combined_df.index >= overlap_start) & (combined_df.index <= overlap_end)
                mask_import = overlap_mask & ~belgium_import_aligned.isna()
                mask_export = overlap_mask & ~belgium_export_aligned.isna()
                
                # Ensure columns exist
                if 'imp_BE' not in combined_df.columns:
                    combined_df['imp_BE'] = 0
                if 'exp_BE' not in combined_df.columns:
                    combined_df['exp_BE'] = 0
                
                # Overwrite Belgium columns
                combined_df.loc[mask_import, 'imp_BE'] = belgium_import_aligned[mask_import].astype(int)
                combined_df.loc[mask_export, 'exp_BE'] = belgium_export_aligned[mask_export].astype(int)
                
                overwritten_count = mask_import.sum()
                print(f"  ✓ Overwritten {overwritten_count} rows for Belgium import/export")
                
                # Recalculate totals after overwriting Belgium data
                print(f"\n  Recalculating totals after Belgium data update...")
                import_cols = [col for col in combined_df.columns if col.startswith('imp_')]
                export_cols = [col for col in combined_df.columns if col.startswith('exp_')]
                
                combined_df['import_NL'] = combined_df[import_cols].sum(axis=1)
                combined_df['export_NL'] = combined_df[export_cols].sum(axis=1)
                combined_df['netCroBoNL'] = combined_df['import_NL'] - combined_df['export_NL']
                
                print(f"  ✓ Totals recalculated with updated Belgium data")
            else:
                print(f"  ⚠️  No overlap between Belgium CSV data and combined_df")
                print(f"    Belgium CSV: {belgium_import.index.min()} to {belgium_import.index.max()}")
                print(f"    Combined DF: {combined_df.index.min()} to {combined_df.index.max()}")
                
        except Exception as e:
            print(f"  ✗ Error loading/processing Belgium CSV file: {e}")
            import traceback
            traceback.print_exc()
    else:
        print(f"\n  ℹ️  No Belgium CSV file found (Belgium_NL_crossborder_*_MW_quarterly.csv)")
        print(f"    Skipping Belgium data import")
    
    print("="*60 + "\n")

    # Create output dataframe with only netCroBoNL
    # Note: netCroBoNL has been recalculated after Belgium data processing above
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
