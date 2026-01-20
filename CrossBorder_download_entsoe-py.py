import pandas as pd
import numpy as np
from datetime import date, timedelta
from entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError
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
        first_date = historical_data.index.min()
        print(f"  Existing data: {len(historical_data)} rows")
        print(f"    Date range: {first_date.strftime('%Y-%m-%d %H:%M')} to {last_date.strftime('%Y-%m-%d %H:%M')}")

        # ------------------------------------------------------------------
        # VALIDATE HISTORICAL NET DATA (row count & gaps)
        # If validation fails, discard historical_data and refetch from 2018
        # ------------------------------------------------------------------
        time_diffs_hist = historical_data.index.to_series().diff()
        most_common_freq_hist = time_diffs_hist.mode()[0]
        is_15min_hist = most_common_freq_hist == pd.Timedelta(minutes=15)
        is_hourly_hist = most_common_freq_hist == pd.Timedelta(hours=1)

        if is_15min_hist:
            expected_intervals_hist = int((last_date - first_date).total_seconds() / 900) + 1
            expected_freq_hist = pd.Timedelta(minutes=15)
        else:
            expected_intervals_hist = int((last_date - first_date).total_seconds() / 3600) + 1
            expected_freq_hist = pd.Timedelta(hours=1)

        actual_rows_hist = len(historical_data)
        gaps_hist = time_diffs_hist[time_diffs_hist != expected_freq_hist].dropna()

        if actual_rows_hist != expected_intervals_hist or len(gaps_hist) > 0:
            print("\n  ⚠️  Detected gaps or inconsistent row count in existing net CSV.")
            print(f"     Expected intervals: {expected_intervals_hist}, actual rows: {actual_rows_hist}")
            print(f"     Gaps found: {len(gaps_hist)}")
            print("     → Discarding historical net data and refetching from 2018-01-01 for a clean series.")
            historical_data = None
            start = pd.Timestamp('20180101', tz='Europe/Amsterdam')
        else:
            # Start fetching from the next hour after the last existing data
            start = last_date + pd.Timedelta(hours=1)
            print(f"\n  → Historical net data is consistent.")
            print(f"  → Will fetch NEW data from {start.strftime('%Y-%m-%d %H:%M')} onwards")

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

        # ------------------------------------------------------------------
        # VALIDATE HISTORICAL DETAILED DATA (row count, gaps, country columns)
        # If validation fails, discard and refetch detailed data from 2018.
        # ------------------------------------------------------------------
        first_det = historical_detailed_data.index.min()
        last_det = historical_detailed_data.index.max()
        time_diffs_det = historical_detailed_data.index.to_series().diff()
        most_common_freq_det = time_diffs_det.mode()[0]
        is_15min_det = most_common_freq_det == pd.Timedelta(minutes=15)
        if is_15min_det:
            expected_intervals_det = int((last_det - first_det).total_seconds() / 900) + 1
            expected_freq_det = pd.Timedelta(minutes=15)
        else:
            expected_intervals_det = int((last_det - first_det).total_seconds() / 3600) + 1
            expected_freq_det = pd.Timedelta(hours=1)

        actual_rows_det = len(historical_detailed_data)
        gaps_det = time_diffs_det[time_diffs_det != expected_freq_det].dropna()

        # Check that all configured non-BE countries have both imp_/exp_ columns
        expected_countries = [cc for cc in neighboring_countries.keys() if cc != 'BE']
        expected_cols = []
        for cc in expected_countries:
            col_code = country_code_to_column_name.get(cc, cc)
            expected_cols.extend([f'imp_{col_code}', f'exp_{col_code}'])

        missing_cols = [c for c in expected_cols if c not in historical_detailed_data.columns]

        if actual_rows_det != expected_intervals_det or len(gaps_det) > 0 or missing_cols:
            print("\n  ⚠️  Detected issues in existing detailed CSV:")
            print(f"     Expected intervals: {expected_intervals_det}, actual rows: {actual_rows_det}")
            print(f"     Gaps found: {len(gaps_det)}")
            if missing_cols:
                print(f"     Missing expected country columns: {missing_cols}")
            print("     → Discarding historical detailed data and refetching from 2018-01-01.")
            historical_detailed_data = None

    except Exception as e:
        print(f"  ⚠️  Error reading existing detailed file: {e}")
        historical_detailed_data = None

# Define parameters for data retrieval
print("\nData Retrieval Parameters:")
print(f'Country Code: {country_code}')
print(f'Start date: {start}')
print(f'End date:   {end}')

# Define neighboring countries for Netherlands cross-border flows
# Only countries with direct physical interconnections
# 
# IMPORTANT: Country codes and data availability notes:
# - Germany: Prefer 'DE' (Germany bidding zone) and use 'DE_LU'
#   (Germany-Luxembourg bidding zone) as a fallback if 'DE' has no data.
#   This script will first try DE and then fall back to DE_LU automatically
#   for the NL↔DE border when ENTSO-E exposes data only under DE_LU.
# - Norway: Uses 'NO_2' (NO2 bidding zone with underscore) - VERIFIED in entsoe-py mappings.py
#   The entsoe-py library's mappings.py file explicitly uses 'NO_2' (with underscore) as the key
#   mapping to EIC code '10YNO-2--------T'. This is the bidding zone code for the NorNed cable.
#   Note: The GUI parser uses 'NO2' (without underscore) in display names, but API requires 'NO_2'.
# - Great Britain: Uses 'GB' - NOTE: GB STOPPED REPORTING TO ENTSO-E AFTER 15 JUNE 2021
#   Due to Brexit, GB no longer provides data via ENTSO-E. Historical data before
#   June 2021 should be available, but recent data will be zero/missing.
# - Belgium: Uses 'BE' - correct (though we also use Elia data for BE which is more accurate)
# - Denmark: Uses 'DK_1' (DK1 zone with underscore) - VERIFIED in entsoe-py mappings.py
#   The entsoe-py library's mappings.py uses 'DK_1' (with underscore) mapping to EIC code
#   '10YDK-1--------W'. This is for the COBRAcable which went live in September 2019.
neighboring_countries = {
    'BE': 'Belgium',
    'DE': 'Germany (DE)',  # Prefer DE and fall back to DE_LU if needed
    'DK_1': 'Denmark (DK1)',  # COBRAcable went live Sept 2019
    'GB': 'Great Britain',  # NOTE: Data discontinued after 15 June 2021 (Brexit)
    'NO_2': 'Norway (NO2)'  # NO_2 with underscore is correct for entsoe-py
}

# Mapping from API country codes to column name country codes (for backward compatibility)
# This allows us to use correct API codes (e.g., DE or DE_LU) while maintaining column names (e.g., DE)
# so that existing data files and downstream code continue to work
country_code_to_column_name = {
    'BE': 'BE',
    'DE': 'DE',      # Primary code
    'DE_LU': 'DE',   # Fallback code still maps to DE for compatibility
    'DK_1': 'DK_1',
    'GB': 'GB',
    'NO_2': 'NO_2'
}

# Country-specific start dates (for cables that went live after 2018)
country_start_dates = {
    'DK_1': pd.Timestamp('20190801', tz='Europe/Amsterdam'),  # COBRAcable live Sept 2019, start from Aug 2019
}

# Country-specific end dates (for countries that stopped reporting)
# GB stopped reporting to ENTSO-E after 15 June 2021 due to Brexit
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

def fetch_crossborder_flows_in_chunks(country_from, country_to, start, end):
    """Fetch cross-border flows for a specific border in chunks"""
    all_data = []
    current_start = start
    original_country_to = country_to
    current_country_to = country_to

    # Fallback mapping for problematic border codes where ENTSO-E may expose the
    # same physical border under a different code.
    # Example: NL -> DE can sometimes be exposed effectively only via NL -> DE_LU.
    fallback_border_codes = {
        # Start with DE and fall back to DE_LU if DE has no matching data.
        ("NL", "DE"): ["DE_LU"],
    }
    fallback_options = fallback_border_codes.get((country_from, country_to), [])
    fallback_index = 0
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
                    country_code_to=current_country_to,
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
                    # Keep the column name based on the *original* logical border
                    # so downstream code still sees, e.g., NL_to_DE_LU even if we
                    # had to query NL->DE_LU_AT or NL->DE under the hood.
                    chunk_data = chunk_data.to_frame(f'{country_from}_to_{original_country_to}')

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
                print(f"  ⚠️  Timeout fetching {country_from}->{current_country_to}: {e}")
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"  ✗ Max retries reached for {country_from}->{current_country_to} after timeouts")
                    current_start = end
                    success = True  # Skip this border
                    break
                else:
                    delay = base_delay * (2 ** (retry_count - 1))
                    print(f"  ⚠️  Retrying {country_from}->{current_country_to} in {delay}s...")
                    time.sleep(delay)

            except NoMatchingDataError:
                # No data available for this border/period.
                # If we have configured fallback ENTSO-E border codes for this logical
                # border, try them in sequence before giving up.
                if fallback_index < len(fallback_options):
                    new_code = fallback_options[fallback_index]
                    fallback_index += 1
                    print(
                        f"  ℹ️  No data available for {country_from}->{current_country_to} "
                        f"(NoMatchingDataError). Trying fallback code {country_from}->{new_code}..."
                    )
                    current_country_to = new_code
                    # Reset retry counter for the new code and retry the same time window
                    retry_count = 0
                    time.sleep(0.5)
                    # Break out of the inner try/except so the while-loop iteration restarts
                    # with the updated current_country_to.
                    break

                # No data for any of the fallbacks either – skip this border gracefully.
                print(
                    f"  ℹ️  No data available for {country_from}->{original_country_to} "
                    f"after trying fallbacks ({', '.join(fallback_options)})"
                )
                # Important: advance current_start to end to avoid an infinite loop
                # over the same time window when the API consistently returns no data.
                current_start = end
                success = True  # Skip this border / remaining period
                break
                
            except Exception as e:
                error_msg = str(e) if str(e) else repr(e)
                error_lower = error_msg.lower()
                status_code = None
                response = getattr(e, "response", None)
                if response is not None:
                    status_code = getattr(response, "status_code", None)

                # If the code itself is invalid, try the next fallback (if any)
                if "invalid country code" in error_lower or "unknown area" in error_lower:
                    if fallback_index < len(fallback_options):
                        new_code = fallback_options[fallback_index]
                        fallback_index += 1
                        print(
                            f"  ℹ️  Invalid country code for {country_from}->{current_country_to}. "
                            f"Trying fallback code {country_from}->{new_code}..."
                        )
                        current_country_to = new_code
                        retry_count = 0
                        time.sleep(0.5)
                        break

                    print(
                        f"  ✗ Invalid country code for {country_from}->{current_country_to} "
                        f"and no fallbacks left. Skipping {country_from}->{original_country_to}."
                    )
                    current_start = end
                    success = True
                    break

                # Only retry on specific error codes (503 = Service Unavailable, timeouts)
                if status_code not in (503,):
                    print(f"  ✗ Non-retryable error fetching {country_from}->{current_country_to}: {error_msg}")
                    current_start = end
                    success = True  # Skip this border
                    break

                retry_count += 1
                if retry_count >= max_retries:
                    print(f"  ✗ Max retries reached for {country_from}->{current_country_to}: {error_msg}")
                    current_start = end
                    success = True  # Skip this border
                    break
                else:
                    delay = base_delay * (2 ** (retry_count - 1))
                    print(f"  ⚠️  Retrying {country_from}->{current_country_to} in {delay}s... (Error: {error_msg})")
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
                print(f"      ⚠️  WARNING: All data for {country_from}->{country_to} is ZERO!")
                print(f"         This may indicate:")
                print(f"         - Incorrect country code (e.g., DE vs DE_LU)")
                print(f"         - No data available for this border/period")
                print(f"         - API returned empty/zero values")
            elif non_zero_count == 0:
                print(f"      ⚠️  WARNING: All {total_values} values are zero for {country_from}->{country_to}")
            elif non_zero_count < total_values * 0.1:  # Less than 10% non-zero
                zero_percentage = (1 - non_zero_count / total_values) * 100
                print(f"      ⚠️  WARNING: {zero_percentage:.1f}% of values are zero ({non_zero_count}/{total_values} non-zero)")
                print(f"         This may indicate data quality issues")

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


def _fill_quarters_from_hour_start_if_sparse(series: pd.Series, *, label: str) -> pd.Series:
    """
    Some ENTSO-E borders can occasionally return 15-minute series where only the
    first quarter (:00) has the hour's value and the remaining quarters are 0.
    If detected, copy the :00 value into :15/:30/:45 within the same hour.
    """
    if series is None or len(series) == 0:
        return series

    # Only applies to 15-minute indexed data
    idx = series.index
    if not isinstance(idx, pd.DatetimeIndex):
        return series

    # Fast exit if we don't even have quarter-hour stamps
    if not (idx.minute.isin([0, 15, 30, 45]).all() and len(series) >= 8):
        return series

    # Heuristic detection:
    # If a large share of non-zero values occur at :00 while :15/:30/:45 are mostly zero,
    # treat zeros in those quarters as "missing" and fill within the hour.
    minutes = idx.minute
    nonzero = series.ne(0) & series.notna()
    nz_00 = int(nonzero[minutes == 0].sum())
    nz_other = int(nonzero[minutes.isin([15, 30, 45])].sum())
    if nz_00 == 0:
        return series

    # If other quarters almost never have non-zero values compared to :00,
    # assume sparse-quarter encoding.
    if nz_other > max(3, int(nz_00 * 0.10)):
        return series

    print(f"  ℹ️  Detected sparse 15-min quarters for {label}. Filling :15/:30/:45 from :00 within each hour...")

    hour = idx.floor("h")
    s = series.copy()
    for h, g in s.groupby(hour):
        # Expect up to 4 samples per hour; if :00 isn't present, skip
        g = g.sort_index()
        if len(g) < 2:
            continue
        if g.index[0].minute != 0:
            continue
        base = g.iloc[0]
        if pd.isna(base) or base == 0:
            continue
        # Replace zeros in remaining quarters with base
        mask = (g.index.minute != 0) & (g == 0)
        if mask.any():
            s.loc[g.index[mask]] = base
    return s

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
        # Skip Belgium - data comes from Elia CSV file, not ENTSO-E
        if country_to == 'BE':
            print(f"\n  ℹ️  {country_name}: Skipping ENTSO-E fetch (data comes from Elia CSV file)")
            border_count += 2  # Skip both directions
            continue
            
        # Use country-specific start date if available
        country_start = country_start_dates.get(country_to, start)
        # Use country-specific end date if available (e.g., GB stopped reporting after Brexit)
        country_end = country_end_dates.get(country_to, end)
        # Use the earlier of country_end or global end
        effective_end = min(country_end, end)

        if country_start != start:
            print(f"\n  ℹ️  {country_name}: Using custom start date {country_start.strftime('%Y-%m-%d')} (cable went live later)")
        
        if country_end < end:
            print(f"\n  ⚠️  {country_name}: Data only available until {country_end.strftime('%Y-%m-%d')} (stopped reporting to ENTSO-E)")

        # Skip if start is after end
        if country_start >= effective_end:
            print(f"\n  ⚠️  {country_name}: Skipping (start date {country_start.strftime('%Y-%m-%d')} is after end date {effective_end.strftime('%Y-%m-%d')})")
            border_count += 2  # Skip both directions
            continue

        border_count += 1
        print(f"\n  [{border_count}/{total_borders}] Fetching NL -> {country_name}...")
        flow_export = fetch_crossborder_flows_in_chunks('NL', country_to, country_start, effective_end)
        if flow_export is not None:
            # Check if export flow contains only zeros
            numeric_cols = flow_export.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                export_sum = flow_export[numeric_cols].abs().sum().sum()
                export_non_zero = (flow_export[numeric_cols].abs() > 0).sum().sum()
                export_total = len(flow_export) * len(numeric_cols)
                
                if export_sum == 0:
                    print(f"    ⚠️  WARNING: Export flow (NL->{country_name}) contains ONLY ZEROS!")
                    print(f"       This likely indicates a problem with country code '{country_to}' or no data available")
                elif export_non_zero < export_total * 0.1:
                    zero_pct = (1 - export_non_zero / export_total) * 100
                    print(f"    ⚠️  WARNING: {zero_pct:.1f}% of export values are zero ({export_non_zero}/{export_total} non-zero)")
            
            # Use column name mapping for backward compatibility (e.g., DE_LU -> DE)
            column_country_code = country_code_to_column_name.get(country_to, country_to)
            all_flows[f'exp_{column_country_code}'] = flow_export
            export_rows = len(flow_export)
            print(f"    ✓ Received {export_rows} rows")
            if country_to not in country_row_counts:
                country_row_counts[country_to] = []
            country_row_counts[country_to].append(export_rows)

        # Small delay between directions
        time.sleep(0.1)

        border_count += 1
        print(f"  [{border_count}/{total_borders}] Fetching {country_name} -> NL...")
        flow_import = fetch_crossborder_flows_in_chunks(country_to, 'NL', country_start, effective_end)
        if flow_import is not None:
            # Check if import flow contains only zeros
            numeric_cols = flow_import.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                import_sum = flow_import[numeric_cols].abs().sum().sum()
                import_non_zero = (flow_import[numeric_cols].abs() > 0).sum().sum()
                import_total = len(flow_import) * len(numeric_cols)
                
                if import_sum == 0:
                    print(f"    ⚠️  WARNING: Import flow ({country_name}->NL) contains ONLY ZEROS!")
                    print(f"       This likely indicates a problem with country code '{country_to}' or no data available")
                elif import_non_zero < import_total * 0.1:
                    zero_pct = (1 - import_non_zero / import_total) * 100
                    print(f"    ⚠️  WARNING: {zero_pct:.1f}% of import values are zero ({import_non_zero}/{import_total} non-zero)")
            
            # Use column name mapping for backward compatibility (e.g., DE_LU -> DE)
            column_country_code = country_code_to_column_name.get(country_to, country_to)
            all_flows[f'imp_{column_country_code}'] = flow_import
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

# Check for zero-only flows and report summary
if all_flows:
    print("Checking for zero-only data...")
    zero_flows = []
    low_data_flows = []
    country_imp_exp_status = {}
    
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

            # Track, per country, whether import/export series contain any non-zero values
            if flow_name.startswith(('imp_', 'exp_')):
                direction, country_code = flow_name.split('_', 1)
                if country_code not in country_imp_exp_status:
                    country_imp_exp_status[country_code] = {
                        'imp_has_non_zero': None,
                        'exp_has_non_zero': None,
                    }
                has_non_zero = non_zero_count > 0
                if direction == 'imp':
                    country_imp_exp_status[country_code]['imp_has_non_zero'] = has_non_zero
                elif direction == 'exp':
                    country_imp_exp_status[country_code]['exp_has_non_zero'] = has_non_zero
    
    if zero_flows:
        print(f"\n  ⚠️  CRITICAL: {len(zero_flows)} flow(s) contain ONLY ZEROS:")
        for flow in zero_flows:
            print(f"     - {flow}")
        print(f"     → This likely indicates incorrect country codes or no data available")
        print(f"     → Check the country code mappings in the script")
    
    if low_data_flows:
        print(f"\n  ⚠️  WARNING: {len(low_data_flows)} flow(s) have very low data quality (<10% non-zero):")
        for flow_name, zero_pct, non_zero, total in low_data_flows:
            print(f"     - {flow_name}: {zero_pct:.1f}% zeros ({non_zero}/{total} non-zero)")
    
    # Per-country summary: are imp/exp series only zeros, or do they contain non-zero values?
    if country_imp_exp_status:
        print("\n  ℹ️  Import/export data status by country:")
        for country_code in sorted(country_imp_exp_status.keys()):
            status = country_imp_exp_status[country_code]

            def _format_direction(label, has_non_zero):
                if has_non_zero is True:
                    return f"{label}: contains non-zero values"
                elif has_non_zero is False:
                    return f"{label}: ONLY ZEROS"
                else:
                    return f"{label}: no data retrieved"

            imp_msg = _format_direction("imp", status.get('imp_has_non_zero'))
            exp_msg = _format_direction("exp", status.get('exp_has_non_zero'))
            print(f"     - {country_code}: {imp_msg} | {exp_msg}")

    if not zero_flows and not low_data_flows:
        print("  ✓ All flows contain meaningful data (no zero-only flows detected)")
    
    print()

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

    # ============================================================================
    # OPTIONAL FIX: some borders encode hourly values only at :00 (rest quarters=0)
    # ============================================================================
    quarter_fill_countries = ["NO_2"]
    quarter_fill_cols = [c for c in combined_df.columns if any(c.endswith(cc) for cc in quarter_fill_countries)]
    for col in quarter_fill_cols:
        combined_df[col] = _fill_quarters_from_hour_start_if_sparse(combined_df[col], label=col)

    # Fill any remaining NaN values with 0 (no flow)
    combined_df = combined_df.fillna(0)

    # Separate import and export columns (after dropping unwanted countries)
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
            
            # Align Belgium data with combined_df index using an INNER overlap
            # Only timestamps present in BOTH series are used (inner merge behaviour)
            overlap_index = combined_df.index.intersection(belgium_import.index)
            
            if not overlap_index.empty:
                print(f"\n  Overwriting Belgium columns in combined_df using INNER overlap...")
                print(f"    Overlap range: {overlap_index.min().strftime('%Y-%m-%d %H:%M')} to {overlap_index.max().strftime('%Y-%m-%d %H:%M')}")
                
                # Ensure columns exist
                if 'imp_BE' not in combined_df.columns:
                    combined_df['imp_BE'] = 0
                if 'exp_BE' not in combined_df.columns:
                    combined_df['exp_BE'] = 0
                
                # Overwrite Belgium columns only on shared timestamps (no ffill/bfill)
                combined_df.loc[overlap_index, 'imp_BE'] = belgium_import.loc[overlap_index].astype(int)
                combined_df.loc[overlap_index, 'exp_BE'] = belgium_export.loc[overlap_index].astype(int)
                
                overwritten_count = len(overlap_index)
                print(f"  ✓ Overwritten {overwritten_count} rows for Belgium import/export (inner merge)")
                
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

    # NOTE: netCrossBorderExchangeNL CSV exports (extended/hourly) are no longer saved
    # to avoid creating the *_MW_positive_import_extended.csv and *_MW_positive_import_hourly.csv files.
    crossborder_data_to_save = crossborder_data.copy()
    crossborder_data_to_save.index.name = 'datetime'
    crossborder_data_to_save = crossborder_data_to_save.reset_index()

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

    # Print sample of data
    print("\n" + "="*60)
    print("DATA SAMPLE - Net Exchange EXTENDED (15-minute, first 7 rows)")
    print("="*60)
    print(crossborder_data_to_save.head(7))

    print("\n" + "="*60)
    print("DATA SAMPLE - Detailed EXTENDED (15-minute, first 7 rows)")
    print("="*60)
    print(detailed_data_to_save.head(7))

    print("\n" + "="*60)
    print("FILES SAVED SUMMARY")
    print("="*60)
    print(f"✓ Net Exchange Extended (in-memory only)")
    print(f"✓ Detailed Extended:     {detailed_filename_extended}")

    print("\n" + "="*60)
    print("SCRIPT COMPLETED SUCCESSFULLY")
    print("="*60)

else:
    print("\n❌ No cross-border flow data was retrieved.")
