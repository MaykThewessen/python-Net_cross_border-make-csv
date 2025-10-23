#!/usr/bin/env python3
"""
Import script for GUI_NET_CROSS_BORDER_PHYSICAL_FLOWS Excel files
This script reads all Excel files in the current directory and processes them.
"""

import pandas as pd
import glob
from pathlib import Path
import warnings
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from datetime import datetime
os.system('clear')  # for Mac/Linux

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# Set plotting style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def import_excel_files():
    """
    Import all Excel files in the current directory
    Returns a dictionary with file names as keys and DataFrames as values
    """

    # Get all Excel files in the current directory
    excel_files = glob.glob('*.xlsx')

    # Sort excel_files by filename
    excel_files.sort()
    
    if not excel_files:
        print("No Excel files found in the current directory.")
        return {}
    
    print(f"Found {len(excel_files)} Excel files")
    
    # Dictionary to store all imported data
    imported_data = {}
    
    # Import each Excel file
    for file_path in excel_files:
        file_name = os.path.basename(file_path)
        print(f"Importing {file_name}...", end=" ")
        
        try:
            # Read the Excel file, skipping the first 5 rows
            df = pd.read_excel(file_path, skiprows=5)
            
            # For all columns with float dtype, round to 1 decimal
            float_cols = df.select_dtypes(include=['float']).columns
            df[float_cols] = df[float_cols].round(1)
            

            # Store in dictionary
            imported_data[file_name] = df
            
            print(f"✓ ({df.shape[0]} rows)")
            
        except Exception as e:
            print(f"✗ Error: {str(e)}")
            continue
    
    return imported_data

def rename_and_add_columns(df):
    """
    Rename columns to shorter names and add import/export summary columns
    """
    df_copy = df.copy()
    
    # Column mapping for shorter names
    column_mapping = {
        'MTU': 'datetime',
        'BZN|NL -> BZN|BE': 'expBE',
        'BZN|BE -> BZN|NL': 'impBE',
        'BZN|NL -> BZN|DE-AT-LU': 'expDE',
        'BZN|DE-AT-LU -> BZN|NL': 'impDE',
        'BZN|NL -> BZN|DE-LU': 'expDELU',
        'BZN|DE-LU -> BZN|NL': 'impDELU',
        'BZN|NL -> BZN|DK1': 'expDK',
        'BZN|DK1 -> BZN|NL': 'impDK',
        'BZN|NL -> BZN|GB': 'expGB',
        'BZN|GB -> BZN|NL': 'impGB',
        'BZN|NL -> BZN|NO2': 'expNO',
        'BZN|NO2 -> BZN|NL': 'impNO'
    }    

    # Rename columns
    df_copy = df_copy.rename(columns=column_mapping)

    # Sort df_copy on column names
    # df_copy = df_copy.reindex(sorted(df_copy.columns), axis=1)
    
    # Convert MTU to datetime (extract start time from time range)
    if 'datetime' in df_copy.columns:
        # Extract the start time from the time range format: "01/01/2018 00:00:00 - 01/01/2018 01:00:00"
        df_copy['datetime'] = df_copy['datetime'].str.split(' - ').str[0]
        # Remove timezone information like (CET) or (CEST) for cleaner parsing
        df_copy['datetime'] = df_copy['datetime'].str.replace(r'\s*\([^)]+\)', '', regex=True)
        # Parse datetime with DD/MM/YYYY format to avoid confusion with MM/DD/YYYY
        df_copy['datetime'] = pd.to_datetime(df_copy['datetime'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
        # Set timezone to CET (Amsterdam timezone), handling ambiguous DST transitions
        df_copy['datetime'] = df_copy['datetime'].dt.tz_localize('CET', ambiguous='infer')
        
        # Create UTC datetime column as first column
        df_copy['date_UTC'] = df_copy['datetime'].dt.tz_convert('UTC')
        
        # Reorder columns to put date_UTC first
        cols = df_copy.columns.tolist()
        cols.remove('date_UTC')
        cols.insert(0, 'date_UTC')
        df_copy = df_copy[cols]
    
    # Get all columns except datetime columns for processing
    flow_cols = [col for col in df_copy.columns if col not in ['datetime', 'date_UTC']]
    
    # Convert 'n/e' and '-' strings to NaN for proper numeric operations
    for col in flow_cols:
        if col in df_copy.columns:
            df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')
    
    # Now get numeric columns (excluding datetime columns)
    numeric_cols = df_copy.select_dtypes(include=['number']).columns.tolist()
    numeric_cols = [col for col in numeric_cols if col not in ['datetime', 'date_UTC']]
    
    # Get import columns (TO Netherlands) - even indexed
    import_cols = [numeric_cols[i] for i in range(1, len(numeric_cols), 2)]
    
    # Get export columns (FROM Netherlands) - odd indexed  
    export_cols = [numeric_cols[i] for i in range(0, len(numeric_cols), 2)]
    
    # Print column info only for first file to avoid repetition
    if 'import_cols_info_printed' not in globals():
        print(f"  Import columns: {import_cols}")
        print(f"  Export columns: {export_cols}")
        globals()['import_cols_info_printed'] = True
    
    
    # Sum the import columns (TO Netherlands)
    df_copy['import_NL'] = df_copy[import_cols].sum(axis=1, skipna=True)
    
    # Sum the export columns (FROM Netherlands)
    df_copy['export_NL'] = df_copy[export_cols].sum(axis=1, skipna=True)
    
    # Create a new column 'netCroBoNL' net cross border exchange NL: as the difference between import_NL and export_NL
    df_copy['netCroBoNL'] = df_copy['import_NL'] - df_copy['export_NL']
    
    print('df_copy')
    print(df_copy)
    print(df_copy.describe().round(1))
    
    # Sort df_copy on column names
    return df_copy

def consolidate_dataframe(imported_data):
    """
    Consolidate all imported Excel files into a single DataFrame
    """
    if not imported_data:
        print("No data to consolidate.")
        return pd.DataFrame()
    
    print("\n" + "="*60)
    print("CONSOLIDATING DATA INTO SINGLE DATAFRAME")
    print("="*60)
    
    # List to store all DataFrames
    dataframes = []
    
    print("Processing files...", end=" ")
    
    for file_name, df in imported_data.items():
        # Create a copy of the dataframe
        df_copy = df.copy()
        
        # Rename columns and add import/export summary columns
        df_copy = rename_and_add_columns(df_copy)
        
        dataframes.append(df_copy)
    
    print("✓")
    
    # Concatenate all DataFrames
    print("Concatenating DataFrames...", end=" ")
    consolidated_df = pd.concat(dataframes, ignore_index=True, sort=False)
    print(f"✓ ({consolidated_df.shape[0]} rows, {consolidated_df.shape[1]} columns)")
    
    return consolidated_df

def analyze_data(imported_data, consolidated_df=None, verbose=False):
    """
    Analyze the imported data and provide summary statistics
    """
    if not imported_data:
        print("No data to analyze.")
        return
    
    if not verbose:
        # Quick summary only
        print(f"\nSummary: {len(imported_data)} files imported")
        if consolidated_df is not None and not consolidated_df.empty:
            print(f"Consolidated DataFrame: {consolidated_df.shape[0]} rows, {consolidated_df.shape[1]} columns")
            print(f"Columns: {list(consolidated_df.columns)}")
        return
    
    print("\n" + "="*60)
    print("DATA ANALYSIS SUMMARY")
    print("="*60)
    
    # Individual file analysis
    for file_name, df in imported_data.items():
        print(f"\nFile: {file_name}")
        print(f"Shape: {df.shape}")
        print(f"Data types:")
        print(df.dtypes)
        
        # Check for missing values
        missing_values = df.isnull().sum()
        if missing_values.sum() > 0:
            print(f"Missing values:")
            print(missing_values[missing_values > 0])
        else:
            print("No missing values found.")
        
        # Basic statistics for numeric columns
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            print(f"Numeric columns summary:")
            print(df[numeric_cols].describe())
    
    # Consolidated DataFrame analysis
    if consolidated_df is not None and not consolidated_df.empty:
        print(f"\n" + "="*60)
        print("CONSOLIDATED DATAFRAME ANALYSIS")
        print("="*60)
        print(f"Total rows: {consolidated_df.shape[0]}")
        print(f"Total columns: {consolidated_df.shape[1]}")
        
        # Check for missing values in consolidated data
        missing_values = consolidated_df.isnull().sum()
        if missing_values.sum() > 0:
            print(f"Missing values in consolidated data:")
            print(missing_values[missing_values > 0])
        else:
            print("No missing values in consolidated data.")
        
        
        # Basic statistics for numeric columns
        numeric_cols = consolidated_df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            print(f"\nNumeric columns summary:")
            print(consolidated_df[numeric_cols].describe())

def validate_output_data(df_output):
    """
    Validate the output DataFrame for correct row count and consecutive datetime values
    Returns a dictionary with validation results
    """
    validation_results = {
        'row_count_valid': False,
        'consecutive_datetime_valid': False,
        'expected_rows': 0,
        'actual_rows': 0,
        'datetime_gaps': [],
        'validation_passed': False
    }
    
    if df_output.empty:
        print("❌ Validation failed: DataFrame is empty")
        return validation_results
    
    # Get date range
    min_date = df_output['datetime'].min()
    max_date = df_output['datetime'].max()
    
    # Calculate expected number of rows (hourly data)
    # Add 1 hour to include the last hour
    time_diff = max_date - min_date
    expected_hours = int(time_diff.total_seconds() / 3600) + 1
    validation_results['expected_rows'] = expected_hours
    validation_results['actual_rows'] = len(df_output)
    
    print(f"\n" + "="*60)
    print("VALIDATING OUTPUT DATA")
    print("="*60)
    print(f"Date range: {min_date.strftime('%Y-%m-%d %H:%M:%S')} to {max_date.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Expected rows (hourly data): {expected_hours}")
    print(f"Actual rows: {len(df_output)}")
    
    # Check 1: Row count validation
    if len(df_output) == expected_hours:
        print("✅ Row count validation: PASSED")
        validation_results['row_count_valid'] = True
    else:
        print(f"❌ Row count validation: FAILED (Expected: {expected_hours}, Actual: {len(df_output)})")
    
    # Check 2: Consecutive datetime validation
    df_sorted = df_output.sort_values('datetime').reset_index(drop=True)
    
    # Create expected datetime range
    expected_datetimes = pd.date_range(
        start=min_date, 
        end=max_date, 
        freq='H',
        tz=min_date.tz
    )
    
    # Check for gaps in datetime
    datetime_gaps = []
    for i in range(len(df_sorted) - 1):
        current_time = df_sorted.iloc[i]['datetime']
        next_time = df_sorted.iloc[i + 1]['datetime']
        expected_next = current_time + pd.Timedelta(hours=1)
        
        if next_time != expected_next:
            datetime_gaps.append({
                'index': i,
                'current_time': current_time,
                'expected_next': expected_next,
                'actual_next': next_time,
                'gap_hours': (next_time - current_time).total_seconds() / 3600
            })
    
    validation_results['datetime_gaps'] = datetime_gaps
    
    if not datetime_gaps:
        print("✅ Consecutive datetime validation: PASSED")
        validation_results['consecutive_datetime_valid'] = True
    else:
        print(f"❌ Consecutive datetime validation: FAILED ({len(datetime_gaps)} gaps found)")
        print("Gaps found:")
        for gap in datetime_gaps[:5]:  # Show first 5 gaps
            print(f"  - Index {gap['index']}: {gap['current_time']} -> Expected: {gap['expected_next']}, Actual: {gap['actual_next']} (Gap: {gap['gap_hours']:.1f} hours)")
        if len(datetime_gaps) > 5:
            print(f"  ... and {len(datetime_gaps) - 5} more gaps")
    
    # Overall validation result
    validation_results['validation_passed'] = validation_results['row_count_valid'] and validation_results['consecutive_datetime_valid']
    
    if validation_results['validation_passed']:
        print("🎉 Overall validation: PASSED - Data is complete and consecutive")
    else:
        print("⚠️  Overall validation: FAILED - Data has issues")
    
    print("="*60)
    
    return validation_results

def create_yearly_statistics_csv(consolidated_df):
    """
    Create yearly statistics per country showing imports, exports, and net exchange in TWh/y
    """
    if consolidated_df.empty:
        print("❌ Cannot create statistics: DataFrame is empty")
        return None
    
    print(f"\n" + "="*60)
    print("CREATING YEARLY STATISTICS PER COUNTRY")
    print("="*60)
    
    # Create a copy for processing
    df_stats = consolidated_df.copy()
    
    # Extract year from datetime
    df_stats['year'] = df_stats['datetime'].dt.year
    
    # Define country mappings
    countries = {
        'BE': {'import': 'impBE', 'export': 'expBE'},
        'DE': {'import': 'impDE', 'export': 'expDE'},
        'DELU': {'import': 'impDELU', 'export': 'expDELU'},
        'DK': {'import': 'impDK', 'export': 'expDK'},
        'GB': {'import': 'impGB', 'export': 'expGB'},
        'NO': {'import': 'impNO', 'export': 'expNO'}
    }
    
    # Initialize results list
    yearly_stats = []
    
    # Get unique years
    years = sorted(df_stats['year'].unique())
    
    for year in years:
        year_data = df_stats[df_stats['year'] == year]
        
        # Calculate hours in year (accounting for leap years)
        hours_in_year = len(year_data)
        
        # Create row for this year
        year_row = {'Year': year, 'Hours_in_Year': hours_in_year}
        
        # Calculate statistics for each country
        for country_code, columns in countries.items():
            import_col = columns['import']
            export_col = columns['export']
            
            # Calculate totals in MWh (sum of hourly values)
            total_import_mwh = year_data[import_col].sum() if import_col in year_data.columns else 0
            total_export_mwh = year_data[export_col].sum() if export_col in year_data.columns else 0
            
            # Convert to TWh/y
            total_import_twh = total_import_mwh / 1_000_000  # MWh to TWh
            total_export_twh = total_export_mwh / 1_000_000  # MWh to TWh
            
            # Add to row
            year_row[f'{country_code}_Import_TWh_y'] = round(total_import_twh, 3)
            year_row[f'{country_code}_Export_TWh_y'] = round(total_export_twh, 3)
        
        # Calculate total NL imports, exports, and net exchange (sum of all countries)
        total_import_twh = 0
        total_export_twh = 0
        
        for country_code, columns in countries.items():
            import_col = columns['import']
            export_col = columns['export']
            
            # Calculate totals in MWh (sum of hourly values)
            country_import_mwh = year_data[import_col].sum() if import_col in year_data.columns else 0
            country_export_mwh = year_data[export_col].sum() if export_col in year_data.columns else 0
            
            # Convert to TWh/y and add to totals
            country_import_twh = country_import_mwh / 1_000_000  # MWh to TWh
            country_export_twh = country_export_mwh / 1_000_000  # MWh to TWh
            
            total_import_twh += country_import_twh
            total_export_twh += country_export_twh
        
        # Calculate total net exchange
        total_net_exchange_twh = total_import_twh - total_export_twh
        
        # Add total NL statistics to row
        year_row['Total_NL_Import_TWh_y'] = round(total_import_twh, 3)
        year_row['Total_NL_Export_TWh_y'] = round(total_export_twh, 3)
        year_row['Total_NL_NetExchange_TWh_y'] = round(total_net_exchange_twh, 3)
        
        yearly_stats.append(year_row)
    
    # Create DataFrame from results
    stats_df = pd.DataFrame(yearly_stats)
    
    # Display summary
    print(f"✓ Statistics calculated for {len(years)} years: {min(years)}-{max(years)}")
    print(f"✓ Countries included: {', '.join(countries.keys())}")
    print(f"✓ Statistics DataFrame shape: {stats_df.shape}")
    
    # Show preview
    print(f"\nPreview of yearly statistics:")
    print(stats_df.head())
    
    return stats_df

def create_net_cross_border_plot(df_output, output_filename):
    """
    Create a comprehensive plot of the net cross-border exchange data
    and save it as a PDF
    """
    if df_output.empty:
        print("❌ Cannot create plot: DataFrame is empty")
        return
    
    print(f"\n" + "="*60)
    print("CREATING NET CROSS-BORDER EXCHANGE PLOT")
    print("="*60)
    
    # Convert datetime to matplotlib-compatible format
    df_plot = df_output.copy()
    df_plot['datetime_plot'] = df_plot['datetime'].dt.tz_localize(None)  # Remove timezone for plotting
    
    # Get year range from data for dynamic title
    min_year = df_plot['datetime_plot'].min().year
    max_year = df_plot['datetime_plot'].max().year
    year_range = f"{min_year}-{max_year}" if min_year != max_year else str(min_year)
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle(f'Net Cross-Border Exchange Netherlands ({year_range})', fontsize=16, fontweight='bold')
    
    # Plot 1: Time series of net cross-border exchange
    ax1 = axes[0, 0]
    ax1.plot(df_plot['datetime_plot'], df_plot['netCroBoNL'], linewidth=0.8, alpha=0.7, color='steelblue')
    ax1.axhline(y=0, color='red', linestyle='--', alpha=0.5)
    ax1.set_title('Net Cross-Border Exchange Over Time', fontweight='bold')
    ax1.set_ylabel('Net Exchange (MW)')
    ax1.set_xlabel('Date')
    ax1.grid(True, alpha=0.3)
    
    # Format x-axis to show years
    ax1.xaxis.set_major_locator(mdates.YearLocator())
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax1.xaxis.set_minor_locator(mdates.MonthLocator([1, 7]))
    
    # Plot 2: Distribution histogram
    ax2 = axes[0, 1]
    ax2.hist(df_plot['netCroBoNL'], bins=50, alpha=0.7, color='lightcoral', edgecolor='black')
    ax2.axvline(x=0, color='red', linestyle='--', alpha=0.7, linewidth=2)
    ax2.set_title('Distribution of Net Exchange Values', fontweight='bold')
    ax2.set_xlabel('Net Exchange (MW)')
    ax2.set_ylabel('Frequency')
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: Monthly averages
    df_monthly = df_plot.copy()
    df_monthly['year_month'] = df_monthly['datetime_plot'].dt.to_period('M')
    monthly_avg = df_monthly.groupby('year_month')['netCroBoNL'].mean()
    
    ax3 = axes[1, 0]
    monthly_avg.plot(kind='bar', ax=ax3, color='lightgreen', alpha=0.7)
    ax3.axhline(y=0, color='red', linestyle='--', alpha=0.5)
    ax3.set_title('Monthly Average Net Exchange', fontweight='bold')
    ax3.set_ylabel('Average Net Exchange (MW)')
    ax3.set_xlabel('Year-Month')
    ax3.tick_params(axis='x', rotation=45)
    ax3.grid(True, alpha=0.3)
    
    # Plot 4: Yearly summary
    df_yearly = df_plot.copy()
    df_yearly['year'] = df_yearly['datetime_plot'].dt.year
    yearly_stats = df_yearly.groupby('year')['netCroBoNL'].agg(['mean', 'std', 'min', 'max'])
    
    ax4 = axes[1, 1]
    x_pos = range(len(yearly_stats.index))
    ax4.bar(x_pos, yearly_stats['mean'], alpha=0.7, color='gold', 
            yerr=yearly_stats['std'], capsize=5, error_kw={'alpha': 0.7})
    ax4.axhline(y=0, color='red', linestyle='--', alpha=0.5)
    ax4.set_title('Yearly Average Net Exchange', fontweight='bold')
    ax4.set_ylabel('Average Net Exchange (MW)')
    ax4.set_xlabel('Year')
    ax4.set_xticks(x_pos)
    ax4.set_xticklabels(yearly_stats.index)
    ax4.grid(True, alpha=0.3)
    
    # Add statistics text box
    stats_text = f"""Statistics Summary:
Total Hours: {len(df_plot):,}
Mean: {df_plot['netCroBoNL'].mean():.1f} MW
Std Dev: {df_plot['netCroBoNL'].std():.1f} MW
Min: {df_plot['netCroBoNL'].min():.1f} MW
Max: {df_plot['netCroBoNL'].max():.1f} MW
Net Import Hours: {(df_plot['netCroBoNL'] > 0).sum():,}
Net Export Hours: {(df_plot['netCroBoNL'] < 0).sum():,}"""
    
    fig.text(0.02, 0.02, stats_text, fontsize=10, verticalalignment='bottom',
             bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.8))
    
    # Adjust layout to prevent overlap
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.15)
    
    # Save as PDF
    plot_filename = output_filename.replace('.csv', '_plot.pdf')
    plt.savefig(plot_filename, format='pdf', dpi=300, bbox_inches='tight')
    print(f"✓ Plot saved as '{plot_filename}'")
    
    # Show plot (optional - comment out if running headless)
    # plt.show()
    
    # Close the figure to free memory
    plt.close()
    
    return plot_filename

def main():
    """
    Main function to run the import process
    """
    print("Starting Excel file import process...")
    print("="*60)
    
    # Import all Excel files
    imported_data = import_excel_files()
    
    if imported_data:
        # Consolidate all data into a single DataFrame
        consolidated_df = consolidate_dataframe(imported_data)
        
        # Analyze the data (non-verbose by default)
        analyze_data(imported_data, consolidated_df, verbose=False)
        
        # Create df_output with only datetime and netCroBoNL columns
        df_output = consolidated_df[['datetime', 'netCroBoNL']].copy()
        
        # Convert netCroBoNL to integer
        df_output['netCroBoNL'] = df_output['netCroBoNL'].astype(int)
        
        print(f"\n" + "="*60)
        print(f"CREATING OUTPUT DATAFRAME")
        print("="*60)
        print(f"df_output created with {df_output.shape[0]} rows and {df_output.shape[1]} columns")
        print(f"Columns: {list(df_output.columns)}")
        print(f"\nFirst 5 rows of df_output:")
        print(df_output.head())
        print(f"\nLast 5 rows of df_output:")
        print(df_output.tail())
        
        # Extract date range from the data for dynamic filename
        min_date = df_output['datetime'].min().strftime('%Y%m%d')
        max_date = df_output['datetime'].max().strftime('%Y%m%d')
        
        # Validate the output data
        validation_results = validate_output_data(df_output)
        
        # Save df_output to CSV with descriptive filename using actual date range
        output_filename = f"netCrossBorderExchangeNL_{min_date}_{max_date}.csv"
        df_output.to_csv(output_filename, index=False)
        print(f"\n✓ df_output saved as '{output_filename}'")
        
        # Create and save plot
        plot_filename = create_net_cross_border_plot(df_output, output_filename)
        
        # Create yearly statistics per country
        yearly_stats_df = create_yearly_statistics_csv(consolidated_df)
        
        # Save yearly statistics to CSV
        if yearly_stats_df is not None and not yearly_stats_df.empty:
            stats_filename = f"yearly_statistics_per_country_{min_date}_{max_date}.csv"
            yearly_stats_df.to_csv(stats_filename, index=False)
            print(f"\n✓ Yearly statistics saved as '{stats_filename}'")
        
        print(f"\n" + "="*60)
        print(f"Import completed successfully!")
        print(f"Total files imported: {len(imported_data)}")
        print(f"Total rows in consolidated DataFrame: {consolidated_df.shape[0]}")
        print(f"Total rows in df_output: {df_output.shape[0]}")
        print(f"df_output saved as: {output_filename}")
        if plot_filename:
            print(f"Plot saved as: {plot_filename}")
        if yearly_stats_df is not None and not yearly_stats_df.empty:
            print(f"Yearly statistics saved as: {stats_filename}")
        
        # Add validation summary to final output
        if validation_results['validation_passed']:
            print("✅ Data validation: PASSED - Output file is complete and consecutive")
        else:
            print("⚠️  Data validation: FAILED - Output file has issues")
            print(f"   - Row count: {'✅' if validation_results['row_count_valid'] else '❌'}")
            print(f"   - Consecutive datetime: {'✅' if validation_results['consecutive_datetime_valid'] else '❌'}")
        
        print("="*60)
        
        # Return both individual data, consolidated DataFrame, output DataFrame, and validation results
        return {
            'individual_data': imported_data,
            'consolidated_dataframe': consolidated_df,
            'df_output': df_output,
            'validation_results': validation_results
        }
    else:
        print("No files were successfully imported.")
        return {
            'individual_data': {}, 
            'consolidated_dataframe': pd.DataFrame(), 
            'df_output': pd.DataFrame(),
            'validation_results': {'validation_passed': False}
        }

if __name__ == "__main__":
    # Run the import process
    data = main()



