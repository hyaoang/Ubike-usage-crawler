# -*- coding: utf-8 -*-
import duckdb
import pandas as pd
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib
import glob
import numpy as np
from matplotlib import font_manager
import sys
import traceback # Import traceback

# --- Load custom font ---
try:
    # Get script directory correctly, handle potential NameError in interactive environments
    script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Fallback for interactive environments like Jupyter
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
except Exception:
    # Final fallback
    script_dir = os.getcwd()


FONT_PATH = os.path.join(script_dir, "NotoSansTC-Regular.ttf")
custom_font = None
try:
    if os.path.exists(FONT_PATH):
        custom_font = font_manager.FontProperties(fname=FONT_PATH)
        # Add the font to Matplotlib's font cache
        font_manager.fontManager.addfont(FONT_PATH)
        # Set default font (optional, but ensures it's used widely)
        matplotlib.rcParams['font.family'] = custom_font.get_name()
    else:
        # Fallback to default if custom font not found
        custom_font = font_manager.FontProperties()
        print(f"WARNING (PlotGraph): Custom font not found at {FONT_PATH}. Using default font.", file=sys.stderr)
except Exception as e:
    custom_font = font_manager.FontProperties()
    print(f"WARNING (PlotGraph): Error loading custom font: {e}. Using default font.", file=sys.stderr)


matplotlib.rcParams['axes.unicode_minus'] = False # Keep minus signs as is

# --- Define safe integer conversion function ---
def safe_int_conversion(value):
    if value is None or value == '':
        return 0
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0

# --- Settings (Plotting) ---
DB_DIR = "."
DB_FILE_PATTERN = os.path.join(DB_DIR, "youbike_data_simplified*.duckdb")
TABLE_NAME = "bike_readings_simplified"
OUTPUT_IMAGE_FILENAME = "station_history_plot_24h_gaps_no_wrap.png"
GAP_THRESHOLD_MINUTES = 1000 # Threshold for creating plot breaks

# --- Data Loading ---
print("INFO (PlotGraph): Loading data from database files...")
list_of_db_files = glob.glob(DB_FILE_PATTERN)
if not list_of_db_files:
    print(f"ERROR (PlotGraph): No database files found matching pattern '{DB_FILE_PATTERN}'. Ensure Crawler and JSONToDB scripts have run.", file=sys.stderr)
    sys.exit(1)

all_dfs = []
# Query selects all relevant columns and Station_No for filtering later
query_single_file = f"""
SELECT
    timestamp,
    "Station_No",
    "Available_Bikes_YB2",
    "Available_Bikes_EYB",
    "Available_Docks",
    "Forbidden_Spaces"
FROM "{TABLE_NAME}"
"""

for db_file in list_of_db_files:
    try:
        # Connect to each specific file in read-only mode
        with duckdb.connect(database=db_file, read_only=True) as con:
            df_single = con.execute(query_single_file).fetchdf()
            if not df_single.empty:
                all_dfs.append(df_single)
                # print(f"INFO (PlotGraph): Successfully read data from {db_file}. Rows: {len(df_single)}", file=sys.stderr) # Optional: log progress
            else:
                 print(f"INFO (PlotGraph): No data found in file {db_file}.", file=sys.stderr) # Optional: log empty files
    except duckdb.IOException as e:
         print(f"ERROR (PlotGraph): DuckDB IO Error reading file {db_file}: {e}", file=sys.stderr)
         # traceback.print_exc(file=sys.stderr) # Uncomment for detailed traceback
    except duckdb.CatalogException as e:
         print(f"ERROR (PlotGraph): DuckDB Catalog Error reading table '{TABLE_NAME}' from file {db_file}: {e}", file=sys.stderr)
         # traceback.print_exc(file=sys.stderr) # Uncomment for detailed traceback
    except Exception as e: # Catch any other errors per file
         print(f"ERROR (PlotGraph): An unexpected error occurred while reading file {db_file}: {e}", file=sys.stderr)
         # traceback.print_exc(file=sys.stderr) # Uncomment for detailed traceback


# Concatenate all DataFrames
if not all_dfs:
    print(f"ERROR (PlotGraph): No data successfully read from any database file.", file=sys.stderr)
    sys.exit(1)

try:
    df_history = pd.concat(all_dfs, ignore_index=True)
    # Ensure columns are of appropriate type *after* concatenation
    # Convert timestamp first, then others
    df_history['timestamp'] = pd.to_datetime(df_history['timestamp'], errors='coerce')
    df_history.dropna(subset=['timestamp'], inplace=True) # Drop rows where timestamp conversion failed

    df_history['Station_No'] = pd.to_numeric(df_history['Station_No'], errors='coerce').astype('Int64') # Use Int64 to handle potential NaNs gracefully
    df_history.dropna(subset=['Station_No'], inplace=True) # Drop rows where station number is invalid

    df_history['Available_Bikes_YB2'] = pd.to_numeric(df_history['Available_Bikes_YB2'], errors='coerce').fillna(0).astype(int)
    df_history['Available_Bikes_EYB'] = pd.to_numeric(df_history['Available_Bikes_EYB'], errors='coerce').fillna(0).astype(int)
    df_history['Available_Docks'] = pd.to_numeric(df_history['Available_Docks'], errors='coerce').fillna(0).astype(int)
    df_history['Forbidden_Spaces'] = pd.to_numeric(df_history['Forbidden_Spaces'], errors='coerce').fillna(0).astype(int)

    # Add 8 hours to timestamp assuming DuckDB TIMESTAMP stored as UTC
    df_history['timestamp'] = df_history['timestamp'] + pd.Timedelta(hours=8)

    # Calculate day of the week (Monday=0, Sunday=6) for filtering *after* +8 hours
    df_history['day_of_week'] = df_history['timestamp'].dt.dayofweek

    # Calculate Available_Bikes_Total *before* filtering by station
    df_history['Available_Bikes_Total'] = df_history['Available_Bikes_YB2'] + df_history['Available_Bikes_EYB']

    print(f"INFO (PlotGraph): Total records loaded across all files: {len(df_history)}", file=sys.stderr)

except Exception as e:
    print(f"ERROR (PlotGraph): Failed during data concatenation or initial processing: {e}", file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

# --- Interactive Input for Station and Filter ---

# Get list of unique station numbers
unique_stations = df_history['Station_No'].dropna().unique().tolist()
unique_stations.sort()

if not unique_stations:
    print("ERROR (PlotGraph): No valid station numbers found in the loaded data.", file=sys.stderr)
    sys.exit(1)

# Station number input
station_query_int = -1
while station_query_int not in unique_stations:
    station_input_str = input(f"請輸入要查詢的站點編號 ({unique_stations[0]} ~ {unique_stations[-1]}): ").strip()
    try:
        station_query_int = int(station_input_str)
        if station_query_int not in unique_stations:
             print(f"無效的站點編號 '{station_input_str}'。請輸入資料中實際存在的編號。")
    except ValueError:
        print(f"無效的輸入 '{station_input_str}'。請輸入一個數字。")


# Date filter input
filter_choice = -1
while filter_choice not in [1, 2, 3]:
    filter_input_str = input("請選擇輸出的日期範圍 (1. 所有日子 2. 只有平日 3. 只有假日): ").strip()
    try:
        filter_choice = int(filter_input_str)
        if filter_choice not in [1, 2, 3]:
            print("無效的選擇。請輸入 1, 2 或 3。")
    except ValueError:
        print("無效的輸入。請輸入一個數字 (1, 2 或 3)。")

# --- Data Processing (Filtering, Sorting, Gaps) ---

print(f"INFO (PlotGraph): Filtering data for station {station_query_int}...", file=sys.stderr)
# Filter by station number
df_filtered = df_history[df_history['Station_No'] == station_query_int].copy()

if df_filtered.empty:
    print(f"ERROR (PlotGraph): No data found for station number '{station_query_int}'.", file=sys.stderr)
    sys.exit(1)

# Apply date filtering based on choice
filter_description = "所有日子"
if filter_choice == 2: # Weekdays (Mon=0 to Fri=4)
    df_filtered = df_filtered[df_filtered['day_of_week'] < 5].copy()
    filter_description = "平日"
elif filter_choice == 3: # Weekends (Sat=5 to Sun=6)
    df_filtered = df_filtered[df_filtered['day_of_week'] >= 5].copy()
    filter_description = "假日"

if df_filtered.empty:
    print(f"ERROR (PlotGraph): No data found for station {station_query_int} during the selected period ({filter_description}).", file=sys.stderr)
    sys.exit(1)

print(f"INFO (PlotGraph): Data filtered. Records remaining: {len(df_filtered)}. Applying filters: Station={station_query_int}, Period={filter_description}", file=sys.stderr)

# Sort by timestamp
df_filtered = df_filtered.sort_values(by='timestamp').reset_index(drop=True)

record_count = len(df_filtered)

# --- Handle gaps and date changes for plotting breaks ---
# Important: This needs to happen *after* date filtering and *after* adding the 8 hours
if record_count > 1:
    df_filtered['time_diff'] = df_filtered['timestamp'].diff()
    time_threshold = pd.Timedelta(minutes=GAP_THRESHOLD_MINUTES)
    # Use the timestamp *after* adding 8 hours for date checks
    df_filtered['date_only'] = df_filtered['timestamp'].dt.date
    df_filtered['date_changed'] = df_filtered['date_only'] != df_filtered['date_only'].shift(1)

    # Create plotting column, insert NaN for breaks
    df_filtered['plot_Available_Bikes_Total'] = df_filtered['Available_Bikes_Total'].astype(float)

    # Identify indices where a break should occur (large time gap OR date change)
    indices_to_break = df_filtered[
        (df_filtered['time_diff'] > time_threshold) |
        (df_filtered['date_changed'])
    ].index

    if not indices_to_break.empty:
        df_filtered.loc[indices_to_break, 'plot_Available_Bikes_Total'] = np.nan
        print(f"INFO (PlotGraph): {len(indices_to_break)} gaps/date changes identified for plotting breaks.", file=sys.stderr)
else: # Only one or zero records remain after filtering - no gaps to handle
     df_filtered['plot_Available_Bikes_Total'] = df_filtered['Available_Bikes_Total'].astype(float)
     print(f"INFO (PlotGraph): Only {record_count} record(s) found after filtering. No gap analysis performed.", file=sys.stderr)


# --- Plotting setup ---
# Create plot_time based on a fixed date + time of day from the adjusted timestamp
base_date = pd.Timestamp('2000-01-01')
df_filtered['plot_time'] = base_date + (df_filtered['timestamp'] - df_filtered['timestamp'].dt.normalize())


# --- Plotting ---
try:
    print("INFO (PlotGraph): Generating plot...", file=sys.stderr)
    plt.figure(figsize=(15, 7))

    plt.plot(
        df_filtered['plot_time'],
        df_filtered['plot_Available_Bikes_Total'],
        marker='.', markersize=4, linestyle='-', linewidth=1,
        label='總可借 (Total)'
    )

    plt.xlabel("時間 (24小時制)", fontproperties=custom_font)
    plt.ylabel("數量", fontproperties=custom_font)
    plt.title(
        f"站點: {station_query_int}\n車輛數 24小時週期變化 ({filter_description})\n(間隔 > {GAP_THRESHOLD_MINUTES} 分鐘或跨日則斷線)",
        fontproperties=custom_font
    )
    plt.legend(prop=custom_font)
    plt.grid(True, linestyle='--', alpha=0.6)

    # Configure X-axis to show 24 hours
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=2)) # Show ticks every 2 hours
    start_time = base_date
    end_time = base_date + pd.Timedelta(days=1)
    plt.gca().set_xlim(start_time, end_time) # Ensure X-axis spans exactly 24 hours
    plt.gcf().autofmt_xdate() # Auto-format date labels if they overlap

    plt.tight_layout() # Adjust layout to prevent labels overlapping
    plt.savefig(OUTPUT_IMAGE_FILENAME, dpi=150)
    plt.close()

    print(f"INFO (PlotGraph): Plot saved successfully as {OUTPUT_IMAGE_FILENAME}", file=sys.stderr)

except ImportError:
     print("ERROR (PlotGraph): matplotlib library is required. Install with 'pip install matplotlib'.", file=sys.stderr)
     sys.exit(1)
except Exception as e: # Catch any plotting errors
     print(f"ERROR (PlotGraph): Error plotting or saving chart: {e}", file=sys.stderr)
     traceback.print_exc(file=sys.stderr)
     sys.exit(1)

sys.exit(0) # Success exit