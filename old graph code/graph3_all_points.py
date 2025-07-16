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

# --- Define safe integer conversion function (No longer strictly needed with early filtering but good practice) ---
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

# --- Find database files first ---
print("INFO (PlotGraph): Searching for database files...")
list_of_db_files = glob.glob(DB_FILE_PATTERN)
if not list_of_db_files:
    print(f"ERROR (PlotGraph): No database files found matching pattern '{DB_FILE_PATTERN}'. Ensure Crawler and JSONToDB scripts have run.", file=sys.stderr)
    sys.exit(1)
print(f"INFO (PlotGraph): Found {len(list_of_db_files)} database files.")


# --- (CHANGED) Interactive Input for Station and Filter MOVED TO THE TOP ---
# We need the station number BEFORE loading data to filter efficiently.

# For getting a list of stations, we still need to query the database.
# Let's do a quick, distinct query on one file to get a representative list.
# This avoids loading all data just to find station numbers.
print("INFO (PlotGraph): Fetching list of unique stations...")
unique_stations = set()
try:
    # Query a few files to get a good list of stations
    files_to_check = list_of_db_files[:5] # Check up to 5 files
    for db_file in files_to_check:
        with duckdb.connect(database=db_file, read_only=True) as con:
            stations_in_file = con.execute(f'SELECT DISTINCT "Station_No" FROM "{TABLE_NAME}"').fetchdf()['Station_No'].tolist()
            unique_stations.update(stations_in_file)
except Exception as e:
    print(f"WARNING (PlotGraph): Could not pre-fetch station list: {e}. You will need to enter a valid station number manually.", file=sys.stderr)

sorted_stations = sorted([s for s in unique_stations if s is not None])

if not sorted_stations:
    print("ERROR (PlotGraph): No valid station numbers could be found in the first few database files.", file=sys.stderr)
    sys.exit(1)

# Station number input
station_query_int = -1
while True:
    station_input_str = input(f"請輸入要查詢的站點編號 (例如: {sorted_stations[0]} ~ {sorted_stations[-1]}): ").strip()
    try:
        station_query_int = int(station_input_str)
        # We don't strictly need to validate against the list now, as the query will just return empty if it doesn't exist.
        break
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

# --- (CHANGED) Data Loading with Filtering ---
print(f"INFO (PlotGraph): Loading data for station {station_query_int} from database files...")
all_dfs = []

# (CHANGED) The query now includes a WHERE clause to filter AT THE SOURCE.
# We use parameterized queries (?) to prevent SQL injection.
query_single_file = f"""
SELECT
    timestamp,
    "Station_No",
    "Available_Bikes_YB2",
    "Available_Bikes_EYB",
    "Available_Docks",
    "Forbidden_Spaces"
FROM "{TABLE_NAME}"
WHERE "Station_No" = ?
"""

for db_file in list_of_db_files:
    try:
        with duckdb.connect(database=db_file, read_only=True) as con:
            # Execute the query with the station number as a parameter
            df_single = con.execute(query_single_file, [station_query_int]).fetchdf()
            if not df_single.empty:
                all_dfs.append(df_single)
    except duckdb.IOException as e:
         print(f"ERROR (PlotGraph): DuckDB IO Error reading file {db_file}: {e}", file=sys.stderr)
    except duckdb.CatalogException as e:
         print(f"ERROR (PlotGraph): DuckDB Catalog Error reading table '{TABLE_NAME}' from file {db_file}: {e}", file=sys.stderr)
    except Exception as e: # Catch any other errors per file
         print(f"ERROR (PlotGraph): An unexpected error occurred while reading file {db_file}: {e}", file=sys.stderr)

# Concatenate all DataFrames (now this operation is very cheap as we only have data for one station)
if not all_dfs:
    print(f"ERROR (PlotGraph): No data found for station {station_query_int} in any database file.", file=sys.stderr)
    sys.exit(1)

try:
    df_history = pd.concat(all_dfs, ignore_index=True)
    # Ensure columns are of appropriate type *after* concatenation
    df_history['timestamp'] = pd.to_datetime(df_history['timestamp'], errors='coerce')
    df_history.dropna(subset=['timestamp'], inplace=True) # Drop rows where timestamp conversion failed

    # The rest of the processing can largely remain the same
    df_history['Station_No'] = pd.to_numeric(df_history['Station_No'], errors='coerce').astype('Int64')
    df_history['Available_Bikes_YB2'] = pd.to_numeric(df_history['Available_Bikes_YB2'], errors='coerce').fillna(0).astype(int)
    df_history['Available_Bikes_EYB'] = pd.to_numeric(df_history['Available_Bikes_EYB'], errors='coerce').fillna(0).astype(int)
    df_history['Available_Docks'] = pd.to_numeric(df_history['Available_Docks'], errors='coerce').fillna(0).astype(int)
    df_history['Forbidden_Spaces'] = pd.to_numeric(df_history['Forbidden_Spaces'], errors='coerce').fillna(0).astype(int)

    # Add 8 hours to timestamp assuming DuckDB TIMESTAMP stored as UTC
    df_history['timestamp'] = df_history['timestamp'] + pd.Timedelta(hours=8)

    # Calculate day of the week (Monday=0, Sunday=6) for filtering *after* +8 hours
    df_history['day_of_week'] = df_history['timestamp'].dt.dayofweek

    # Calculate Available_Bikes_Total
    df_history['Available_Bikes_Total'] = df_history['Available_Bikes_YB2'] + df_history['Available_Bikes_EYB']

    print(f"INFO (PlotGraph): Total records loaded for station {station_query_int}: {len(df_history)}", file=sys.stderr)

except Exception as e:
    print(f"ERROR (PlotGraph): Failed during data concatenation or initial processing: {e}", file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

# --- Data Processing (Filtering by date, Sorting, Gaps) ---
# NOTE: The variable is now df_history, not df_filtered, for clarity.
# The station filtering is already done.

# Apply date filtering based on choice
filter_description = "所有日子"
if filter_choice == 2: # Weekdays (Mon=0 to Fri=4)
    df_history = df_history[df_history['day_of_week'] < 5].copy()
    filter_description = "平日"
elif filter_choice == 3: # Weekends (Sat=5 to Sun=6)
    df_history = df_history[df_history['day_of_week'] >= 5].copy()
    filter_description = "假日"

if df_history.empty:
    print(f"ERROR (PlotGraph): No data found for station {station_query_int} during the selected period ({filter_description}).", file=sys.stderr)
    sys.exit(1)

print(f"INFO (PlotGraph): Data filtered by date. Records remaining: {len(df_history)}. Period={filter_description}", file=sys.stderr)

# Sort by timestamp
df_filtered = df_history.sort_values(by='timestamp').reset_index(drop=True) # Renaming to match the rest of your script

record_count = len(df_filtered)

# --- Handle gaps and date changes for plotting breaks ---
if record_count > 1:
    df_filtered['time_diff'] = df_filtered['timestamp'].diff()
    time_threshold = pd.Timedelta(minutes=GAP_THRESHOLD_MINUTES)
    df_filtered['date_only'] = df_filtered['timestamp'].dt.date
    df_filtered['date_changed'] = df_filtered['date_only'] != df_filtered['date_only'].shift(1)
    df_filtered['plot_Available_Bikes_Total'] = df_filtered['Available_Bikes_Total'].astype(float)
    indices_to_break = df_filtered[
        (df_filtered['time_diff'] > time_threshold) |
        (df_filtered['date_changed'])
    ].index
    if not indices_to_break.empty:
        df_filtered.loc[indices_to_break, 'plot_Available_Bikes_Total'] = np.nan
        print(f"INFO (PlotGraph): {len(indices_to_break)} gaps/date changes identified for plotting breaks.", file=sys.stderr)
else:
     df_filtered['plot_Available_Bikes_Total'] = df_filtered['Available_Bikes_Total'].astype(float)
     print(f"INFO (PlotGraph): Only {record_count} record(s) found. No gap analysis performed.", file=sys.stderr)

# --- Plotting setup ---
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

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=2))
    start_time = base_date
    end_time = base_date + pd.Timedelta(days=1)
    plt.gca().set_xlim(start_time, end_time)
    plt.gcf().autofmt_xdate()

    plt.tight_layout()
    plt.savefig(OUTPUT_IMAGE_FILENAME, dpi=150)
    plt.close()

    print(f"INFO (PlotGraph): Plot saved successfully as {OUTPUT_IMAGE_FILENAME}", file=sys.stderr)

except ImportError:
     print("ERROR (PlotGraph): matplotlib library is required. Install with 'pip install matplotlib'.", file=sys.stderr)
     sys.exit(1)
except Exception as e:
     print(f"ERROR (PlotGraph): Error plotting or saving chart: {e}", file=sys.stderr)
     traceback.print_exc(file=sys.stderr)
     sys.exit(1)

sys.exit(0) # Success exit