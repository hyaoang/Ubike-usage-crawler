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
import traceback
from datetime import datetime # (FIX 1) Import the standard datetime library

# --- Load custom font ---
try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
except Exception:
    script_dir = os.getcwd()

FONT_PATH = os.path.join(script_dir, "NotoSansTC-Regular.ttf")
custom_font = None
try:
    if os.path.exists(FONT_PATH):
        custom_font = font_manager.FontProperties(fname=FONT_PATH)
        font_manager.fontManager.addfont(FONT_PATH)
        matplotlib.rcParams['font.family'] = custom_font.get_name()
    else:
        custom_font = font_manager.FontProperties()
        print(f"WARNING (PlotGraph): Custom font not found at {FONT_PATH}. Using default font.", file=sys.stderr)
except Exception as e:
    custom_font = font_manager.FontProperties()
    print(f"WARNING (PlotGraph): Error loading custom font: {e}. Using default font.", file=sys.stderr)

matplotlib.rcParams['axes.unicode_minus'] = False

# --- Settings ---
DB_DIR = "."
DB_FILE_PATTERN = os.path.join(DB_DIR, "youbike_data_simplified*.duckdb")
TABLE_NAME = "bike_readings_simplified"
OUTPUT_IMAGE_FILENAME = "station_history_plot_average_day.png"
AVERAGING_INTERVAL = '10min' # (FIX 2) Changed '10T' to the recommended '10min'

# --- Find database files ---
print("INFO (PlotGraph): Searching for database files...")
list_of_db_files = glob.glob(DB_FILE_PATTERN)
if not list_of_db_files:
    print(f"ERROR (PlotGraph): No database files found matching pattern '{DB_FILE_PATTERN}'.", file=sys.stderr)
    sys.exit(1)
print(f"INFO (PlotGraph): Found {len(list_of_db_files)} database files.")

# --- Interactive Input ---
print("INFO (PlotGraph): Fetching list of unique stations...")
unique_stations = set()
try:
    files_to_check = list_of_db_files[:5]
    for db_file in files_to_check:
        with duckdb.connect(database=db_file, read_only=True) as con:
            stations_in_file = con.execute(f'SELECT DISTINCT "Station_No" FROM "{TABLE_NAME}"').fetchdf()['Station_No'].tolist()
            unique_stations.update(stations_in_file)
except Exception as e:
    print(f"WARNING (PlotGraph): Could not pre-fetch station list: {e}. You will need to enter a valid station number manually.", file=sys.stderr)

sorted_stations = sorted([s for s in unique_stations if s is not None])

if not sorted_stations:
    print("ERROR (PlotGraph): No valid station numbers could be found.", file=sys.stderr)
    sys.exit(1)

station_query_int = -1
while True:
    station_input_str = input(f"請輸入要查詢的站點編號 (例如: {sorted_stations[0]} ~ {sorted_stations[-1]}): ").strip()
    try:
        station_query_int = int(station_input_str)
        break
    except ValueError:
        print(f"無效的輸入 '{station_input_str}'。請輸入一個數字。")

filter_choice = -1
while filter_choice not in [1, 2, 3]:
    filter_input_str = input("請選擇輸出的日期範圍 (1. 所有日子 2. 只有平日 3. 只有假日): ").strip()
    try:
        filter_choice = int(filter_input_str)
        if filter_choice not in [1, 2, 3]:
            print("無效的選擇。請輸入 1, 2 或 3。")
    except ValueError:
        print("無效的輸入。請輸入一個數字 (1, 2 或 3)。")

# --- Data Loading with Filtering ---
print(f"INFO (PlotGraph): Loading data for station {station_query_int}...")
all_dfs = []
query_single_file = f"""
SELECT
    timestamp, "Available_Bikes_YB2", "Available_Bikes_EYB"
FROM "{TABLE_NAME}"
WHERE "Station_No" = ?
"""
for db_file in list_of_db_files:
    try:
        with duckdb.connect(database=db_file, read_only=True) as con:
            df_single = con.execute(query_single_file, [station_query_int]).fetchdf()
            if not df_single.empty:
                all_dfs.append(df_single)
    except Exception as e:
         print(f"ERROR (PlotGraph): An unexpected error occurred while reading file {db_file}: {e}", file=sys.stderr)

if not all_dfs:
    print(f"ERROR (PlotGraph): No data found for station {station_query_int} in any database file.", file=sys.stderr)
    sys.exit(1)

try:
    df_history = pd.concat(all_dfs, ignore_index=True)
    df_history['timestamp'] = pd.to_datetime(df_history['timestamp'], errors='coerce')
    df_history.dropna(subset=['timestamp'], inplace=True)
    df_history['Available_Bikes_YB2'] = pd.to_numeric(df_history['Available_Bikes_YB2'], errors='coerce').fillna(0).astype(int)
    df_history['Available_Bikes_EYB'] = pd.to_numeric(df_history['Available_Bikes_EYB'], errors='coerce').fillna(0).astype(int)
    df_history['timestamp'] = df_history['timestamp'] + pd.Timedelta(hours=8)
    df_history['day_of_week'] = df_history['timestamp'].dt.dayofweek
    df_history['Available_Bikes_Total'] = df_history['Available_Bikes_YB2'] + df_history['Available_Bikes_EYB']
    print(f"INFO (PlotGraph): Total records loaded for station {station_query_int}: {len(df_history)}", file=sys.stderr)
except Exception as e:
    print(f"ERROR (PlotGraph): Failed during data concatenation or initial processing: {e}", file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

# --- Date Filtering ---
df_filtered = df_history.copy()
filter_description = "所有日子"
if filter_choice == 2:
    df_filtered = df_filtered[df_filtered['day_of_week'] < 5].copy()
    filter_description = "平日"
elif filter_choice == 3:
    df_filtered = df_filtered[df_filtered['day_of_week'] >= 5].copy()
    filter_description = "假日"

if df_filtered.empty:
    print(f"ERROR (PlotGraph): No data found for station {station_query_int} during the selected period ({filter_description}).", file=sys.stderr)
    sys.exit(1)

print(f"INFO (PlotGraph): Data filtered by date. Records remaining: {len(df_filtered)}. Period={filter_description}", file=sys.stderr)


# --- Averaging Logic ---
print(f"INFO (PlotGraph): Calculating daily average trend using a '{AVERAGING_INTERVAL}' interval...", file=sys.stderr)
df_filtered.set_index('timestamp', inplace=True)
df_resampled = df_filtered.resample(AVERAGING_INTERVAL).mean()
df_daily_avg = df_resampled.groupby(df_resampled.index.time)[['Available_Bikes_Total']].mean()


# --- Plotting Setup ---
base_date = pd.Timestamp('2000-01-01').date()
# (FIX 1) Use the correct `datetime.combine` to create plottable timestamps
df_daily_avg['plot_time'] = [datetime.combine(base_date, t) for t in df_daily_avg.index]

# --- Plotting ---
try:
    print("INFO (PlotGraph): Generating plot...", file=sys.stderr)
    plt.figure(figsize=(15, 7))

    # (FIX 3) Make label generation robust to the change from 'T' to 'min'
    interval_number = AVERAGING_INTERVAL.replace("min", "")
    plt.plot(
        df_daily_avg['plot_time'],
        df_daily_avg['Available_Bikes_Total'],
        marker='.', markersize=5, linestyle='-', linewidth=1.5,
        label=f'平均可借 (每 {interval_number} 分鐘)'
    )

    plt.xlabel("時間 (24小時制)", fontproperties=custom_font)
    plt.ylabel("平均車輛數", fontproperties=custom_font)
    plt.title(
        f"站點: {station_query_int}\n平均每日車輛數變化 ({filter_description})",
        fontproperties=custom_font
    )
    plt.legend(prop=custom_font)
    plt.grid(True, linestyle='--', alpha=0.6)

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=2))
    start_time = pd.Timestamp(base_date)
    end_time = start_time + pd.Timedelta(days=1)
    plt.gca().set_xlim(start_time, end_time)
    plt.gca().set_ylim(bottom=0)
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

sys.exit(0)