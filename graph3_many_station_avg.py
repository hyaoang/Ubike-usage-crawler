# -*- coding: utf-8 -*-
import duckdb
import pandas as pd
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib
import glob
from matplotlib import font_manager
import sys
import traceback
from datetime import datetime

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
STATION_LIST_FILE = "stations_with_park.txt"
AVERAGING_INTERVAL = '10min'
OUTPUT_IMAGE_FILENAME = "park_stations_combined_average.png" # Single output file

# --- Read Station List from File ---
print(f"INFO: Reading station list from '{STATION_LIST_FILE}'...")
try:
    with open(STATION_LIST_FILE, 'r') as f:
        station_list = [line.strip() for line in f if line.strip()]
    if not station_list:
        print(f"ERROR: The station file '{STATION_LIST_FILE}' is empty.", file=sys.stderr)
        sys.exit(1)
    station_list = [int(s) for s in station_list]
    print(f"INFO: Successfully read {len(station_list)} station numbers to process.")
except FileNotFoundError:
    print(f"ERROR: File '{STATION_LIST_FILE}' not found.", file=sys.stderr)
    sys.exit(1)
except ValueError:
    print(f"ERROR: '{STATION_LIST_FILE}' contains non-numeric values.", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"ERROR: An error occurred while reading '{STATION_LIST_FILE}': {e}", file=sys.stderr)
    sys.exit(1)

# --- Get Shared User Input ---
filter_choice = -1
while filter_choice not in [1, 2, 3]:
    filter_input_str = input("請選擇日期範圍 (1. 所有日子 2. 只有平日 3. 只有假日): ").strip()
    try:
        filter_choice = int(filter_input_str)
        if filter_choice not in [1, 2, 3]:
            print("無效的選擇。請輸入 1, 2 或 3。")
    except ValueError:
        print("無效的輸入。請輸入一個數字 (1, 2 或 3)。")

filter_description_map = {1: "所有日子", 2: "平日", 3: "假日"}
filter_description = filter_description_map[filter_choice]

# --- Find database files ---
print("\nINFO: Searching for database files...")
list_of_db_files = glob.glob(DB_FILE_PATTERN)
if not list_of_db_files:
    print(f"ERROR: No database files found matching pattern '{DB_FILE_PATTERN}'.", file=sys.stderr)
    sys.exit(1)
print(f"INFO: Found {len(list_of_db_files)} database files.")

# --- Main Processing Loop ---
# This list will hold the averaged DataFrame from each station
list_of_station_averages = []
processed_stations_count = 0

for station_query_int in station_list:
    print(f"\n--- Processing Station: {station_query_int} ---")
    
    try:
        # 1. Load data for the current station
        all_dfs = []
        query_single_file = f'SELECT timestamp, "Available_Bikes_YB2", "Available_Bikes_EYB" FROM "{TABLE_NAME}" WHERE "Station_No" = ?'
        for db_file in list_of_db_files:
            with duckdb.connect(database=db_file, read_only=True) as con:
                df_single = con.execute(query_single_file, [station_query_int]).fetchdf()
                if not df_single.empty:
                    all_dfs.append(df_single)

        if not all_dfs:
            raise ValueError("No data found in any database file.")

        # 2. Process and filter data
        df_history = pd.concat(all_dfs, ignore_index=True)
        df_history['timestamp'] = pd.to_datetime(df_history['timestamp'], errors='coerce')
        df_history.dropna(subset=['timestamp'], inplace=True)
        df_history['Available_Bikes_YB2'] = pd.to_numeric(df_history['Available_Bikes_YB2'], errors='coerce').fillna(0).astype(int)
        df_history['Available_Bikes_EYB'] = pd.to_numeric(df_history['Available_Bikes_EYB'], errors='coerce').fillna(0).astype(int)
        df_history['timestamp'] = df_history['timestamp'] + pd.Timedelta(hours=8)
        df_history['day_of_week'] = df_history['timestamp'].dt.dayofweek
        df_history['Available_Bikes_Total'] = df_history['Available_Bikes_YB2'] + df_history['Available_Bikes_EYB']
        
        df_filtered = df_history.copy()
        if filter_choice == 2: df_filtered = df_filtered[df_filtered['day_of_week'] < 5]
        elif filter_choice == 3: df_filtered = df_filtered[df_filtered['day_of_week'] >= 5]

        if df_filtered.empty:
            raise ValueError(f"No data found for the selected period ({filter_description}).")

        # 3. Calculate this station's average trend
        df_filtered.set_index('timestamp', inplace=True)
        df_resampled = df_filtered.resample(AVERAGING_INTERVAL).mean()
        df_daily_avg_single_station = df_resampled.groupby(df_resampled.index.time)[['Available_Bikes_Total']].mean()
        
        # 4. Add the result to our list for final aggregation
        list_of_station_averages.append(df_daily_avg_single_station)
        processed_stations_count += 1
        print(f"INFO (Station {station_query_int}): Successfully processed and added to the final average pool.")

    except ValueError as e:
        print(f"SKIPPING (Station {station_query_int}): {e}", file=sys.stderr)
    except Exception as e:
        print(f"ERROR (Station {station_query_int}): An unexpected error occurred. Skipping station.", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)

# --- Final Aggregation and Plotting ---
if not list_of_station_averages:
    print("\nERROR: No data could be processed for any of the stations in the list. No plot will be generated.", file=sys.stderr)
    sys.exit(1)

print(f"\n{'='*20} Finalizing Report {'='*20}")
print(f"INFO: Combining averages from {processed_stations_count} successfully processed stations...")

# Concatenate all individual station averages and then calculate the final mean across all of them
# The groupby(level=0) groups by the index (which is the time of day)
final_combined_average = pd.concat(list_of_station_averages).groupby(level=0).mean()

# Create the plottable time axis
base_date = pd.Timestamp('2000-01-01').date()
final_combined_average['plot_time'] = [datetime.combine(base_date, t) for t in final_combined_average.index]

try:
    print("INFO: Generating final combined plot...")
    plt.figure(figsize=(15, 7))
    interval_number = AVERAGING_INTERVAL.replace("min", "")
    plt.plot(
        final_combined_average['plot_time'],
        final_combined_average['Available_Bikes_Total'],
        marker='.', markersize=5, linestyle='-', linewidth=1.5,
        label=f'平均可借 (每 {interval_number} 分鐘)'
    )
    plt.xlabel("時間 (24小時制)", fontproperties=custom_font)
    plt.ylabel("平均車輛數", fontproperties=custom_font)
    plt.title(
        f"周邊站點 ({processed_stations_count} 個) 平均每日車輛數變化 ({filter_description})",
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

    print(f"\nSUCCESS: Combined plot saved as {OUTPUT_IMAGE_FILENAME}")
except Exception as e:
    print(f"\nERROR: Failed to generate the final plot: {e}", file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

sys.exit(0)