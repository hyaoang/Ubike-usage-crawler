import os
import datetime
import json
import glob
import duckdb
import pandas as pd
import sys


DB_BASENAME = "youbike_data_simplified"
DB_EXTENSION = ".duckdb"
FILE_SIZE_LIMIT_MB = 90
FILE_SIZE_LIMIT_BYTES = FILE_SIZE_LIMIT_MB * 1024 * 1024
TABLE_NAME = "bike_readings_simplified"

def get_active_db_file(base_dir, base_name, extension, size_limit_bytes):
    n = 1
    while True:
        current_filename = f"{base_name}{n if n > 1 else ''}{extension}"
        current_filepath = os.path.join(base_dir, current_filename)

        if os.path.exists(current_filepath):
            try:
                current_size = os.path.getsize(current_filepath)
                if current_size < size_limit_bytes:
                    return current_filepath, n
                else:
                    n += 1
            except OSError:
                n += 1
        else:
            os.makedirs(base_dir, exist_ok=True)
            return current_filepath, n

DB_FILENAME, current_db_index = get_active_db_file('.', DB_BASENAME, DB_EXTENSION, FILE_SIZE_LIMIT_BYTES)


try:
    list_of_files = glob.glob('youbike_raw_*.json')
    if not list_of_files:
        sys.exit(1)
    latest_raw_json_file = max(list_of_files, key=os.path.getmtime)
except Exception:
    sys.exit(1)

all_records_to_write = []
raw_data_list = []
try:
    with open(latest_raw_json_file, 'r', encoding='utf-8') as f:
        raw_data_list = json.load(f)

    def safe_int_conversion(value):
        if value is None:
            return 0
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0

    try:
        processing_timestamp = pd.Timestamp.now(tz='Asia/Taipei')
    except Exception:
        processing_timestamp = pd.Timestamp.now(tz='UTC')


    for station_rt in raw_data_list:
         if isinstance(station_rt, dict):
            station_no_raw = station_rt.get('station_no')
            if station_no_raw is not None:
                available_spaces_detail = station_rt.get('available_spaces_detail', {})
                if isinstance(available_spaces_detail, dict):
                    s_no_int = safe_int_conversion(station_no_raw)
                    if s_no_int > 0: # Only proceed if station_no is a valid positive integer
                        yb2_raw = available_spaces_detail.get('yb2')
                        eyb_raw = available_spaces_detail.get('eyb')
                        docks_raw = station_rt.get('empty_spaces')
                        forbidden_raw = station_rt.get('forbidden_spaces')

                        yb2_bikes = safe_int_conversion(yb2_raw)
                        eyb_bikes = safe_int_conversion(eyb_raw)
                        docks = safe_int_conversion(docks_raw)
                        forbidden = safe_int_conversion(forbidden_raw)

                        all_records_to_write.append({
                            'timestamp': processing_timestamp,
                            'Station_No': s_no_int,
                            'Available_Bikes_YB2': yb2_bikes,
                            'Available_Bikes_EYB': eyb_bikes,
                            'Available_Docks': docks,
                            'Forbidden_Spaces': forbidden
                        })

except (json.JSONDecodeError, IOError):
    sys.exit(1)
except Exception:
    sys.exit(1)

if all_records_to_write:
    try:
        df_to_write = pd.DataFrame(all_records_to_write)
        df_to_write['timestamp'] = pd.to_datetime(df_to_write['timestamp'], utc=True).dt.tz_convert('Asia/Taipei')
        df_to_write['Station_No'] = pd.to_numeric(df_to_write['Station_No'], errors='coerce').astype('Int64')
        df_to_write['Available_Bikes_YB2'] = pd.to_numeric(df_to_write['Available_Bikes_YB2'], errors='coerce').astype('UInt8')
        df_to_write['Available_Bikes_EYB'] = pd.to_numeric(df_to_write['Available_Bikes_EYB'], errors='coerce').astype('UInt8')
        df_to_write['Available_Docks'] = pd.to_numeric(df_to_write['Available_Docks'], errors='coerce').astype('UInt8')
        df_to_write['Forbidden_Spaces'] = pd.to_numeric(df_to_write['Forbidden_Spaces'], errors='coerce').astype('UInt8')

        df_to_write.dropna(subset=['timestamp', 'Station_No', 'Available_Bikes_YB2', 'Available_Bikes_EYB', 'Available_Docks', 'Forbidden_Spaces'], inplace=True)

        if not df_to_write.empty:
            with duckdb.connect(database=DB_FILENAME, read_only=False) as con:
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    timestamp           TIMESTAMP WITH TIME ZONE,
                    "Station_No"        BIGINT,
                    "Available_Bikes_YB2" UTINYINT,
                    "Available_Bikes_EYB" UTINYINT,
                    "Available_Docks"   UTINYINT,
                    "Forbidden_Spaces"  UTINYINT,
                    PRIMARY KEY ("Station_No", timestamp)
                );
                """
                con.execute(create_table_sql)

                insert_sql = f"""
                INSERT OR IGNORE INTO {TABLE_NAME}
                SELECT * FROM df_to_write;
                """
                con.execute(insert_sql)

    except Exception:
        sys.exit(1)