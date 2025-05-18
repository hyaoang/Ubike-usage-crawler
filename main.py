import requests
import time
import os
import datetime
import json
import sys
import urllib3
import duckdb
import pandas as pd
import glob # Add import glob here

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

input_coordinates = [
    {'lat': 22.425263991404677, 'lng': 120.582004057851}, {'lat': 22.580102887267227, 'lng': 120.48209323577278},
    {'lat': 22.73792754978397, 'lng': 120.57653834036613}, {'lat': 22.891159620382254, 'lng': 120.37895598433667},
    {'lat': 23.049048552661123, 'lng': 120.47345304305459}, {'lat': 23.20209424278999, 'lng': 120.27527705826682},
    {'lat': 23.20689753787391, 'lng': 120.56815781307346}, {'lat': 23.360048700993353, 'lng': 120.36982508326363},
    {'lat': 23.514651501722646, 'lng': 120.26888158894467}, {'lat': 23.517963275879, 'lng': 120.46458382793315},
    {'lat': 23.98844529936981, 'lng': 120.55369665678275}, {'lat': 24.147775518560874, 'lng': 120.74742123214999},
    {'lat': 24.150553500995354, 'lng': 120.94414438599475}, {'lat': 24.305524123495232, 'lng': 120.84314606066775},
    {'lat': 24.62087900942737, 'lng': 121.03527658945077}, {'lat': 24.77848257828158, 'lng': 121.13168777505285},
    {'lat': 24.78082021027597, 'lng': 121.32943853434708}, {'lat': 24.782896816333114, 'lng': 121.5272127904202},
    {'lat': 24.938258000406208, 'lng': 121.42634472581803}, {'lat': 24.940217968239573, 'lng': 121.62437842338083},
    {'lat': 25.09564149347577, 'lng': 121.5234895251505}, {'lat': 23.052046616258256, 'lng': 120.6685066077913},
    {'lat': 24.4604072036473, 'lng': 120.74189175021667}, {'lat': 24.936035080542425, 'lng': 121.22833325854033},
    {'lat': 24.302598351953687, 'lng': 120.64619822446141}, {'lat': 22.744570010601805, 'lng': 121.06315266002395},
    {'lat': 22.894254278500295, 'lng': 120.57376921502104}, {'lat': 22.734856343688836, 'lng': 120.38194685893976},
    {'lat': 23.045810334125566, 'lng': 120.27843340585149}, {'lat': 24.144744346894143, 'lng': 120.5507292127468},
    {'lat': 25.097482960484612, 'lng': 121.72178417652836}, {'lat': 24.618169554001263, 'lng': 120.83781252215609},
    {'lat': 23.356639674539824, 'lng': 120.17437047966706}, {'lat': 25.09353514639796, 'lng': 121.3252158230692},
    {'lat': 22.73937442943124, 'lng': 120.67384635172384}, {'lat': 23.989981271459342, 'lng': 120.65191973056952},
    {'lat': 23.20375581887705, 'lng': 120.37289523225358}, {'lat': 23.361661838728452, 'lng': 120.46756589907908},
    {'lat': 23.519527071163353, 'lng': 120.56244803542445}, {'lat': 22.581597447588155, 'lng': 120.5792832472417},
    {'lat': 25.173321990522773, 'lng': 121.4729468672537}, {'lat': 22.88952293275849, 'lng': 120.28156235133225},
    {'lat': 24.69839246270602, 'lng': 120.98465249150593}, {'lat': 23.512903568026346, 'lng': 120.1710442196712},
    {'lat': 24.85602683724819, 'lng': 121.081050039344}, {'lat': 25.17431244725984, 'lng': 121.57215227759824},
    {'lat': 25.020494500156612, 'lng': 121.87123388470181}, {'lat': 23.200372218833564, 'lng': 120.17766806543968},
    {'lat': 22.423780881096935, 'lng': 120.48492300943461}, {'lat': 22.81380140183371, 'lng': 120.42912705109582},
    {'lat': 22.657490384528085, 'lng': 120.43204965242876}, {'lat': 22.665784707548486, 'lng': 121.01561065041615},
    {'lat': 22.733232058840148, 'lng': 120.28466403466005}, {'lat': 24.381508506094313, 'lng': 120.69401675270105},
    {'lat': 24.223676909554552, 'lng': 120.59843582448234}, {'lat': 22.036490559485458, 'lng': 120.73393539414874},
    {'lat': 22.971662201882094, 'lng': 120.523641170888}, {'lat': 23.12641315153905, 'lng': 120.4232045057004},
    {'lat': 23.129482814867504, 'lng': 120.61836243641828}, {'lat': 23.59530476821713, 'lng': 120.41412533190929},
    {'lat': 23.43736131367541, 'lng': 120.31938429062403}, {'lat': 24.22665561042598, 'lng': 120.79525556099003},
    {'lat': 24.22938016057632, 'lng': 120.99210583609916}, {'lat': 24.3843808891004, 'lng': 120.89109307152162},
    {'lat': 24.70091673781121, 'lng': 121.1822603683162}, {'lat': 24.85843829334344, 'lng': 121.27891848505345},
    {'lat': 24.860587822022353, 'lng': 121.47681118841818}, {'lat': 24.862475304261185, 'lng': 121.6747253643108},
    {'lat': 25.018857570832566, 'lng': 121.67305111011298}, {'lat': 25.016956620247353, 'lng': 121.47488712479118},
    {'lat': 23.44212703106612, 'lng': 120.61279121370782}, {'lat': 24.542058501573816, 'lng': 120.98715799308282},
    {'lat': 24.068884016236392, 'lng': 120.69964273479806}, {'lat': 22.82334418927597, 'lng': 121.11074674982149},
    {'lat': 24.539294275949434, 'lng': 120.78982355902652}, {'lat': 25.014791753852933, 'lng': 121.27674473108664},
    {'lat': 24.065800831639898, 'lng': 120.50307805023205}, {'lat': 25.252008699180976, 'lng': 121.52160436772681},
    {'lat': 23.044101221222427, 'lng': 120.18093712524313}, {'lat': 22.968495990956114, 'lng': 120.3287248775017},
    {'lat': 23.281076374693868, 'lng': 120.32252489538843}, {'lat': 22.660490968859744, 'lng': 120.6265392803215},
    {'lat': 24.775884049662054, 'lng': 120.93396328853825}, {'lat': 22.578549641930614, 'lng': 120.3849115776941},
    {'lat': 22.88782670202258, 'lng': 120.18417780565336}, {'lat': 24.93482505152464, 'lng': 121.12933673393988},
    {'lat': 22.113990020251908, 'lng': 120.68424778005516}, {'lat': 25.172265086071757, 'lng': 121.373746884953},
    {'lat': 24.94266465127948, 'lng': 121.9214645387845}, {'lat': 22.346364567055698, 'lng': 120.53484098499591},
    {'lat': 24.697033, 'lng': 120.885859}, {'lat': 22.501176, 'lng': 120.434947},
    {'lat': 25.175236, 'lng': 121.671363}
]
REALTIME_INFO_URL = "https://apis.youbike.com.tw/tw2/parkingInfo"

# --- DB 設定 (Crawler) ---
DB_DIR = "."
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


# --- API 抓取部分 (保存到 JSON) ---
current_time_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_JSON_FILENAME = f"youbike_raw_{current_time_str}.json"

HEADERS_REALTIME = {
    'accept': '*/*', 'accept-language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    'origin': 'https://www.youbike.com.tw', 'priority': 'u=1, i',
    'referer': 'https://www.youbike.com.tw/', 'content-type': 'application/json',
    'sec-ch-ua': '"Chromium";v="136", "Not/A)Brand";v="99", "Google Chrome";v="136"',
    'sec-ch-ua-mobile': '?0', 'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty', 'sec-fetch-mode': 'cors', 'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36'
}

MAX_DISTANCE = 10000
REQUEST_DELAY = 0.1

all_stations_combined = []
successful_requests = 0
total_stations_processed = 0

try:
    for i, coord_info in enumerate(input_coordinates):
        lat = coord_info['lat']
        lng = coord_info['lng']
        payload = { "lat": lat, "lng": lng, "maxDistance": MAX_DISTANCE }

        try:
            response_realtime = requests.post(REALTIME_INFO_URL, headers=HEADERS_REALTIME, json=payload, timeout=20, verify=False)
            response_realtime.raise_for_status()
            realtime_data = response_realtime.json()

            if isinstance(realtime_data, dict) and realtime_data.get("retCode") == 1 and 'retVal' in realtime_data:
                stations_nearby_info = realtime_data['retVal']
                if isinstance(stations_nearby_info, list):
                    successful_requests += 1
                    total_stations_processed += len(stations_nearby_info)
                    all_stations_combined.extend(stations_nearby_info)

        except requests.exceptions.RequestException:
            sys.exit(1)

        time.sleep(REQUEST_DELAY)

except Exception:
    sys.exit(1)

if all_stations_combined:
    try:
        with open(OUTPUT_JSON_FILENAME, 'w', encoding='utf-8') as f:
            json.dump(all_stations_combined, f, ensure_ascii=False, indent=2)
    except IOError:
        sys.exit(1)


# --- JSON 讀取與 DuckDB 寫入部分 ---
try:
    # Use glob.glob from the imported glob module
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
                    if s_no_int > 0:
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
        df_to_write['timestamp'] = pd.to_datetime(df_to_write['timestamp']).dt.tz_convert(None) # TIMESTAMP without time zone
        df_to_write['Station_No'] = pd.to_numeric(df_to_write['Station_No'], errors='coerce').astype('Int32') # INTEGER
        df_to_write['Available_Bikes_YB2'] = pd.to_numeric(df_to_write['Available_Bikes_YB2'], errors='coerce').astype('UInt8')
        df_to_write['Available_Bikes_EYB'] = pd.to_numeric(df_to_write['Available_Bikes_EYB'], errors='coerce').astype('UInt8')
        df_to_write['Available_Docks'] = pd.to_numeric(df_to_write['Available_Docks'], errors='coerce').astype('UInt8')
        df_to_write['Forbidden_Spaces'] = pd.to_numeric(df_to_write['Forbidden_Spaces'], errors='coerce').astype('UInt8')

        df_to_write.dropna(subset=['timestamp', 'Station_No', 'Available_Bikes_YB2', 'Available_Bikes_EYB', 'Available_Docks', 'Forbidden_Spaces'], inplace=True)

        if not df_to_write.empty:
            DB_FILENAME_TO_WRITE, current_db_index = get_active_db_file(DB_DIR, DB_BASENAME, DB_EXTENSION, FILE_SIZE_LIMIT_BYTES)

            with duckdb.connect(database=DB_FILENAME_TO_WRITE, read_only=False) as con:
                # Use the new, more compact schema
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    timestamp           TIMESTAMP, -- Use TIMESTAMP
                    "Station_No"        INTEGER,   -- Use INTEGER
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

sys.exit(0)