import json
import re

# 載入 JSON 檔案
with open('ubikestation_names.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# 正則表達式：找出包含「公園」的資料
pattern = re.compile(r'公園')

# 過濾包含「公園」的站點
stations_with_park = [
    item.get('station_no') for item in data
    if pattern.search(item.get('name_tw', '')) or pattern.search(item.get('address_tw', ''))
]

# 存入文字檔
with open('stations_with_park.txt', 'w', encoding='utf-8') as f:
    for station_no in stations_with_park:
        if station_no:
            f.write(station_no + '\n')
