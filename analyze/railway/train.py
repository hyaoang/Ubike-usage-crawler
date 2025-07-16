import json
import json
import re

# 載入 JSON 檔案
with open('ubikestation_names.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# 正則表達式：匹配「車站」或「火車站」，但排除「公車站」
train_pattern = re.compile(r'(火)?車站')
bus_pattern = re.compile(r'公車站')

# 過濾並印出符合條件的 station_no
for item in data:
    name = item.get('name_tw', '')
    if train_pattern.search(name) and not bus_pattern.search(name):
        print(item.get('station_no'))