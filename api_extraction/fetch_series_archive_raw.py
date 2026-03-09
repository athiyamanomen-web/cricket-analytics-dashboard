'''
Cricbuzz API (Series Archives for 2020 to 2025)
        ↓
For each year:
        ↓
Call series archive endpoint
        ↓
Read JSON response
        ↓
Extract series_id, series_name, startDt, endDt, year
        ↓
Append all series into one list
        ↓
Save combined series list to JSON file
        ↓
Print total series fetched
'''

# extracting series from 2020 to 2026
import http.client
import json
import time

API_KEY = "YOUR_API_KEY"

headers = {
    "x-rapidapi-key": "x",
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

all_series = []

for year in range(2020, 2026):

    print(f"\nFetching series for {year}")

    conn = http.client.HTTPSConnection("cricbuzz-cricket.p.rapidapi.com")

    endpoint = f"/series/v1/archives/international?year={year}"

    conn.request("GET", endpoint, headers=headers)

    res = conn.getresponse()

    data = res.read()

    json_data = json.loads(data.decode("utf-8"))

    if "seriesMapProto" not in json_data:
        print("No series found")
        continue

    for block in json_data["seriesMapProto"]:

        if "series" not in block:
            continue

        for s in block["series"]:

            series_id = s.get("id")
            name = s.get("name")
            start = s.get("startDt")
            end = s.get("endDt")

            row = {
                "series_id": series_id,
                "series_name": name,
                "startDt": start,
                "endDt": end,
                "year": year
            }

            all_series.append(row)

            print(series_id, "|", name)

    conn.close()

    time.sleep(1)  # prevents rate limit


print("\nTotal series fetched:", len(all_series))


with open("series_2020_2025.json", "w") as f:
    json.dump(all_series, f, indent=2)


print("\nSaved to series_2020_2025.json")