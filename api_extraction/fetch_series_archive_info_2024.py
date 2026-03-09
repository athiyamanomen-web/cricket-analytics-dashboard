###############   8

'''
Cricbuzz API (Series Archives - 2024)
        ↓
Fetch archive payload (list of series)
        ↓
Extract series IDs and basic details
        ↓
For each series_id:
        ↓
Call Series Details API
        ↓
Extract series info (match formats, dates, total matches)
        ↓
Call Series Venues API
        ↓
Extract venue data (venue_id, name, city, country)
        ↓
Connect to MySQL (cricket_db)
        ↓
Create tables: series_list, series_info, series_venues
        ↓
Insert / Update series list
        ↓
Insert / Update series info
        ↓
Insert / Update series venues
        ↓
Store series and venue data in database
'''

import requests
import mysql.connector
from datetime import datetime

# =========================================================
# API CONFIG
# =========================================================
ARCHIVE_URL = "https://cricbuzz-cricket.p.rapidapi.com/series/v1/archives/international?year=2024"

# Replace these with your real endpoint formats if different
MATCH_DETAILS_URL_TEMPLATE = "https://cricbuzz-cricket.p.rapidapi.com/series/v1/{series_id}"
VENUES_URL_TEMPLATE = "https://cricbuzz-cricket.p.rapidapi.com/series/v1/{series_id}/venues"

HEADERS = {
    "x-rapidapi-key": "",
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

# =========================================================
# MYSQL CONFIG
# =========================================================
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
MYSQL_DATABASE = "cricket_db"

# =========================================================
# HELPERS
# =========================================================
def ms_to_date(ms):
    if not ms:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000).strftime("%Y-%m-%d")
    except:
        return None

def fetch_json(url):
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    return resp.json()

# =========================================================
# PAYLOAD 1 -> SERIES LIST
# =========================================================
def extract_series_list(archive_payload, limit=20):
    rows = []

    for block in archive_payload.get("seriesMapProto", []):
        for s in block.get("series", []):
            rows.append({
                "series_id": s.get("id"),
                "series_name": s.get("name"),
                "archive_start_date": ms_to_date(s.get("startDt")),
                "archive_end_date": ms_to_date(s.get("endDt")),
                "image_id": s.get("thumborImageId")
            })

    return rows[:limit]

# =========================================================
# PAYLOAD 2 -> SERIES INFO
# =========================================================
def extract_series_info(detail_payload):
    total_matches = 0
    series_id = None
    series_name = None
    series_start_date = None
    series_end_date = None
    match_formats = set()

    for item in detail_payload.get("matchDetails", []):
        match_map = item.get("matchDetailsMap")
        if not match_map:
            continue   # skip adDetail

        for match in match_map.get("match", []):
            match_info = match.get("matchInfo", {})
            if not match_info:
                continue

            total_matches += 1

            if series_id is None:
                series_id = match_info.get("seriesId")

            if series_name is None:
                series_name = match_info.get("seriesName")

            if series_start_date is None:
                series_start_date = (
                    ms_to_date(match_info.get("seriesStartDt")) or
                    ms_to_date(match_info.get("startDate"))
                )

            if series_end_date is None:
                series_end_date = (
                    ms_to_date(match_info.get("seriesEndDt")) or
                    ms_to_date(match_info.get("endDate"))
                )

            fmt = match_info.get("matchFormat")
            if fmt:
                match_formats.add(fmt)

    return {
        "series_id": series_id,
        "series_name": series_name,
        "match_type": ", ".join(sorted(match_formats)) if match_formats else None,
        "series_start_date": series_start_date,
        "series_end_date": series_end_date,
        "total_matches_planned": total_matches
    }

# =========================================================
# PAYLOAD 3 -> SERIES VENUES
# =========================================================
def extract_series_venues(venues_payload):
    series_id = venues_payload.get("seriesId")
    series_name = venues_payload.get("seriesName")
    venue_rows = []
    countries = set()

    for v in venues_payload.get("seriesVenue", []):
        country = v.get("country")
        if country:
            countries.add(country)

        venue_rows.append({
            "series_id": series_id,
            "series_name": series_name,
            "venue_id": v.get("id"),
            "venue_name": v.get("ground"),
            "city": v.get("city"),
            "country": country,
            "image_id": v.get("imageId")
        })

    return venue_rows, ", ".join(sorted(countries)) if countries else "Unknown"

# =========================================================
# FETCH ARCHIVE SERIES IDS
# =========================================================
archive_payload = fetch_json(ARCHIVE_URL)
series_list_rows = extract_series_list(archive_payload, limit=20)

print(f"Fetched {len(series_list_rows)} series from archive payload")

# =========================================================
# MYSQL CONNECTION
# =========================================================
conn = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD
)
cursor = conn.cursor()

# =========================================================
# CREATE DATABASE + TABLES
# =========================================================
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DATABASE}")
cursor.execute(f"USE {MYSQL_DATABASE}")

cursor.execute("""
CREATE TABLE IF NOT EXISTS series_list (
    series_id INT PRIMARY KEY,
    series_name VARCHAR(255),
    archive_start_date DATE,
    archive_end_date DATE,
    image_id INT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS series_info (
    series_id INT PRIMARY KEY,
    series_name VARCHAR(255),
    host_countries VARCHAR(255),
    match_type VARCHAR(100),
    series_start_date DATE,
    series_end_date DATE,
    total_matches_planned INT,
    FOREIGN KEY (series_id) REFERENCES series_list(series_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS series_venues (
    series_id INT,
    venue_id INT,
    venue_name VARCHAR(255),
    city VARCHAR(100),
    country VARCHAR(100),
    image_id INT,
    PRIMARY KEY (series_id, venue_id),
    FOREIGN KEY (series_id) REFERENCES series_list(series_id)
)
""")

# =========================================================
# INSERT INTO series_list
# =========================================================
insert_series_list_sql = """
INSERT INTO series_list (
    series_id, series_name, archive_start_date, archive_end_date, image_id
)
VALUES (%s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    series_name = VALUES(series_name),
    archive_start_date = VALUES(archive_start_date),
    archive_end_date = VALUES(archive_end_date),
    image_id = VALUES(image_id)
"""

series_list_values = [
    (
        row["series_id"],
        row["series_name"],
        row["archive_start_date"],
        row["archive_end_date"],
        row["image_id"]
    )
    for row in series_list_rows
]

cursor.executemany(insert_series_list_sql, series_list_values)
conn.commit()

print("Inserted series_list rows")

# =========================================================
# FETCH PAYLOAD 2 + PAYLOAD 3 FOR EACH SERIES
# =========================================================
insert_series_info_sql = """
INSERT INTO series_info (
    series_id, series_name, host_countries, match_type,
    series_start_date, series_end_date, total_matches_planned
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    series_name = VALUES(series_name),
    host_countries = VALUES(host_countries),
    match_type = VALUES(match_type),
    series_start_date = VALUES(series_start_date),
    series_end_date = VALUES(series_end_date),
    total_matches_planned = VALUES(total_matches_planned)
"""

insert_series_venues_sql = """
INSERT INTO series_venues (
    series_id, venue_id, venue_name, city, country, image_id
)
VALUES (%s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    venue_name = VALUES(venue_name),
    city = VALUES(city),
    country = VALUES(country),
    image_id = VALUES(image_id)
"""

for row in series_list_rows:
    series_id = row["series_id"]
    print(f"Processing series_id = {series_id}")

    # ---------- payload 2 ----------
    match_url = MATCH_DETAILS_URL_TEMPLATE.format(series_id=series_id)
    try:
        detail_payload = fetch_json(match_url)
        info = extract_series_info(detail_payload)
    except Exception as e:
        print(f"  Match details fetch failed for {series_id}: {e}")
        info = {
            "series_id": series_id,
            "series_name": row["series_name"],
            "match_type": None,
            "series_start_date": row["archive_start_date"],
            "series_end_date": row["archive_end_date"],
            "total_matches_planned": None
        }

    # ---------- payload 3 ----------
    venue_url = VENUES_URL_TEMPLATE.format(series_id=series_id)
    try:
        venues_payload = fetch_json(venue_url)
        venue_rows, host_countries = extract_series_venues(venues_payload)

        for v in venue_rows:
            cursor.execute(
                insert_series_venues_sql,
                (
                    v["series_id"],
                    v["venue_id"],
                    v["venue_name"],
                    v["city"],
                    v["country"],
                    v["image_id"]
                )
            )
        conn.commit()

    except Exception as e:
        print(f"  Venues fetch failed for {series_id}: {e}")
        host_countries = "Unknown"

    # ---------- insert series_info ----------
    cursor.execute(
        insert_series_info_sql,
        (
            info["series_id"] if info["series_id"] else series_id,
            info["series_name"] if info["series_name"] else row["series_name"],
            host_countries,
            info["match_type"],
            info["series_start_date"] if info["series_start_date"] else row["archive_start_date"],
            info["series_end_date"] if info["series_end_date"] else row["archive_end_date"],
            info["total_matches_planned"]
        )
    )
    conn.commit()

print("Inserted series_info and series_venues rows")

# =========================================================
# FINAL QUERY OUTPUT
# =========================================================
print("\nAnswer to the question:\n")

cursor.execute("""
SELECT
    si.series_name,
    si.host_countries,
    si.match_type,
    si.series_start_date,
    si.total_matches_planned
FROM series_info si
ORDER BY si.series_start_date DESC, si.series_name
""")

rows = cursor.fetchall()
for r in rows:
    print(r)

cursor.close()
conn.close()