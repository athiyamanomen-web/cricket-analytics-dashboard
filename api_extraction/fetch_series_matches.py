'''
Cricbuzz API (Series Archives 2024)
        ↓
Fetch archive payload
        ↓
Filter bilateral series
        ↓
Extract first 20 series IDs
        ↓
For each series_id:
        ↓
Call Series Details API
        ↓
Extract match information (teams, venue, match format, dates, scores)
        ↓
Transform fields into SQL row format
        ↓
Connect to MySQL (cricket_db)
        ↓
Create tables: series_list and series_matches if not exists
        ↓
Insert / Update series list
        ↓
Insert / Update match records for each series
        ↓
Store series match data in database
        ↓
Run SQL query to show series-wise match counts
'''

#12 series matches  fetched using series id and stored in my sql
import requests
import mysql.connector
from datetime import datetime

# =========================================================
# CONFIG
# =========================================================
ARCHIVE_URL = "https://cricbuzz-cricket.p.rapidapi.com/series/v1/archives/international?year=2024"
SERIES_URL_TEMPLATE = "https://cricbuzz-cricket.p.rapidapi.com/series/v1/{series_id}"

HEADERS = {
    "x-rapidapi-key": "x",
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
MYSQL_DATABASE = "cricket_db"

# =========================================================
# HELPERS
# =========================================================
def fetch_json(url):
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()

def ms_to_date(ms):
    if not ms:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000).strftime("%Y-%m-%d")
    except Exception:
        return None

def ms_to_datetime(ms):
    if not ms:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def is_bilateral(series_name):
    if not series_name:
        return False
    name = series_name.lower().strip()
    return ("tour of" in name) or (" v " in name) or (" vs " in name)

# =========================================================
# STEP 1: FETCH 2024 ARCHIVE SERIES
# =========================================================
archive_payload = fetch_json(ARCHIVE_URL)

series_rows = []
for block in archive_payload.get("seriesMapProto", []):
    for s in block.get("series", []):
        series_name = s.get("name", "")
        if is_bilateral(series_name):
            series_rows.append((
                s.get("id"),
                series_name,
                ms_to_date(s.get("startDt")),
                ms_to_date(s.get("endDt")),
                s.get("thumborImageId")
            ))

# take first 20 bilateral series
series_rows = series_rows[:20]

print(f"Fetched {len(series_rows)} bilateral series")

# =========================================================
# STEP 2: MYSQL CONNECTION
# =========================================================
conn = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD
)
cursor = conn.cursor()

cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DATABASE}")
cursor.execute(f"USE {MYSQL_DATABASE}")

# =========================================================
# STEP 3: CREATE / ALIGN TABLES
# =========================================================
# Existing series_list schema is kept as:
# series_id, series_name, archive_start_date, archive_end_date, image_id

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
CREATE TABLE IF NOT EXISTS series_matches (
    match_id BIGINT PRIMARY KEY,
    series_id INT,
    series_name VARCHAR(255),
    match_desc VARCHAR(255),
    match_format VARCHAR(50),
    start_date DATETIME,
    end_date DATETIME,
    state VARCHAR(50),
    status TEXT,

    team1_id BIGINT,
    team1_name VARCHAR(150),
    team1_short_name VARCHAR(50),
    team1_image_id BIGINT,

    team2_id BIGINT,
    team2_name VARCHAR(150),
    team2_short_name VARCHAR(50),
    team2_image_id BIGINT,

    venue_id BIGINT,
    venue_name VARCHAR(255),
    venue_city VARCHAR(100),
    venue_timezone VARCHAR(20),

    curr_bat_team_id BIGINT,
    series_start_dt DATETIME,
    series_end_dt DATETIME,
    is_time_announced BOOLEAN,

    team1_inngs1_runs INT,
    team1_inngs1_wickets INT,
    team1_inngs1_overs DECIMAL(6,1),

    team1_inngs2_runs INT,
    team1_inngs2_wickets INT,
    team1_inngs2_overs DECIMAL(6,1),

    team2_inngs1_runs INT,
    team2_inngs1_wickets INT,
    team2_inngs1_overs DECIMAL(6,1),

    team2_inngs2_runs INT,
    team2_inngs2_wickets INT,
    team2_inngs2_overs DECIMAL(6,1),

    CONSTRAINT fk_series_matches_series
        FOREIGN KEY (series_id) REFERENCES series_list(series_id)
)
""")

# =========================================================
# STEP 4: INSERT INTO series_list
# =========================================================
insert_series_sql = """
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

cursor.executemany(insert_series_sql, series_rows)
conn.commit()

print("Inserted series_list")

# =========================================================
# STEP 5: FETCH MATCHES FOR EACH SERIES AND STORE
# =========================================================
insert_match_sql = """
INSERT INTO series_matches (
    match_id, series_id, series_name, match_desc, match_format,
    start_date, end_date, state, status,

    team1_id, team1_name, team1_short_name, team1_image_id,
    team2_id, team2_name, team2_short_name, team2_image_id,

    venue_id, venue_name, venue_city, venue_timezone,

    curr_bat_team_id, series_start_dt, series_end_dt, is_time_announced,

    team1_inngs1_runs, team1_inngs1_wickets, team1_inngs1_overs,
    team1_inngs2_runs, team1_inngs2_wickets, team1_inngs2_overs,
    team2_inngs1_runs, team2_inngs1_wickets, team2_inngs1_overs,
    team2_inngs2_runs, team2_inngs2_wickets, team2_inngs2_overs
)
VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s,

    %s, %s, %s, %s,
    %s, %s, %s, %s,

    %s, %s, %s, %s,

    %s, %s, %s, %s,

    %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s
)
ON DUPLICATE KEY UPDATE
    series_id = VALUES(series_id),
    series_name = VALUES(series_name),
    match_desc = VALUES(match_desc),
    match_format = VALUES(match_format),
    start_date = VALUES(start_date),
    end_date = VALUES(end_date),
    state = VALUES(state),
    status = VALUES(status),

    team1_id = VALUES(team1_id),
    team1_name = VALUES(team1_name),
    team1_short_name = VALUES(team1_short_name),
    team1_image_id = VALUES(team1_image_id),

    team2_id = VALUES(team2_id),
    team2_name = VALUES(team2_name),
    team2_short_name = VALUES(team2_short_name),
    team2_image_id = VALUES(team2_image_id),

    venue_id = VALUES(venue_id),
    venue_name = VALUES(venue_name),
    venue_city = VALUES(venue_city),
    venue_timezone = VALUES(venue_timezone),

    curr_bat_team_id = VALUES(curr_bat_team_id),
    series_start_dt = VALUES(series_start_dt),
    series_end_dt = VALUES(series_end_dt),
    is_time_announced = VALUES(is_time_announced),

    team1_inngs1_runs = VALUES(team1_inngs1_runs),
    team1_inngs1_wickets = VALUES(team1_inngs1_wickets),
    team1_inngs1_overs = VALUES(team1_inngs1_overs),

    team1_inngs2_runs = VALUES(team1_inngs2_runs),
    team1_inngs2_wickets = VALUES(team1_inngs2_wickets),
    team1_inngs2_overs = VALUES(team1_inngs2_overs),

    team2_inngs1_runs = VALUES(team2_inngs1_runs),
    team2_inngs1_wickets = VALUES(team2_inngs1_wickets),
    team2_inngs1_overs = VALUES(team2_inngs1_overs),

    team2_inngs2_runs = VALUES(team2_inngs2_runs),
    team2_inngs2_wickets = VALUES(team2_inngs2_wickets),
    team2_inngs2_overs = VALUES(team2_inngs2_overs)
"""

for s in series_rows:
    series_id = s[0]
    series_name = s[1]

    print(f"Processing series_id={series_id} | {series_name}")

    try:
        detail_payload = fetch_json(SERIES_URL_TEMPLATE.format(series_id=series_id))
        match_values = []

        for item in detail_payload.get("matchDetails", []):
            match_map = item.get("matchDetailsMap")
            if not match_map:
                continue

            for match in match_map.get("match", []):
                info = match.get("matchInfo", {})
                if not info:
                    continue

                team1 = info.get("team1", {})
                team2 = info.get("team2", {})
                venue = info.get("venueInfo", {})
                score = match.get("matchScore", {})

                team1_score = score.get("team1Score", {})
                team2_score = score.get("team2Score", {})

                t1_i1 = team1_score.get("inngs1", {})
                t1_i2 = team1_score.get("inngs2", {})
                t2_i1 = team2_score.get("inngs1", {})
                t2_i2 = team2_score.get("inngs2", {})

                match_values.append((
                    info.get("matchId"),
                    info.get("seriesId"),
                    info.get("seriesName"),
                    info.get("matchDesc"),
                    info.get("matchFormat"),

                    ms_to_datetime(info.get("startDate")),
                    ms_to_datetime(info.get("endDate")),
                    info.get("state"),
                    info.get("status"),

                    team1.get("teamId"),
                    team1.get("teamName"),
                    team1.get("teamSName"),
                    team1.get("imageId"),

                    team2.get("teamId"),
                    team2.get("teamName"),
                    team2.get("teamSName"),
                    team2.get("imageId"),

                    venue.get("id"),
                    venue.get("ground"),
                    venue.get("city"),
                    venue.get("timezone"),

                    info.get("currBatTeamId"),
                    ms_to_datetime(info.get("seriesStartDt")),
                    ms_to_datetime(info.get("seriesEndDt")),
                    info.get("isTimeAnnounced"),

                    t1_i1.get("runs"),
                    t1_i1.get("wickets"),
                    t1_i1.get("overs"),

                    t1_i2.get("runs"),
                    t1_i2.get("wickets"),
                    t1_i2.get("overs"),

                    t2_i1.get("runs"),
                    t2_i1.get("wickets"),
                    t2_i1.get("overs"),

                    t2_i2.get("runs"),
                    t2_i2.get("wickets"),
                    t2_i2.get("overs")
                ))

        if match_values:
            cursor.executemany(insert_match_sql, match_values)
            conn.commit()
            print(f"  Inserted {len(match_values)} matches")
        else:
            print("  No matches found")

    except Exception as e:
        print(f"  Match fetch failed for {series_id}: {e}")

print("Inserted series_matches")

# =========================================================
# STEP 6: CHECK OUTPUT
# =========================================================
cursor.execute("""
SELECT
    series_id,
    series_name,
    COUNT(*) AS total_matches
FROM series_matches
GROUP BY series_id, series_name
ORDER BY series_id
""")

rows = cursor.fetchall()

print("\nSeries-wise match counts:\n")
for r in rows:
    print(r)

cursor.close()
conn.close()