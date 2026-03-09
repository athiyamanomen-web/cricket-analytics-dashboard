'''
Connect to MySQL (cricket_db)
        ↓
Create batting_scorecard and bowling_scorecard tables
        ↓
Read completed match_ids from series_matches
        ↓
For each match_id:
        ↓
Check if scorecard JSON exists in local cache
        ↓
If cached → load JSON
If not cached → call Cricbuzz Scorecard API
        ↓
Save raw JSON to cache
        ↓
Read innings list from scorecard payload
        ↓
Extract batting statistics
        ↓
Extract bowling statistics
        ↓
Insert rows into batting_scorecard and bowling_scorecard tables
        ↓
Commit after each match
        ↓
Print pipeline summary
'''

# FILLING SCORECARD USING MATCH ID
import requests
import mysql.connector
import time
import os
import json

# ============================================================
# PIPELINE
# 1) Connect to MySQL
# 2) Create batting_scorecard and bowling_scorecard tables
# 3) Read completed match_ids from series_matches
# 4) For each match_id, check if raw JSON already exists in local cache
# 5) If cache exists, load JSON from file
# 6) If cache does not exist, call API once and save raw JSON file
# 7) Read innings list from data["scorecard"]
# 8) Extract batting rows from innings["batsman"]
# 9) Extract bowling rows from innings["bowler"]
# 10) Insert rows into DB
# 11) Commit after each match
# 12) Print final summary
# ============================================================

# =========================
# CONFIG
# =========================
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "cricket_db"
}

RAPIDAPI_KEY = ""
RAPIDAPI_HOST = "cricbuzz-cricket.p.rapidapi.com"

HEADERS = {
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": RAPIDAPI_HOST
}

SCORECARD_URL = "https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{match_id}/scard"

CACHE_DIR = "raw_scorecards"


# =========================
# HELPERS
# =========================
def safe_int(val, default=0):
    try:
        if val is None or val == "":
            return default
        return int(float(val))
    except:
        return default


def safe_float(val, default=0.0):
    try:
        if val is None or val == "":
            return default
        return float(val)
    except:
        return default


def safe_bool(val):
    return 1 if val is True else 0


def ensure_cache_dir():
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)


def get_cache_path(match_id):
    return os.path.join(CACHE_DIR, f"{match_id}.json")


def load_or_fetch_scorecard(match_id):
    """
    Returns:
        data, source
    where source is either 'cache' or 'api'
    """
    cache_path = get_cache_path(match_id)

    # =========================
    # PIPELINE STEP 4 + 5:
    # LOAD FROM CACHE IF FILE EXISTS
    # =========================
    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data, "cache"

    # =========================
    # PIPELINE STEP 6:
    # CALL API AND SAVE RAW JSON
    # =========================
    url = SCORECARD_URL.format(match_id=match_id)
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()

    data = response.json()

    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return data, "api"


# =========================
# PIPELINE STEP 1:
# CONNECT TO MYSQL
# =========================
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor(dictionary=True)

ensure_cache_dir()


# =========================
# PIPELINE STEP 2:
# CREATE TABLES
# =========================
create_batting_table_sql = """
CREATE TABLE IF NOT EXISTS batting_scorecard (
    id INT AUTO_INCREMENT PRIMARY KEY,
    match_id BIGINT NOT NULL,
    innings_id INT NOT NULL,
    innings_no INT NOT NULL,
    batting_team VARCHAR(255),
    batting_team_short VARCHAR(50),
    player_id BIGINT NOT NULL,
    batsman_name VARCHAR(255),
    nickname VARCHAR(255),
    is_captain TINYINT(1),
    is_keeper TINYINT(1),
    out_desc TEXT,
    runs INT,
    balls INT,
    fours INT,
    sixes INT,
    strike_rate DECIMAL(10,2),
    UNIQUE KEY uq_batting_match_innings_player (match_id, innings_id, player_id)
)
"""

create_bowling_table_sql = """
CREATE TABLE IF NOT EXISTS bowling_scorecard (
    id INT AUTO_INCREMENT PRIMARY KEY,
    match_id BIGINT NOT NULL,
    innings_id INT NOT NULL,
    innings_no INT NOT NULL,
    batting_team VARCHAR(255),
    batting_team_short VARCHAR(50),
    player_id BIGINT NOT NULL,
    bowler_name VARCHAR(255),
    nickname VARCHAR(255),
    is_captain TINYINT(1),
    is_keeper TINYINT(1),
    overs DECIMAL(10,1),
    maidens INT,
    runs_conceded INT,
    wickets INT,
    economy DECIMAL(10,2),
    balls INT,
    dots INT,
    UNIQUE KEY uq_bowling_match_innings_player (match_id, innings_id, player_id)
)
"""

cursor.execute(create_batting_table_sql)
cursor.execute(create_bowling_table_sql)
conn.commit()


# =========================
# PIPELINE STEP 3:
# READ MATCH_IDS
# =========================
get_matches_sql = """
SELECT match_id, series_name, match_format, start_date
FROM series_matches
WHERE state = 'Complete'
ORDER BY start_date DESC
LIMIT 20
"""

cursor.execute(get_matches_sql)
matches = cursor.fetchall()

print(f"Fetched {len(matches)} completed match_ids")

if not matches:
    print("No completed matches found.")
    cursor.close()
    conn.close()
    raise SystemExit


# =========================
# INSERT SQL
# =========================
insert_batting_sql = """
INSERT INTO batting_scorecard (
    match_id, innings_id, innings_no, batting_team, batting_team_short,
    player_id, batsman_name, nickname, is_captain, is_keeper,
    out_desc, runs, balls, fours, sixes, strike_rate
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    batting_team = VALUES(batting_team),
    batting_team_short = VALUES(batting_team_short),
    batsman_name = VALUES(batsman_name),
    nickname = VALUES(nickname),
    is_captain = VALUES(is_captain),
    is_keeper = VALUES(is_keeper),
    out_desc = VALUES(out_desc),
    runs = VALUES(runs),
    balls = VALUES(balls),
    fours = VALUES(fours),
    sixes = VALUES(sixes),
    strike_rate = VALUES(strike_rate)
"""

insert_bowling_sql = """
INSERT INTO bowling_scorecard (
    match_id, innings_id, innings_no, batting_team, batting_team_short,
    player_id, bowler_name, nickname, is_captain, is_keeper,
    overs, maidens, runs_conceded, wickets, economy, balls, dots
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    batting_team = VALUES(batting_team),
    batting_team_short = VALUES(batting_team_short),
    bowler_name = VALUES(bowler_name),
    nickname = VALUES(nickname),
    is_captain = VALUES(is_captain),
    is_keeper = VALUES(is_keeper),
    overs = VALUES(overs),
    maidens = VALUES(maidens),
    runs_conceded = VALUES(runs_conceded),
    wickets = VALUES(wickets),
    economy = VALUES(economy),
    balls = VALUES(balls),
    dots = VALUES(dots)
"""


# =========================
# PIPELINE STEP 4 TO 11:
# PROCESS EACH MATCH
# =========================
api_success = 0
api_failed = 0
cache_hits = 0
batting_rows = 0
bowling_rows = 0

for idx, match in enumerate(matches, start=1):
    match_id = match["match_id"]
    print(f"\n[{idx}/{len(matches)}] Processing match_id={match_id}")

    try:
        data, source = load_or_fetch_scorecard(match_id)

        if source == "cache":
            cache_hits += 1
            print("  Loaded from cache")
        else:
            api_success += 1
            print("  Fetched from API and saved to cache")

    except Exception as e:
        print(f"  Failed to load/fetch match_id={match_id}: {e}")
        api_failed += 1
        continue

    # =========================
    # PIPELINE STEP 7:
    # READ INNINGS LIST
    # =========================
    scorecards = data.get("scorecard", [])

    if not isinstance(scorecards, list) or not scorecards:
        print(f"  No scorecard found for match_id={match_id}")
        continue

    match_batting_count = 0
    match_bowling_count = 0

    for innings_no, innings in enumerate(scorecards, start=1):
        innings_id = safe_int(innings.get("inningsid"))
        batting_team = innings.get("batteamname", "")
        batting_team_short = innings.get("batteamsname", "")

        # =========================
        # PIPELINE STEP 8:
        # EXTRACT BATTING
        # =========================
        batsmen = innings.get("batsman", [])

        if isinstance(batsmen, list):
            for b in batsmen:
                if not isinstance(b, dict):
                    continue

                player_id = safe_int(b.get("id"), None)
                if player_id is None:
                    continue

                batting_values = (
                    match_id,
                    innings_id,
                    innings_no,
                    batting_team,
                    batting_team_short,
                    player_id,
                    b.get("name", ""),
                    b.get("nickname", ""),
                    safe_bool(b.get("iscaptain")),
                    safe_bool(b.get("iskeeper")),
                    b.get("outdec", ""),
                    safe_int(b.get("runs")),
                    safe_int(b.get("balls")),
                    safe_int(b.get("fours")),
                    safe_int(b.get("sixes")),
                    safe_float(b.get("strkrate"))
                )

                try:
                    cursor.execute(insert_batting_sql, batting_values)
                    match_batting_count += 1
                except Exception as e:
                    print(f"  Batting insert failed | match_id={match_id} | player={b.get('name', '')} | {e}")

        # =========================
        # PIPELINE STEP 9:
        # EXTRACT BOWLING
        # =========================
        bowlers = innings.get("bowler", [])

        if isinstance(bowlers, list):
            for bw in bowlers:
                if not isinstance(bw, dict):
                    continue

                player_id = safe_int(bw.get("id"), None)
                if player_id is None:
                    continue

                bowling_values = (
                    match_id,
                    innings_id,
                    innings_no,
                    batting_team,
                    batting_team_short,
                    player_id,
                    bw.get("name", ""),
                    bw.get("nickname", ""),
                    safe_bool(bw.get("iscaptain")),
                    safe_bool(bw.get("iskeeper")),
                    safe_float(bw.get("overs")),
                    safe_int(bw.get("maidens")),
                    safe_int(bw.get("runs")),
                    safe_int(bw.get("wickets")),
                    safe_float(bw.get("economy")),
                    safe_int(bw.get("balls")),
                    safe_int(bw.get("dots"))
                )

                try:
                    cursor.execute(insert_bowling_sql, bowling_values)
                    match_bowling_count += 1
                except Exception as e:
                    print(f"  Bowling insert failed | match_id={match_id} | player={bw.get('name', '')} | {e}")

    # =========================
    # PIPELINE STEP 10 + 11:
    # COMMIT
    # =========================
    conn.commit()

    batting_rows += match_batting_count
    bowling_rows += match_bowling_count

    print(f"  Batting rows processed: {match_batting_count}")
    print(f"  Bowling rows processed: {match_bowling_count}")

    time.sleep(1)


# =========================
# PIPELINE STEP 12:
# FINAL SUMMARY
# =========================
print("\n========== PIPELINE SUMMARY ==========")
print(f"Matches selected      : {len(matches)}")
print(f"Fetched from API      : {api_success}")
print(f"Loaded from cache     : {cache_hits}")
print(f"Load/API failed       : {api_failed}")
print(f"Batting rows processed: {batting_rows}")
print(f"Bowling rows processed: {bowling_rows}")

cursor.close()
conn.close()