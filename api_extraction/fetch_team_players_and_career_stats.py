'''
Cricbuzz API (team players endpoint)
        ↓
Fetch all players for Team India
        ↓
Extract unique player IDs
        ↓
Connect to MySQL (cricket_db)
        ↓
Create player-related tables if not exists
(players, team_players, raw_endpoints, batting summary, bowling summary)
        ↓
For each player:
        ↓
Call Player Profile API
        ↓
Store structured profile + raw JSON
        ↓
Call Player Batting API
        ↓
Pivot batting matrix into format-wise stats
        ↓
Store batting career summary + raw JSON
        ↓
Call Player Bowling API
        ↓
Pivot bowling matrix into format-wise stats
        ↓
Store bowling career summary + raw JSON
        ↓
Commit changes for each player
        ↓
Print final success message
'''

import json
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
import mysql.connector
from mysql.connector import Error

# -----------------------------
# CONFIG
# -----------------------------
TEAM_ID = 2
TEAM_NAME = "India"

TEAM_PLAYERS_URL = f"https://cricbuzz-cricket.p.rapidapi.com/teams/v1/{TEAM_ID}/players"
PLAYER_INFO_URL_TPL = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player_id}"
BATTING_URL_TPL = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player_id}/batting"
BOWLING_URL_TPL = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player_id}/bowling"

RAPID_HEADERS = {
    "x-rapidapi-key": "x",
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com",
}

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "cricket_db",
}

SLEEP_BETWEEN_CALLS_SEC = 0.35


# -----------------------------
# HELPERS
# -----------------------------
def as_text(x) -> Optional[str]:
    return None if x is None else str(x)

def as_int(x) -> Optional[int]:
    try:
        s = str(x).strip()
        if s == "" or s.upper() in {"-", "NA", "N/A"}:
            return None
        # remove stars like "119*"
        s = s.replace("*", "")
        return int(s)
    except Exception:
        return None

def as_float(x) -> Optional[float]:
    try:
        s = str(x).strip()
        if s == "" or s.upper() in {"-", "NA", "N/A"}:
            return None
        return float(s)
    except Exception:
        return None

def json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)

def request_json(url: str) -> Dict[str, Any]:
    r = requests.get(url, headers=RAPID_HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


# -----------------------------
# TEAM PLAYERS PARSER (based on  payload)
# payload["player"] is a list where some rows are section headers without "id"
# -----------------------------
def extract_player_ids(team_players_payload: Dict[str, Any]) -> List[Tuple[int, str]]:
    players = team_players_payload.get("player")
    if not isinstance(players, list):
        return []

    res: List[Tuple[int, str]] = []
    seen = set()

    for item in players:
        if not isinstance(item, dict):
            continue

        pid = item.get("id")
        name = item.get("name")

        # skip section headers like {"name":"BATSMEN","imageId":...} (no id)
        if pid is None or name is None:
            continue

        pid_int = as_int(pid)
        if pid_int is None:
            continue

        if pid_int not in seen:
            seen.add(pid_int)
            res.append((pid_int, str(name)))

    return res


# -----------------------------
# PLAYER PROFILE PARSER (based on your sample payload)
# -----------------------------
def parse_player_profile(player_payload: Dict[str, Any]) -> Dict[str, Any]:
    # use DoBFormat if available; else DoB; else dob/dateOfBirth
    dob_text = (
        player_payload.get("DoBFormat")
        or player_payload.get("DoB")
        or player_payload.get("dob")
        or player_payload.get("dateOfBirth")
    )

    # image can be URL; some endpoints also have imageId/faceImageId
    image_url = player_payload.get("image")

    return {
        "player_id": as_int(player_payload.get("id") or player_payload.get("playerId")),
        "name": as_text(player_payload.get("name")),
        "nick_name": as_text(player_payload.get("nickName")),
        "bat_style": as_text(player_payload.get("bat")),
        "bowl_style": as_text(player_payload.get("bowl")),
        "role": as_text(player_payload.get("role")),
        "birth_place": as_text(player_payload.get("birthPlace")),
        "dob_text": as_text(dob_text),
        "country": as_text(player_payload.get("intlTeam")) or TEAM_NAME,  # best "country"
        "intl_team": as_text(player_payload.get("intlTeam")),
        "teams": as_text(player_payload.get("teams")),
        "image_id": as_int(player_payload.get("faceImageId") or player_payload.get("imageId")),
        "image_url": as_text(image_url),
    }


# -----------------------------
# MATRIX PIVOT PARSER (works for BOTH batting & bowling payload you pasted)
# headers: ["ROWHEADER","Test","ODI","T20","IPL"]
# values: [{"values":["Matches","163","189",...]} , ...]
# Output: { "Test": {"Matches":"163", ...}, "ODI": {...}, ... }
# -----------------------------
def pivot_matrix_payload(payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    headers = payload.get("headers")
    values = payload.get("values")

    if not isinstance(headers, list) or not isinstance(values, list):
        return {}

    hdr = [str(h).strip() if h is not None else "" for h in headers]
    if len(hdr) < 2:
        return {}

    formats = [h for h in hdr[1:] if h]
    if not formats:
        return {}

    out: Dict[str, Dict[str, Any]] = {fmt: {} for fmt in formats}

    for row_obj in values:
        # your payload uses dict rows: {"values":[...]}
        row = row_obj.get("values") if isinstance(row_obj, dict) else row_obj
        if not isinstance(row, list) or len(row) < 2:
            continue

        stat_name = str(row[0]).strip()
        if not stat_name:
            continue

        for i, fmt in enumerate(formats, start=1):
            out[fmt][stat_name] = row[i] if i < len(row) else None

    return out


# -----------------------------
# DB SETUP
# -----------------------------
CREATE_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS players (
        player_id INT PRIMARY KEY,
        name VARCHAR(120),
        nick_name VARCHAR(120),
        bat_style VARCHAR(120),
        bowl_style VARCHAR(120),
        role VARCHAR(120),
        birth_place VARCHAR(200),
        dob_text VARCHAR(80),
        country VARCHAR(120),
        intl_team VARCHAR(120),
        teams TEXT,
        image_id INT,
        image_url TEXT,
        raw_profile_json LONGTEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS team_players (
        team_id INT NOT NULL,
        player_id INT NOT NULL,
        player_name VARCHAR(120),
        raw_team_player_json LONGTEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (team_id, player_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS player_raw_endpoints (
        player_id INT NOT NULL,
        endpoint VARCHAR(30) NOT NULL,   -- profile | batting | bowling
        raw_json LONGTEXT,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (player_id, endpoint)
    )
    """,
    # Batting: one row per format
    """
    CREATE TABLE IF NOT EXISTS player_batting_career_summary (
        player_id INT NOT NULL,
        format VARCHAR(20) NOT NULL,

        matches INT,
        innings INT,
        runs INT,
        balls INT,
        highest INT,

        average DECIMAL(10,2),
        strike_rate DECIMAL(10,2),

        not_out INT,
        fours INT,
        sixes INT,
        ducks INT,

        fifties INT,
        hundreds INT,
        two_hundreds INT,
        three_hundreds INT,
        four_hundreds INT,

        raw_format_json LONGTEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

        PRIMARY KEY (player_id, format)
    )
    """,
    # Bowling: one row per format (FIXED: maidens + 4w included)
    """
    CREATE TABLE IF NOT EXISTS player_bowling_career_summary (
        player_id INT NOT NULL,
        format VARCHAR(20) NOT NULL,

        matches INT,
        innings INT,
        balls INT,
        runs INT,
        maidens INT,
        wickets INT,

        average DECIMAL(10,2),
        economy DECIMAL(10,2),
        strike_rate DECIMAL(10,2),

        best_innings VARCHAR(20),
        best_match VARCHAR(20),

        four_wkts INT,
        five_wkts INT,
        ten_wkts INT,

        raw_format_json LONGTEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

        PRIMARY KEY (player_id, format)
    )
    """,
]

UPSERT_PLAYER_SQL = """
INSERT INTO players (
    player_id, name, nick_name, bat_style, bowl_style, role,
    birth_place, dob_text, country, intl_team, teams, image_id, image_url, raw_profile_json
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
    name=VALUES(name),
    nick_name=VALUES(nick_name),
    bat_style=VALUES(bat_style),
    bowl_style=VALUES(bowl_style),
    role=VALUES(role),
    birth_place=VALUES(birth_place),
    dob_text=VALUES(dob_text),
    country=VALUES(country),
    intl_team=VALUES(intl_team),
    teams=VALUES(teams),
    image_id=VALUES(image_id),
    image_url=VALUES(image_url),
    raw_profile_json=VALUES(raw_profile_json)
"""

UPSERT_TEAM_PLAYER_SQL = """
INSERT INTO team_players (team_id, player_id, player_name, raw_team_player_json)
VALUES (%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
    player_name=VALUES(player_name),
    raw_team_player_json=VALUES(raw_team_player_json)
"""

UPSERT_RAW_ENDPOINT_SQL = """
INSERT INTO player_raw_endpoints (player_id, endpoint, raw_json)
VALUES (%s,%s,%s)
ON DUPLICATE KEY UPDATE
    raw_json=VALUES(raw_json),
    fetched_at=CURRENT_TIMESTAMP
"""

UPSERT_BATTING_SQL = """
INSERT INTO player_batting_career_summary (
    player_id, format,
    matches, innings, runs, balls, highest,
    average, strike_rate,
    not_out, fours, sixes, ducks,
    fifties, hundreds, two_hundreds, three_hundreds, four_hundreds,
    raw_format_json
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
    matches=VALUES(matches),
    innings=VALUES(innings),
    runs=VALUES(runs),
    balls=VALUES(balls),
    highest=VALUES(highest),
    average=VALUES(average),
    strike_rate=VALUES(strike_rate),
    not_out=VALUES(not_out),
    fours=VALUES(fours),
    sixes=VALUES(sixes),
    ducks=VALUES(ducks),
    fifties=VALUES(fifties),
    hundreds=VALUES(hundreds),
    two_hundreds=VALUES(two_hundreds),
    three_hundreds=VALUES(three_hundreds),
    four_hundreds=VALUES(four_hundreds),
    raw_format_json=VALUES(raw_format_json)
"""

# FIXED bowling UPSERT (maidens + 4w)
UPSERT_BOWLING_SQL = """
INSERT INTO player_bowling_career_summary (
    player_id, format,
    matches, innings, balls, runs, maidens, wickets,
    average, economy, strike_rate,
    best_innings, best_match,
    four_wkts, five_wkts, ten_wkts,
    raw_format_json
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
    matches=VALUES(matches),
    innings=VALUES(innings),
    balls=VALUES(balls),
    runs=VALUES(runs),
    maidens=VALUES(maidens),
    wickets=VALUES(wickets),
    average=VALUES(average),
    economy=VALUES(economy),
    strike_rate=VALUES(strike_rate),
    best_innings=VALUES(best_innings),
    best_match=VALUES(best_match),
    four_wkts=VALUES(four_wkts),
    five_wkts=VALUES(five_wkts),
    ten_wkts=VALUES(ten_wkts),
    raw_format_json=VALUES(raw_format_json)
"""


# -----------------------------
# MAIN
# -----------------------------
def main():
    conn = None
    cur = None

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor()

        for sql in CREATE_TABLES_SQL:
            cur.execute(sql)
        conn.commit()

        # 1) Fetch team players (1 call)
        team_payload = request_json(TEAM_PLAYERS_URL)
        team_raw = json_dumps(team_payload)

        player_list = extract_player_ids(team_payload)
        if not player_list:
            print("No players found from team payload. Check payload keys.")
            return

        print(f"Found {len(player_list)} unique players for teamId={TEAM_ID}")

        for pid, pname in player_list:
            cur.execute(UPSERT_TEAM_PLAYER_SQL, (TEAM_ID, pid, pname, team_raw))
        conn.commit()

        # 2) For each player: profile + batting + bowling
        for idx, (pid, pname) in enumerate(player_list, start=1):
            print(f"[{idx}/{len(player_list)}] Player {pid} - {pname}")

            # Profile
            prof = request_json(PLAYER_INFO_URL_TPL.format(player_id=pid))
            time.sleep(SLEEP_BETWEEN_CALLS_SEC)

            core = parse_player_profile(prof)
            core["player_id"] = core["player_id"] or pid

            cur.execute(
                UPSERT_PLAYER_SQL,
                (
                    core["player_id"], core["name"], core["nick_name"],
                    core["bat_style"], core["bowl_style"], core["role"],
                    core["birth_place"], core["dob_text"],
                    core["country"], core["intl_team"], core["teams"],
                    core["image_id"], core["image_url"],
                    json_dumps(prof),
                ),
            )
            cur.execute(UPSERT_RAW_ENDPOINT_SQL, (pid, "profile", json_dumps(prof)))
            conn.commit()

            # Batting (raw + structured)
            bat = request_json(BATTING_URL_TPL.format(player_id=pid))
            time.sleep(SLEEP_BETWEEN_CALLS_SEC)
            cur.execute(UPSERT_RAW_ENDPOINT_SQL, (pid, "batting", json_dumps(bat)))
            conn.commit()

            bat_pivot = pivot_matrix_payload(bat)  # {format: {stat: value}}
            for fmt, stats in bat_pivot.items():
                cur.execute(
                    UPSERT_BATTING_SQL,
                    (
                        pid, fmt,
                        as_int(stats.get("Matches")),
                        as_int(stats.get("Innings")),
                        as_int(stats.get("Runs")),
                        as_int(stats.get("Balls")),
                        as_int(stats.get("Highest")),
                        as_float(stats.get("Average")),
                        as_float(stats.get("SR")),
                        as_int(stats.get("Not Out")),
                        as_int(stats.get("Fours")),
                        as_int(stats.get("Sixes")),
                        as_int(stats.get("Ducks")),
                        as_int(stats.get("50s")),
                        as_int(stats.get("100s")),
                        as_int(stats.get("200s")),
                        as_int(stats.get("300s")),
                        as_int(stats.get("400s")),
                        json_dumps(stats),
                    ),
                )
            conn.commit()

            # Bowling (raw + structured) — FIXED for your payload: Avg/Eco/4w/Maidens
            bowl = request_json(BOWLING_URL_TPL.format(player_id=pid))
            time.sleep(SLEEP_BETWEEN_CALLS_SEC)
            cur.execute(UPSERT_RAW_ENDPOINT_SQL, (pid, "bowling", json_dumps(bowl)))
            conn.commit()

            bowl_pivot = pivot_matrix_payload(bowl)
            for fmt, stats in bowl_pivot.items():
                cur.execute(
                    UPSERT_BOWLING_SQL,
                    (
                        pid, fmt,
                        as_int(stats.get("Matches")),
                        as_int(stats.get("Innings")),
                        as_int(stats.get("Balls")),
                        as_int(stats.get("Runs")),
                        as_int(stats.get("Maidens")),
                        as_int(stats.get("Wickets")),
                        as_float(stats.get("Avg")),
                        as_float(stats.get("Eco")),
                        as_float(stats.get("SR")),
                        as_text(stats.get("BBI")),
                        as_text(stats.get("BBM")),
                        as_int(stats.get("4w")),
                        as_int(stats.get("5w")),
                        as_int(stats.get("10w")),
                        json_dumps(stats),
                    ),
                )
            conn.commit()

        print("✅ Done. Saved ALL players with structured batting+bowling + raw JSON backups.")

    except requests.HTTPError as e:
        print("HTTP Error:", e)
        raise
    except Error as e:
        print("MySQL Error:", e)
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()