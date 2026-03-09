'''
Connect to MySQL (cricket_db)
        ↓
Create matches_leanback table if not exists
        ↓
Read completed match_ids from series_matches
        ↓
For each match_id:
        ↓
Call Cricbuzz Leanback API
        ↓
Save raw leanback JSON locally
        ↓
Extract toss, winner, team, series, and status details
        ↓
Insert new row or update existing row in matches_leanback
        ↓
Commit after each match
        ↓
Print progress and final summary
'''
# creating matches-leanback table to get toss results and status to create toss win ? match win %
import os
import json
import time
import requests
import mysql.connector

# ---------------------------------
# CONFIG
# ---------------------------------
API_KEY = "x"
API_HOST = "cricbuzz-cricket.p.rapidapi.com"

HEADERS = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": API_HOST
}

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "cricket_db"
}

SAVE_DIR = "leanback_json"
os.makedirs(SAVE_DIR, exist_ok=True)

# how many matches to test first
LIMIT_MATCHES = 20

# ---------------------------------
# MYSQL TABLE
# ---------------------------------
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS matches_leanback (
    match_id BIGINT PRIMARY KEY,
    series_id BIGINT,
    series_name VARCHAR(255),
    match_desc VARCHAR(255),
    match_format VARCHAR(50),
    match_start_timestamp BIGINT,
    match_end_timestamp BIGINT,
    state VARCHAR(100),
    status TEXT,
    winning_team_id BIGINT,
    toss_winner_id BIGINT,
    toss_winner_name VARCHAR(255),
    toss_decision VARCHAR(50),
    team1_id BIGINT,
    team1_name VARCHAR(255),
    team2_id BIGINT,
    team2_name VARCHAR(255),
    raw_json LONGTEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

INSERT_SQL = """
INSERT INTO matches_leanback (
    match_id,
    series_id,
    series_name,
    match_desc,
    match_format,
    match_start_timestamp,
    match_end_timestamp,
    state,
    status,
    winning_team_id,
    toss_winner_id,
    toss_winner_name,
    toss_decision,
    team1_id,
    team1_name,
    team2_id,
    team2_name,
    raw_json
)
VALUES (
    %(match_id)s,
    %(series_id)s,
    %(series_name)s,
    %(match_desc)s,
    %(match_format)s,
    %(match_start_timestamp)s,
    %(match_end_timestamp)s,
    %(state)s,
    %(status)s,
    %(winning_team_id)s,
    %(toss_winner_id)s,
    %(toss_winner_name)s,
    %(toss_decision)s,
    %(team1_id)s,
    %(team1_name)s,
    %(team2_id)s,
    %(team2_name)s,
    %(raw_json)s
)
ON DUPLICATE KEY UPDATE
    series_id = VALUES(series_id),
    series_name = VALUES(series_name),
    match_desc = VALUES(match_desc),
    match_format = VALUES(match_format),
    match_start_timestamp = VALUES(match_start_timestamp),
    match_end_timestamp = VALUES(match_end_timestamp),
    state = VALUES(state),
    status = VALUES(status),
    winning_team_id = VALUES(winning_team_id),
    toss_winner_id = VALUES(toss_winner_id),
    toss_winner_name = VALUES(toss_winner_name),
    toss_decision = VALUES(toss_decision),
    team1_id = VALUES(team1_id),
    team1_name = VALUES(team1_name),
    team2_id = VALUES(team2_id),
    team2_name = VALUES(team2_name),
    raw_json = VALUES(raw_json)
"""

# ---------------------------------
# HELPERS
# ---------------------------------
def get_match_ids_from_series_matches(conn, limit_n=20):
    query = """
    SELECT match_id
    FROM series_matches
    WHERE state = 'Complete'
    ORDER BY start_date DESC
    LIMIT %s
    """
    cur = conn.cursor()
    cur.execute(query, (limit_n,))
    rows = cur.fetchall()
    cur.close()
    return [row[0] for row in rows]


def fetch_leanback(match_id):
    url = f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{match_id}/leanback"
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()


def save_payload_json(match_id, payload):
    file_path = os.path.join(SAVE_DIR, f"leanback_{match_id}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return file_path


def safe_get(d, *keys):
    cur = d
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
        if cur is None:
            return None
    return cur


def parse_leanback_payload(payload, match_id):
    mh = payload.get("matchheaders", {}) or {}
    toss = mh.get("tossresults", {}) or {}
    team1 = mh.get("team1", {}) or {}
    team2 = mh.get("team2", {}) or {}

    return {
        "match_id": match_id,
        "series_id": mh.get("seriesid"),
        "series_name": mh.get("seriesname"),
        "match_desc": mh.get("matchdesc"),
        "match_format": mh.get("matchformat"),
        "match_start_timestamp": mh.get("matchstarttimestamp"),
        "match_end_timestamp": mh.get("matchendtimestamp"),
        "state": mh.get("state"),
        "status": mh.get("status"),
        "winning_team_id": mh.get("winningteamid"),
        "toss_winner_id": toss.get("tosswinnerid") if toss.get("tosswinnerid") not in (0, "", None) else None,
        "toss_winner_name": toss.get("tosswinnername") if toss.get("tosswinnername") not in ("", None) else None,
        "toss_decision": toss.get("decision") if toss.get("decision") not in ("", None) else None,
        "team1_id": team1.get("teamid"),
        "team1_name": team1.get("teamname"),
        "team2_id": team2.get("teamid"),
        "team2_name": team2.get("teamname"),
        "raw_json": json.dumps(payload, ensure_ascii=False)
    }


def insert_row(conn, row):
    cur = conn.cursor()
    cur.execute(INSERT_SQL, row)
    conn.commit()
    cur.close()


# ---------------------------------
# MAIN
# ---------------------------------
def main():
    db = mysql.connector.connect(**DB_CONFIG)

    cur = db.cursor()
    cur.execute(CREATE_TABLE_SQL)
    db.commit()
    cur.close()

    match_ids = get_match_ids_from_series_matches(db, LIMIT_MATCHES)
    print(f"Found {len(match_ids)} completed match_ids")

    for i, match_id in enumerate(match_ids, start=1):
        try:
            payload = fetch_leanback(match_id)
            json_path = save_payload_json(match_id, payload)
            row = parse_leanback_payload(payload, match_id)
            insert_row(db, row)

            print(f"[{i}/{len(match_ids)}] Done match_id={match_id}")
            print(f"   JSON saved: {json_path}")
            print(f"   Toss winner id: {row['toss_winner_id']}")
            print(f"   Toss winner name: {row['toss_winner_name']}")
            print(f"   Toss decision: {row['toss_decision']}")
            print(f"   Winning team id: {row['winning_team_id']}")
            print("-" * 60)

            time.sleep(1)  # small delay to avoid rate limit spikes

        except Exception as e:
            print(f"[{i}/{len(match_ids)}] Failed match_id={match_id}: {e}")
            print("-" * 60)

    db.close()
    print("Finished.")

if __name__ == "__main__":
    main()