'''
Cricbuzz API (series points-table endpoint)
        ↓
Fetch JSON payload using requests
        ↓
Extract groups from pointsTable
        ↓
Extract team statistics (team_id, team_name, matches, wins, losses, points, NRR)
        ↓
Transform fields into SQL row format
        ↓
Connect to MySQL (cricket_db)
        ↓
Create table series_points_table if not exists
        ↓
Insert / Update rows (upsert using primary key)
        ↓
Store series points table data in database
'''

import requests
import mysql.connector
from mysql.connector import Error

# -----------------------------
# CONFIG
# -----------------------------
SERIES_ID = 11253
URL = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/series/{SERIES_ID}/points-table"

RAPID_HEADERS = {
    "x-rapidapi-key": "",
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "cricket_db"
}

# -----------------------------
# HELPERS
# -----------------------------
def safe_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default

def safe_str(x, default=""):
    return default if x is None else str(x)

# -----------------------------
# MAIN
# -----------------------------
def main():
    # 1) Fetch payload
    resp = requests.get(URL, headers=RAPID_HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    points_table = data.get("pointsTable", [])
    if not points_table:
        print("No pointsTable found in response.")
        return

    conn = None
    cursor = None

    try:
        # 2) Connect MySQL
        conn = mysql.connector.connect(
            host=DB_CONFIG["host"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"]
        )
        cursor = conn.cursor()

        # 3) Create DB if not exists
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
        cursor.execute(f"USE {DB_CONFIG['database']}")

        # 4) Create table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS series_points_table (
            series_id INT NOT NULL,
            group_name VARCHAR(100) NOT NULL,
            team_id INT NOT NULL,
            team_short_name VARCHAR(20),
            team_full_name VARCHAR(100),
            matches_played INT DEFAULT 0,
            matches_won INT DEFAULT 0,
            matches_lost INT DEFAULT 0,
            no_result INT DEFAULT 0,
            points INT DEFAULT 0,
            nrr VARCHAR(20),
            qualify_status VARCHAR(10),
            team_image_id INT,
            PRIMARY KEY (series_id, group_name, team_id)
        )
        """)

        # 5) Insert / Update rows
        insert_sql = """
        INSERT INTO series_points_table (
            series_id, group_name, team_id, team_short_name, team_full_name,
            matches_played, matches_won, matches_lost, no_result,
            points, nrr, qualify_status, team_image_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            team_short_name = VALUES(team_short_name),
            team_full_name = VALUES(team_full_name),
            matches_played = VALUES(matches_played),
            matches_won = VALUES(matches_won),
            matches_lost = VALUES(matches_lost),
            no_result = VALUES(no_result),
            points = VALUES(points),
            nrr = VALUES(nrr),
            qualify_status = VALUES(qualify_status),
            team_image_id = VALUES(team_image_id)
        """

        row_count = 0
        for group in points_table:
            group_name = safe_str(group.get("groupName", "Unknown Group"))
            teams = group.get("pointsTableInfo", [])
            for t in teams:
                team_id = safe_int(t.get("teamId"))
                team_short = safe_str(t.get("teamName", ""))
                team_full = safe_str(t.get("teamFullName", ""))

                matches_played = safe_int(t.get("matchesPlayed", 0))
                matches_won = safe_int(t.get("matchesWon", 0))
                matches_lost = safe_int(t.get("matchesLost", 0))
                no_result = safe_int(t.get("noRes", 0))

                points = safe_int(t.get("points", 0))
                nrr = safe_str(t.get("nrr", ""))
                qualify_status = safe_str(t.get("teamQualifyStatus", ""))
                team_image_id = safe_int(t.get("teamImageId", 0))

                cursor.execute(insert_sql, (
                    SERIES_ID, group_name, team_id, team_short, team_full,
                    matches_played, matches_won, matches_lost, no_result,
                    points, nrr, qualify_status, team_image_id
                ))
                row_count += 1

        conn.commit()
        print(f"Inserted/Updated {row_count} rows into series_points_table.")

    except Error as e:
        print("MySQL Error:", e)
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    main()