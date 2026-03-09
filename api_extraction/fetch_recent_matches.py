'''
API endpoint (/matches/v1/recent)
        ↓
requests.get(...)
        ↓
JSON response
        ↓
extract_matches(...)
        ↓
filter last 30 days
        ↓
map fields to SQL columns
        ↓
list of rows
        ↓
connect MySQL
        ↓
create cricket_db if needed
        ↓
create recent_matches table if needed
        ↓
INSERT ... ON DUPLICATE KEY UPDATE
        ↓
commit
        ↓
select latest 20 rows
        ↓
print output

'''

import requests
import mysql.connector

# -----------------------------
# CONFIG
# -----------------------------
API_URL = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/topstats/0?statsType=mostRuns"

.

RAPID_HEADERS = {
    "x-rapidapi-key": "",#Enter the api key
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com",
}

MYSQL_CFG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "cricket_db",
}

TABLE = "odi_most_runs"


def safe_int(x):
    try:
        return int(str(x).replace(",", "").strip())
    except:
        return None


def safe_float(x):
    try:
        return float(str(x).replace(",", "").strip())
    except:
        return None


def extract_rows(payload: dict):
    """
    payload structure:
    headers: ["Batter","M","I","R","Avg"]
    values: [ {"values":[playerId, batter, M, I, R, Avg]}, ... ]
    """
    out = []
    for item in payload.get("values", []):
        arr = item.get("values", [])
        if len(arr) < 6:
            continue

        player_id = safe_int(arr[0])
        batter = str(arr[1]).strip()
        matches = safe_int(arr[2])
        innings = safe_int(arr[3])
        runs = safe_int(arr[4])
        avg = safe_float(arr[5])

        if player_id is None or batter == "":
            continue

        out.append((player_id, batter, matches, innings, runs, avg))
    return out


def main():
    resp = requests.get(API_URL, headers=RAPID_HEADERS, timeout=20)
    data = resp.json()

    if isinstance(data, dict) and data.get("message"):
        raise RuntimeError(f"API error: {data['message']}")

    # Optional: sanity check ODI selected
    selected = (
        data.get("filter", {})
            .get("selectedMatchType", "")
    )
    print("selectedMatchType:", selected)  # expect "odi"

    rows = extract_rows(data)
    print("rows extracted:", len(rows))

    # 2) Connect MySQL
    conn = mysql.connector.connect(
        host=MYSQL_CFG["host"],
        user=MYSQL_CFG["user"],
        password=MYSQL_CFG["password"],
    )
    cur = conn.cursor()

    # 3) Create DB if not exists + use it
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_CFG['database']}")
    cur.execute(f"USE {MYSQL_CFG['database']}")

    # 4) Create table
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
          player_id INT NOT NULL,
          batter VARCHAR(100) NOT NULL,
          matches INT,
          innings INT,
          runs INT,
          avg DECIMAL(6,2),
          PRIMARY KEY (player_id)
        )
    """)

    # 5) Insert (upsert)
    cur.executemany(
        f"""
        INSERT INTO {TABLE} (player_id, batter, matches, innings, runs, avg)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            batter=VALUES(batter),
            matches=VALUES(matches),
            innings=VALUES(innings),
            runs=VALUES(runs),
            avg=VALUES(avg)
        """,
        rows
    )
    conn.commit()

    # 6) SQL query: Top 10 ODI run scorers 
    cur.execute(f"""
        SELECT
            batter AS player_name,
            runs  AS total_runs,
            avg   AS batting_average
        FROM {TABLE}
        ORDER BY runs DESC
        LIMIT 10;
    """)
    result = cur.fetchall()

    print("\nTop 10 ODI run scorers (runs + avg):")
    for r in result:
        print(r)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()