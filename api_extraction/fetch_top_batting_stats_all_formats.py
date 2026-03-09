# 3,7
'''
Cricbuzz API (Top Stats - Most Runs for Test, ODI, T20I)
        ↓
Call API separately for each format (Test / ODI / T20I)
        ↓
Fetch JSON responses
        ↓
Extract player stats (player_id, batter, matches, innings, runs, avg)
        ↓
Clean numeric values
        ↓
Add format label to each row
        ↓
Connect to MySQL (cricket_db)
        ↓
Create table top_batting_stats_runs if not exists
        ↓
Insert / Update rows for all formats
        ↓
Store batting statistics for Test, ODI, and T20I in one table
'''

#### 7  TOP RUNS STATS ALL FORMAT
import requests
import mysql.connector

# -----------------------------
# CONFIG
# -----------------------------
APIS = {
    "Test": "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/topstats/0?statsType=mostRuns&matchType=1",
    "ODI":  "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/topstats/0?statsType=mostRuns&matchType=2",
    "T20I": "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/topstats/0?statsType=mostRuns&matchType=3",
}

RAPID_HEADERS = {
    "x-rapidapi-key": "",
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com",
}

MYSQL_CFG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "cricket_db",
}

TABLE = "top_batting_stats_runs"


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
    Expected payload:
      values: [ {"values":[playerId, batter, M, I, R, Avg]}, ... ]
    Returns tuples: (player_id, batter, matches, innings, runs, avg)
    """
    out = []
    for item in payload.get("values", []):
        arr = item.get("values", [])
        if not isinstance(arr, list) or len(arr) < 6:
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
    conn = mysql.connector.connect(**MYSQL_CFG)
    cur = conn.cursor()

    # Create ONE table for all formats
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
          format VARCHAR(10) NOT NULL,
          player_id INT NOT NULL,
          batter VARCHAR(120) NOT NULL,
          matches INT,
          innings INT,
          runs INT,
          avg DECIMAL(6,2),
          fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (format, player_id)
        )
    """)
    conn.commit()

    # Loop through formats
    for fmt, url in APIS.items():
        print(f"Fetching {fmt} mostRuns...")

        resp = requests.get(url, headers=RAPID_HEADERS, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        rows = extract_rows(data)
        print(f"{fmt}: rows extracted = {len(rows)}")

        # Add format to each row
        rows_with_format = [(fmt, *r) for r in rows]

        # Insert/Upsert into ONE table
        cur.executemany(
            f"""
            INSERT INTO {TABLE}
              (format, player_id, batter, matches, innings, runs, avg)
            VALUES
              (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              batter=VALUES(batter),
              matches=VALUES(matches),
              innings=VALUES(innings),
              runs=VALUES(runs),
              avg=VALUES(avg),
              fetched_at=CURRENT_TIMESTAMP
            """,
            rows_with_format
        )
        conn.commit()

    # Quick check
    cur.execute(f"""
        SELECT format, batter, runs, avg
        FROM {TABLE}
        ORDER BY format, runs DESC
        LIMIT 15;
    """)
    print("\nSample rows:")
    for row in cur.fetchall():
        print(row)

    cur.close()
    conn.close()
    print("\n✅ Done. All formats stored in ONE table:", TABLE)


if __name__ == "__main__":
    main()