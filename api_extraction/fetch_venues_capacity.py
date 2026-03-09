#  4

'''
Cricbuzz API (recent matches endpoint)
        ↓
Fetch recent matches JSON
        ↓
Extract unique venue IDs from matchInfo
        ↓
Call Venue API for each venue_id
        ↓
Extract venue details (name, city, country, capacity, established, home_team)
        ↓
Filter venues with capacity > 25000
        ↓
Select first 10 valid venues
        ↓
Connect to MySQL (cricket_db)
        ↓
Create venues table if not exists
        ↓
Insert / Update venue records
        ↓
Store venues data in database
        ↓
Run SQL query to display top venues by capacity
'''

import requests
import mysql.connector
import re

# -----------------------------
# CONFIG
# -----------------------------
MATCHES_URL = "https://cricbuzz-cricket.p.rapidapi.com/matches/v1/recent"
VENUE_URL_TEMPLATE = "https://cricbuzz-cricket.p.rapidapi.com/venues/v1/{venue_id}"

RAPID_HEADERS = {
    "x-rapidapi-key": "####",
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com",
}

MYSQL_CFG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "cricket_db",
}

TABLE = "venues"

CAPACITY_MIN = 25000
TARGET = 10


def to_int_capacity(cap):
    """Convert '33,000' -> 33000"""
    if cap is None:
        return None
    s = str(cap).replace(",", "").strip()
    s = re.sub(r"[^0-9]", "", s)
    return int(s) if s else None


def iter_match_infos(payload):
    """Yield matchInfo dicts from nested matches payload"""
    for tm in payload.get("typeMatches", []):
        for sm in tm.get("seriesMatches", []):
            wrapper = sm.get("seriesAdWrapper")
            if not wrapper:
                continue
            for m in wrapper.get("matches", []):
                info = m.get("matchInfo")
                if info:
                    yield info


def fetch_venue(venue_id: int):
    url = VENUE_URL_TEMPLATE.format(venue_id=venue_id)
    r = requests.get(url, headers=RAPID_HEADERS, timeout=20)
    j = r.json()

    if isinstance(j, dict) and j.get("message"):
        raise RuntimeError(f"Venue {venue_id}: API error: {j['message']}")

    venue_name = (j.get("ground") or "").strip()
    city = (j.get("city") or "").strip()
    country = (j.get("country") or "").strip()
    capacity = to_int_capacity(j.get("capacity"))

    established = j.get("established")
    try:
        established = int(established) if established is not None else None
    except:
        established = None

    home_team = (j.get("homeTeam") or "").strip()

    return (venue_id, venue_name, city, country, capacity, established, home_team)


def main():
    # 1) Fetch recent matches
    resp = requests.get(MATCHES_URL, headers=RAPID_HEADERS, timeout=20)
    matches_data = resp.json()

    if isinstance(matches_data, dict) and matches_data.get("message"):
        raise RuntimeError(f"Matches API error: {matches_data['message']}")

    # 2) Extract unique venue IDs from matches payload
    venue_ids = []
    seen = set()

    for info in iter_match_infos(matches_data):
        v = info.get("venueInfo") or {}
        vid = v.get("id") or v.get("groundId")
        if vid is None:
            continue
        vid = int(vid)
        if vid not in seen:
            seen.add(vid)
            venue_ids.append(vid)

    print("Unique venue IDs found in matches:", len(venue_ids))

    # 3) Fetch venues until we get 10 with capacity > 25000
    rows = []
    for vid in venue_ids:
        if len(rows) >= TARGET:
            break

        try:
            row = fetch_venue(vid)

            cap = row[4]  # capacity field
            if cap is None or cap <= CAPACITY_MIN:
                continue

            rows.append(row)
            print(f"Accepted ({len(rows)}/{TARGET}): {vid} | {row[1]} | cap={cap}")

        except Exception as e:
            print("Skipped venue:", vid, "| reason:", e)

    if len(rows) < TARGET:
        print(f"\nWARNING: Only found {len(rows)} venues with capacity > {CAPACITY_MIN} from recent matches.")
        print("Try a different matches endpoint or widen the match list.")

    # 4) Connect MySQL
    conn = mysql.connector.connect(
        host=MYSQL_CFG["host"],
        user=MYSQL_CFG["user"],
        password=MYSQL_CFG["password"],
    )
    cur = conn.cursor()

    # 5) Create DB if not exists + use it
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_CFG['database']}")
    cur.execute(f"USE {MYSQL_CFG['database']}")

    # 6) Create table
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
          venue_id INT PRIMARY KEY,
          venue_name VARCHAR(150),
          city VARCHAR(100),
          country VARCHAR(100),
          capacity INT,
          established INT,
          home_team VARCHAR(200)
        )
    """)

    # 7) Insert (upsert)
    cur.executemany(
        f"""
        INSERT INTO {TABLE}
          (venue_id, venue_name, city, country, capacity, established, home_team)
        VALUES
          (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          venue_name=VALUES(venue_name),
          city=VALUES(city),
          country=VALUES(country),
          capacity=VALUES(capacity),
          established=VALUES(established),
          home_team=VALUES(home_team)
        """,
        rows
    )
    conn.commit()

    # 8) Test SQL
    cur.execute(f"""
        SELECT
          venue_name AS Venue,
          city AS City,
          country AS Country,
          capacity AS Capacity
        FROM {TABLE}
        WHERE capacity > {CAPACITY_MIN}
        ORDER BY capacity DESC
        LIMIT 10;
    """)
    out = cur.fetchall()

    print("\nTop venues capacity > 25000:")
    for r in out:
        print(r)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()