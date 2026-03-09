'''
Load filtered series list JSON
(series_india_icc_clean_unique.json)
        ↓
For each series_id:
        ↓
Call Cricbuzz Series Matches API
        ↓
Extract match details
(teams, venue, score, status)
        ↓
Parse result info
(winner, result_type, win margins)
        ↓
Store unique matches in list
        ↓
Save all matches to JSON file
(all_series_matches.json)
        ↓
Identify ICC tournament series
        ↓
Collect venue_ids used in those tournaments
        ↓
Call Venue API for each venue_id
        ↓
Save tournament venue details to JSON
(tournament_venues.json)
'''
import http.client
import json
import time
from pathlib import Path

# =========================================================
# CONFIG
# =========================================================
API_HOST = "cricbuzz-cricket.p.rapidapi.com"
API_KEY = "ca82725572msh7794348cec8a7d9p1fcfb0jsne446ed9dd21f"

INPUT_SERIES_JSON = "series_india_icc_clean_unique.json"

# series matches endpoint
SERIES_MATCHES_URL_TEMPLATE = "/series/v1/{series_id}"

# venue endpoint (replace this with the exact URL path you mentioned)
VENUE_URL_TEMPLATE = "/venues/v1/{venue_id}"

OUTPUT_DIR = Path("saved_matches_json")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MATCHES_OUTPUT_JSON = OUTPUT_DIR / "all_series_matches.json"
TOURNAMENT_VENUES_OUTPUT_JSON = OUTPUT_DIR / "tournament_venues.json"

SLEEP_SECONDS = 1.0   # to reduce rate-limit risk

ICC_MAIN_KEYWORDS = [
    "ICC Cricket World Cup",
    "ICC Mens T20 World Cup",
    "ICC Champions Trophy",
    "ICC World Test Championship Final",
    "World Test Championship Final"
]

HEADERS = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": API_HOST
}

# =========================================================
# HELPERS
# =========================================================
def make_request(endpoint: str):
    conn = http.client.HTTPSConnection(API_HOST)
    try:
        conn.request("GET", endpoint, headers=HEADERS)
        res = conn.getresponse()
        raw = res.read()
        status_code = res.status

        if status_code != 200:
            raise Exception(f"HTTP {status_code}: {raw[:300].decode('utf-8', errors='ignore')}")

        return json.loads(raw.decode("utf-8"))
    finally:
        conn.close()


def load_series(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def is_tournament(series_name: str) -> bool:
    name = (series_name or "").lower()
    return any(k.lower() in name for k in ICC_MAIN_KEYWORDS)


def safe_get(dct, *keys, default=None):
    cur = dct
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur


def parse_status(status_text: str):
    """
    Very light parser for:
    - winner
    - result_type
    - win_margin_runs
    - win_margin_wickets
    - win_method

    Handles common patterns like:
    "India won by 29 runs"
    "Pakistan won by 3 wkts"
    "Match tied and 1st Super over tied (SA won 2nd Super Over)"
    "Match abandoned due to rain (no toss)"
    """
    result = {
        "winner": None,
        "result_type": None,
        "win_margin_runs": None,
        "win_margin_wickets": None,
        "win_method": None
    }

    if not status_text:
        return result

    s = status_text.strip()
    sl = s.lower()

    if "abandon" in sl:
        result["result_type"] = "abandoned"
        result["win_method"] = "abandoned"
        return result

    if "tied" in sl:
        result["result_type"] = "tied"
        if "super over" in sl:
            result["win_method"] = "super_over"
            # crude extraction of winner inside parentheses like "(SA won 2nd Super Over)"
            if "(" in s and " won " in s:
                inside = s[s.find("(") + 1:s.rfind(")")] if ")" in s else s[s.find("(") + 1:]
                result["winner"] = inside.split(" won ")[0].strip()
        return result

    if "won by" in sl:
        winner = s.split(" won by ")[0].strip()
        margin_part = s.split(" won by ")[1].strip()

        result["winner"] = winner
        result["result_type"] = "win"

        if "run" in margin_part:
            result["win_method"] = "runs"
            try:
                result["win_margin_runs"] = int(margin_part.split()[0])
            except Exception:
                pass

        elif "wkt" in margin_part or "wicket" in margin_part:
            result["win_method"] = "wickets"
            try:
                result["win_margin_wickets"] = int(margin_part.split()[0])
            except Exception:
                pass
        else:
            result["win_method"] = "other"

        return result

    if "match starts at" in sl:
        result["result_type"] = "scheduled"
        return result

    return result


def build_match_row(match_obj):
    mi = match_obj.get("matchInfo", {})
    ms = match_obj.get("matchScore", {})

    t1 = mi.get("team1", {}) or {}
    t2 = mi.get("team2", {}) or {}
    venue = mi.get("venueInfo", {}) or {}

    t1s = ms.get("team1Score", {}) or {}
    t2s = ms.get("team2Score", {}) or {}

    t1_i1 = t1s.get("inngs1", {}) or {}
    t1_i2 = t1s.get("inngs2", {}) or {}
    t2_i1 = t2s.get("inngs1", {}) or {}
    t2_i2 = t2s.get("inngs2", {}) or {}

    status_info = parse_status(mi.get("status"))

    return {
        "match_id": mi.get("matchId"),
        "series_id": mi.get("seriesId"),
        "series_name": mi.get("seriesName"),
        "match_desc": mi.get("matchDesc"),
        "match_format": mi.get("matchFormat"),
        "start_date": mi.get("startDate"),
        "end_date": mi.get("endDate"),
        "state": mi.get("state"),
        "status": mi.get("status"),

        "team1_id": t1.get("teamId"),
        "team1_name": t1.get("teamName"),
        "team1_short_name": t1.get("teamSName"),
        "team1_image_id": t1.get("imageId"),

        "team2_id": t2.get("teamId"),
        "team2_name": t2.get("teamName"),
        "team2_short_name": t2.get("teamSName"),
        "team2_image_id": t2.get("imageId"),

        "venue_id": venue.get("id"),
        "venue_name": venue.get("ground"),
        "venue_city": venue.get("city"),
        "venue_timezone": venue.get("timezone"),

        "curr_bat_team_id": mi.get("currBatTeamId"),
        "series_start_dt": mi.get("seriesStartDt"),
        "series_end_dt": mi.get("seriesEndDt"),
        "is_time_announced": mi.get("isTimeAnnounced"),

        "team1_inngs1_runs": t1_i1.get("runs"),
        "team1_inngs1_wickets": t1_i1.get("wickets"),
        "team1_inngs1_overs": t1_i1.get("overs"),

        "team1_inngs2_runs": t1_i2.get("runs"),
        "team1_inngs2_wickets": t1_i2.get("wickets"),
        "team1_inngs2_overs": t1_i2.get("overs"),

        "team2_inngs1_runs": t2_i1.get("runs"),
        "team2_inngs1_wickets": t2_i1.get("wickets"),
        "team2_inngs1_overs": t2_i1.get("overs"),

        "team2_inngs2_runs": t2_i2.get("runs"),
        "team2_inngs2_wickets": t2_i2.get("wickets"),
        "team2_inngs2_overs": t2_i2.get("overs"),

        "winner": status_info["winner"],
        "result_type": status_info["result_type"],
        "win_margin_runs": status_info["win_margin_runs"],
        "win_margin_wickets": status_info["win_margin_wickets"],
        "win_method": status_info["win_method"]
    }


# =========================================================
# MAIN
# =========================================================
series_list = load_series(INPUT_SERIES_JSON)

all_matches = []
seen_match_ids = set()

tournament_venue_ids = set()
tournament_venues = []
seen_tournament_venue_ids = set()

print("=" * 90)
print("FETCHING MATCHES")
print("=" * 90)

for idx, series in enumerate(series_list, start=1):
    series_id = series.get("series_id")
    series_name = series.get("series_name", "")

    if not series_id:
        continue

    print(f"[{idx}/{len(series_list)}] Fetching matches for series_id={series_id} | {series_name}")

    try:
        endpoint = SERIES_MATCHES_URL_TEMPLATE.format(series_id=series_id)
        payload = make_request(endpoint)

        for block in payload.get("matchDetails", []):
            match_map = block.get("matchDetailsMap")
            if not match_map:
                continue

            for match_obj in match_map.get("match", []):
                row = build_match_row(match_obj)

                match_id = row.get("match_id")
                if not match_id or match_id in seen_match_ids:
                    continue

                seen_match_ids.add(match_id)
                all_matches.append(row)

                # collect venue ids only for ICC tournaments
                if is_tournament(series_name):
                    venue_id = row.get("venue_id")
                    if venue_id:
                        tournament_venue_ids.add(venue_id)

        time.sleep(SLEEP_SECONDS)

    except Exception as e:
        print(f"  -> Match fetch failed for {series_id}: {e}")

# save matches json
with open(MATCHES_OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(all_matches, f, indent=2, ensure_ascii=False)

print("\n" + "=" * 90)
print("MATCH FETCH SUMMARY")
print("=" * 90)
print(f"Total unique matches saved: {len(all_matches)}")
print(f"Matches JSON saved to: {MATCHES_OUTPUT_JSON}")

# =========================================================
# FETCH TOURNAMENT VENUES ONLY
# =========================================================
print("\n" + "=" * 90)
print("FETCHING TOURNAMENT VENUES")
print("=" * 90)
print(f"Unique tournament venue ids found: {len(tournament_venue_ids)}")

for idx, venue_id in enumerate(sorted(tournament_venue_ids), start=1):
    print(f"[{idx}/{len(tournament_venue_ids)}] Fetching venue_id={venue_id}")

    if venue_id in seen_tournament_venue_ids:
        continue

    try:
        endpoint = VENUE_URL_TEMPLATE.format(venue_id=venue_id)
        venue_payload = make_request(endpoint)

        # keep raw venue payload, plus venue_id for safety
        venue_record = {
            "venue_id": venue_id,
            "payload": venue_payload
        }

        tournament_venues.append(venue_record)
        seen_tournament_venue_ids.add(venue_id)

        time.sleep(SLEEP_SECONDS)

    except Exception as e:
        print(f"  -> Venue fetch failed for {venue_id}: {e}")

with open(TOURNAMENT_VENUES_OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(tournament_venues, f, indent=2, ensure_ascii=False)

print("\n" + "=" * 90)
print("VENUE FETCH SUMMARY")
print("=" * 90)
print(f"Total tournament venues saved: {len(tournament_venues)}")
print(f"Tournament venues JSON saved to: {TOURNAMENT_VENUES_OUTPUT_JSON}")                                                                                                                   