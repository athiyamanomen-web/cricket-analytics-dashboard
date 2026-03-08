'''fetch_live_matches() calls the live endpoint.
extract_live_matches() flattens nested live JSON into a simple list.
The user picks one live match.
fetch_scorecard(match_id) calls the scorecard endpoint only for that selected match.
Streamlit renders:
live match cards
selected match header
summary
scorecard
partnerships
fall of wickets
match info'''


import math
import requests
import pandas as pd
import streamlit as st


# =========================================================
# CONFIG
# =========================================================

BASE_URL = "https://cricbuzz-cricket.p.rapidapi.com"

# Put your regenerated key in .streamlit/secrets.toml like:
# RAPIDAPI_KEY = "your_new_key_here"
try:
    RAPIDAPI_KEY = ""
except Exception:
    RAPIDAPI_KEY = None

if not RAPIDAPI_KEY:
    st.error("Missing RAPIDAPI_KEY in Streamlit secrets.")
    st.stop()

HEADERS = {
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}


# =========================================================
# API FETCH
# =========================================================
@st.cache_data(show_spinner=False)
def fetch_live_matches():
    url = f"{BASE_URL}/matches/v1/live"
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    return resp.json()


@st.cache_data(show_spinner=False)
def fetch_scorecard(match_id: int):
    url = f"{BASE_URL}/mcenter/v1/{match_id}/scard"
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    return resp.json()


# =========================================================
# HELPERS
# =========================================================
def safe_get(dct, *keys, default=None):
    cur = dct
    for key in keys:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def fmt_overs(value):
    if value is None:
        return "-"
    try:
        val = float(value)
        if val.is_integer():
            return str(int(val))
        return str(val)
    except Exception:
        return str(value)


def fmt_score(score_block: dict) -> str:
    if not score_block:
        return "-"
    runs = score_block.get("runs", "-")
    wickets = score_block.get("wickets", "-")
    overs = fmt_overs(score_block.get("overs"))
    return f"{runs}/{wickets} ({overs})"


def get_status_badge(state: str) -> str:
    state_upper = (state or "").strip().upper()
    if "PROGRESS" in state_upper:
        return "LIVE"
    if "BREAK" in state_upper:
        return "INNINGS BREAK"
    if "STUMPS" in state_upper:
        return "STUMPS"
    if "COMPLETE" in state_upper:
        return "COMPLETE"
    return state_upper or "MATCH"


def extract_live_matches(live_payload: dict) -> list:
    flat_matches = []

    for type_block in live_payload.get("typeMatches", []):
        match_type = type_block.get("matchType", "")

        for series_block in type_block.get("seriesMatches", []):
            wrapper = series_block.get("seriesAdWrapper")
            if not wrapper:
                continue

            series_id = wrapper.get("seriesId")
            series_name = wrapper.get("seriesName", "")
            matches = wrapper.get("matches", [])

            for match in matches:
                match_info = match.get("matchInfo", {})
                match_score = match.get("matchScore", {})

                team1 = match_info.get("team1", {})
                team2 = match_info.get("team2", {})
                venue = match_info.get("venueInfo", {})

                team1_inng1 = safe_get(match_score, "team1Score", "inngs1", default={})
                team1_inng2 = safe_get(match_score, "team1Score", "inngs2", default={})
                team2_inng1 = safe_get(match_score, "team2Score", "inngs1", default={})
                team2_inng2 = safe_get(match_score, "team2Score", "inngs2", default={})

                state = match_info.get("state", "")
                state_title = match_info.get("stateTitle", "")

                # Keep only active-style matches
                active_states = {"In Progress", "Innings Break", "Stumps"}
                if state not in active_states and state_title not in {"In Progress", "Ings Break", "Stumps"}:
                    continue

                flat_matches.append({
                    "matchTypeGroup": match_type,
                    "seriesId": series_id,
                    "seriesName": series_name,
                    "matchId": match_info.get("matchId"),
                    "matchDesc": match_info.get("matchDesc", ""),
                    "matchFormat": match_info.get("matchFormat", ""),
                    "state": state,
                    "stateTitle": state_title,
                    "status": match_info.get("status", ""),
                    "team1Id": team1.get("teamId"),
                    "team1Name": team1.get("teamName", ""),
                    "team1SName": team1.get("teamSName", ""),
                    "team2Id": team2.get("teamId"),
                    "team2Name": team2.get("teamName", ""),
                    "team2SName": team2.get("teamSName", ""),
                    "currBatTeamId": match_info.get("currBatTeamId"),
                    "venueGround": venue.get("ground", ""),
                    "venueCity": venue.get("city", ""),
                    "venueTimezone": venue.get("timezone", ""),
                    "team1_inng1": team1_inng1,
                    "team1_inng2": team1_inng2,
                    "team2_inng1": team2_inng1,
                    "team2_inng2": team2_inng2,
                })

    return flat_matches


def build_batting_df(innings: dict) -> pd.DataFrame:
    rows = []
    for batter in innings.get("batsman", []):
        dismissal = batter.get("outdec", "") or "yet to bat"
        rows.append({
            "Batter": batter.get("name", ""),
            "Dismissal": dismissal,
            "R": batter.get("runs", 0),
            "B": batter.get("balls", 0),
            "4s": batter.get("fours", 0),
            "6s": batter.get("sixes", 0),
            "SR": batter.get("strkrate", "0"),
        })
    return pd.DataFrame(rows)


def build_bowling_df(innings: dict) -> pd.DataFrame:
    rows = []
    for bowler in innings.get("bowler", []):
        rows.append({
            "Bowler": bowler.get("name", ""),
            "O": bowler.get("overs", ""),
            "M": bowler.get("maidens", 0),
            "R": bowler.get("runs", 0),
            "W": bowler.get("wickets", 0),
            "Econ": bowler.get("economy", ""),
        })
    return pd.DataFrame(rows)


def build_partnerships_df(innings: dict) -> pd.DataFrame:
    rows = []
    for p in safe_get(innings, "partnership", "partnership", default=[]):
        rows.append({
            "Pair": f"{p.get('bat1name', '')} + {p.get('bat2name', '')}",
            "Runs": p.get("totalruns", 0),
            "Balls": p.get("totalballs", 0),
            "Bat 1": f"{p.get('bat1runs', 0)} ({p.get('bat1balls', 0)})",
            "Bat 2": f"{p.get('bat2runs', 0)} ({p.get('bat2balls', 0)})",
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("Runs", ascending=False).reset_index(drop=True)
    return df


def build_fow_df(innings: dict) -> pd.DataFrame:
    rows = []
    for wicket_num, item in enumerate(safe_get(innings, "fow", "fow", default=[]), start=1):
        rows.append({
            "Wicket": wicket_num,
            "Score": item.get("runs", 0),
            "Player": item.get("batsmanname", ""),
            "Over": item.get("overnbr", ""),
        })
    return pd.DataFrame(rows)


def build_powerplay_df(innings: dict) -> pd.DataFrame:
    rows = []
    for pp in safe_get(innings, "pp", "powerplay", default=[]):
        rows.append({
            "Type": pp.get("pptype", ""),
            "From": pp.get("ovrfrom", ""),
            "To": pp.get("ovrto", ""),
            "Runs": pp.get("run", 0),
            "Wickets": pp.get("wickets", 0),
        })
    return pd.DataFrame(rows)


def extras_text(innings: dict) -> str:
    extras = innings.get("extras", {})
    return (
        f"Extras: {extras.get('total', 0)} "
        f"(b {extras.get('byes', 0)}, lb {extras.get('legbyes', 0)}, "
        f"w {extras.get('wides', 0)}, nb {extras.get('noballs', 0)}, "
        f"pen {extras.get('penalty', 0)})"
    )


def overs_to_balls(overs_value):
    try:
        overs_float = float(overs_value)
        whole = int(overs_float)
        balls_part = round((overs_float - whole) * 10)
        return whole * 6 + balls_part
    except Exception:
        return None


def compute_chase_metrics(match_data: dict, scorecard_payload: dict | None):
    target = None
    runs_needed = None
    balls_left = None
    current_rr = None
    required_rr = None

    innings_list = scorecard_payload.get("scorecard", []) if scorecard_payload else []

    first = innings_list[0] if len(innings_list) > 0 else {}
    second = innings_list[1] if len(innings_list) > 1 else {}

    first_innings_score = first.get("score")
    second_innings_score = second.get("score")
    second_innings_overs = second.get("overs")
    current_rr = second.get("runrate")

    if first_innings_score is None:
        t1 = match_data.get("team1_inng1", {}) or {}
        t2 = match_data.get("team2_inng1", {}) or {}
        first_innings_score = max(t1.get("runs", 0), t2.get("runs", 0))

    if second_innings_score is None:
        curr_bat_team_id = match_data.get("currBatTeamId")
        if curr_bat_team_id == match_data.get("team1Id"):
            second_innings_score = safe_get(match_data, "team1_inng1", "runs", default=0)
            second_innings_overs = safe_get(match_data, "team1_inng1", "overs", default=0)
        elif curr_bat_team_id == match_data.get("team2Id"):
            second_innings_score = safe_get(match_data, "team2_inng1", "runs", default=0)
            second_innings_overs = safe_get(match_data, "team2_inng1", "overs", default=0)

    if first_innings_score is not None:
        target = first_innings_score + 1

    if target is not None and second_innings_score is not None:
        runs_needed = max(target - second_innings_score, 0)

    match_format = (match_data.get("matchFormat") or "").upper()
    total_overs = 20 if match_format == "T20" else 50 if match_format == "ODI" else None

    balls_bowled = overs_to_balls(second_innings_overs)
    if total_overs is not None and balls_bowled is not None:
        balls_left = max(total_overs * 6 - balls_bowled, 0)

    if runs_needed is not None and balls_left and balls_left > 0:
        required_rr = round((runs_needed / balls_left) * 6, 2)

    return {
        "target": target,
        "runs_needed": runs_needed,
        "balls_left": balls_left,
        "current_rr": current_rr,
        "required_rr": required_rr,
    }


def render_match_card(match: dict):
    badge = get_status_badge(match.get("state", ""))
    team1_score = fmt_score(match.get("team1_inng1"))
    team2_score = fmt_score(match.get("team2_inng1"))
    venue = f"{match.get('venueGround', '')}, {match.get('venueCity', '')}".strip(", ")

    with st.container(border=True):
        c1, c2 = st.columns([4, 1])

        with c1:
            st.markdown(f"### {match.get('seriesName', '')}")
            st.markdown(f"**{match.get('matchDesc', '')} • {match.get('matchFormat', '')}**")

        with c2:
            st.markdown(f"#### `{badge}`")

        st.markdown(
            f"""
**{match.get('team1SName', '')}** — {team1_score}  
**{match.get('team2SName', '')}** — {team2_score}
"""
        )

        st.markdown(f"**Status:** {match.get('status', '-')}")
        st.caption(venue if venue else "Venue not available")

        if st.button("Open Match", key=f"open_{match.get('matchId')}"):
            st.session_state["selected_match_id"] = match.get("matchId")


def render_summary_tab(match: dict, scorecard: dict | None):
    st.subheader("Match Summary")

    if not scorecard or not scorecard.get("scorecard"):
        st.info("Scorecard not available for this match.")
        return

    innings_list = scorecard["scorecard"]
    first = innings_list[0] if len(innings_list) > 0 else {}
    second = innings_list[1] if len(innings_list) > 1 else {}

    top_batter = max(first.get("batsman", []), key=lambda x: x.get("runs", 0), default=None)
    top_bowler_first = max(first.get("bowler", []), key=lambda x: x.get("wickets", 0), default=None)
    top_bowler_second = max(second.get("bowler", []), key=lambda x: x.get("wickets", 0), default=None)

    metrics = compute_chase_metrics(match, scorecard)

    lines = []
    if first:
        lines.append(
            f"{first.get('batteamname', '')} scored {first.get('score', '-')}/{first.get('wickets', '-')} "
            f"in {first.get('overs', '-')} overs"
        )
    if top_batter:
        lines.append(
            f"{top_batter.get('name', '')} top-scored with {top_batter.get('runs', 0)} "
            f"off {top_batter.get('balls', 0)}"
        )
    if second:
        lines.append(
            f"{second.get('batteamname', '')} are {second.get('score', '-')}/{second.get('wickets', '-')} "
            f"after {second.get('overs', '-')}"
        )
    if metrics["required_rr"] is not None:
        lines.append(f"required run rate is {metrics['required_rr']}")

    st.write(". ".join(lines) + "." if lines else "No summary available.")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        if top_batter:
            st.metric("Top Batter", top_batter["name"], f"{top_batter['runs']} ({top_batter['balls']})")
    with c2:
        if top_bowler_first:
            st.metric("Best Bowling (1st inns)", top_bowler_first["name"], f"{top_bowler_first['wickets']} wkts")
    with c3:
        if top_bowler_second:
            st.metric("Best Bowling (chase)", top_bowler_second["name"], f"{top_bowler_second['wickets']} wkts")
    with c4:
        if second:
            st.metric("Wickets in Hand", 10 - second.get("wickets", 0))


def render_scorecard_tab(scorecard: dict | None):
    st.subheader("Scorecard")

    if not scorecard or not scorecard.get("scorecard"):
        st.info("Scorecard not available for this match.")
        return

    for idx, innings in enumerate(scorecard["scorecard"], start=1):
        st.markdown("---")
        st.markdown(
            f"### {innings.get('batteamname', f'Innings {idx}')} — "
            f"{innings.get('score', '-')}/{innings.get('wickets', '-')} "
            f"({innings.get('overs', '-')})"
        )
        st.caption(f"Run Rate: {innings.get('runrate', '-')}")

        c1, c2 = st.columns([3, 2])

        with c1:
            st.markdown("**Batting**")
            st.dataframe(build_batting_df(innings), use_container_width=True, hide_index=True)
            st.caption(extras_text(innings))

        with c2:
            st.markdown("**Bowling**")
            st.dataframe(build_bowling_df(innings), use_container_width=True, hide_index=True)

            pp_df = build_powerplay_df(innings)
            if not pp_df.empty:
                st.markdown("**Powerplay**")
                st.dataframe(pp_df, use_container_width=True, hide_index=True)


def render_partnerships_tab(scorecard: dict | None):
    st.subheader("Partnerships")

    if not scorecard or not scorecard.get("scorecard"):
        st.info("Scorecard not available for this match.")
        return

    for innings in scorecard["scorecard"]:
        st.markdown(f"### {innings.get('batteamname', '')}")
        df = build_partnerships_df(innings)
        if df.empty:
            st.info("No partnership data available.")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)


def render_fow_tab(scorecard: dict | None):
    st.subheader("Fall of Wickets")

    if not scorecard or not scorecard.get("scorecard"):
        st.info("Scorecard not available for this match.")
        return

    for innings in scorecard["scorecard"]:
        st.markdown(f"### {innings.get('batteamname', '')}")
        df = build_fow_df(innings)
        if df.empty:
            st.info("No fall-of-wickets data available.")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)


def render_match_info_tab(match: dict):
    st.subheader("Match Info")
    info = {
        "Series": match.get("seriesName", ""),
        "Match": match.get("matchDesc", ""),
        "Format": match.get("matchFormat", ""),
        "State": match.get("state", ""),
        "Status": match.get("status", ""),
        "Team 1": f"{match.get('team1Name', '')} ({match.get('team1SName', '')})",
        "Team 2": f"{match.get('team2Name', '')} ({match.get('team2SName', '')})",
        "Venue": match.get("venueGround", ""),
        "City": match.get("venueCity", ""),
        "Timezone": match.get("venueTimezone", ""),
        "Match ID": match.get("matchId", ""),
    }
    df = pd.DataFrame(list(info.items()), columns=["Field", "Value"])
    st.dataframe(df, use_container_width=True, hide_index=True)


# =========================================================
# LOAD LIVE DATA
# =========================================================
try:
    live_payload = fetch_live_matches()
    live_matches = extract_live_matches(live_payload)
except requests.RequestException as e:
    st.error(f"Failed to fetch live matches: {e}")
    st.stop()

if not live_matches:
    st.warning("No active live matches found.")
    st.stop()

if "selected_match_id" not in st.session_state:
    st.session_state["selected_match_id"] = live_matches[0]["matchId"]


# =========================================================
# SIDEBAR
# =========================================================
st.sidebar.title("🏏Cricbuzz Live Match Center")
st.sidebar.caption("Live list from API. Scorecard fetched only for selected match.")

if st.sidebar.button("Refresh Live Matches"):
    fetch_live_matches.clear()
    st.rerun()

st.sidebar.markdown("### Active Matches")
for m in live_matches:
    label = f"{m.get('team1SName')} vs {m.get('team2SName')} • {m.get('matchDesc')}"
    if st.sidebar.button(label, key=f"side_{m.get('matchId')}"):
        st.session_state["selected_match_id"] = m.get("matchId")


# =========================================================
# MAIN LAYOUT
# =========================================================
st.title("🏏 Live Cricket Match Center")

left_col, right_col = st.columns([1.2, 1.8])

with left_col:
    st.markdown("## Live Matches")
    for match in live_matches:
        render_match_card(match)

selected_match = next(
    (m for m in live_matches if m.get("matchId") == st.session_state["selected_match_id"]),
    None
)

with right_col:
    if not selected_match:
        st.info("Select a match.")
        st.stop()

    selected_scorecard = None
    try:
        selected_scorecard = fetch_scorecard(selected_match["matchId"])
    except requests.RequestException:
        selected_scorecard = None

    st.markdown(f"## {selected_match.get('seriesName', '')}")
    st.markdown(f"### {selected_match.get('matchDesc', '')}")

    badge = get_status_badge(selected_match.get("state", ""))
    st.markdown(
        f"**{selected_match.get('team1Name', '')} vs {selected_match.get('team2Name', '')}**  |  `{badge}`"
    )

    h1, h2 = st.columns(2)
    with h1:
        st.metric(selected_match.get("team1SName", "Team 1"), fmt_score(selected_match.get("team1_inng1")))
    with h2:
        st.metric(selected_match.get("team2SName", "Team 2"), fmt_score(selected_match.get("team2_inng1")))

    st.markdown(f"**Status:** {selected_match.get('status', '-')}")
    st.caption(
        f"{selected_match.get('venueGround', '')}, {selected_match.get('venueCity', '')} "
        f"• Timezone: {selected_match.get('venueTimezone', '-')}"
    )
    st.caption("Detailed scorecard may refresh slightly later than the live summary.")

    metrics = compute_chase_metrics(selected_match, selected_scorecard)
    q1, q2, q3, q4, q5 = st.columns(5)
    with q1:
        st.metric("Target", metrics["target"] if metrics["target"] is not None else "-")
    with q2:
        st.metric("Runs Needed", metrics["runs_needed"] if metrics["runs_needed"] is not None else "-")
    with q3:
        st.metric("Balls Left", metrics["balls_left"] if metrics["balls_left"] is not None else "-")
    with q4:
        st.metric("Current RR", metrics["current_rr"] if metrics["current_rr"] is not None else "-")
    with q5:
        st.metric("Required RR", metrics["required_rr"] if metrics["required_rr"] is not None else "-")

    if st.button("Refresh Selected Scorecard"):
        fetch_scorecard.clear()
        st.rerun()

    st.markdown("---")

    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        ["Summary", "Scorecard", "Partnerships", "Fall of Wickets", "Match Info"]
    )

    with tab1:
        render_summary_tab(selected_match, selected_scorecard)

    with tab2:
        render_scorecard_tab(selected_scorecard)

    with tab3:
        render_partnerships_tab(selected_scorecard)

    with tab4:
        render_fow_tab(selected_scorecard)

    with tab5:
        render_match_info_tab(selected_match)
