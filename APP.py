# app.py
# Streamlit: type player name -> choose player -> load profile
# Layout (like your sketch):
#   Row 1: Player Info | Rankings (tabs) + Teams
#   Row 2: Recent Performance (tabs: Batting/Bowling)
#   Row 3: Batting Career Summary | Bowling Career Summary
#   Row 4: Timeline
#   Row 5: Bio
#
# Uses RapidAPI Cricbuzz endpoints:
#   /stats/v1/player/search?plrN=<name>
#   /stats/v1/player/<id>
#   /stats/v1/player/<id>/batting
#   /stats/v1/player/<id>/bowling
#   /stats/v1/player/<id>/career

import re
from collections import deque
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import streamlit as st

# =========================
# CONFIG  
# =========================


st.markdown("""
<style>

/* TAB TEXT */
button[data-baseweb="tab"] {
    font-size:18px;
    font-weight:600;
    flex:1;
    text-align:center;
}

/* ACTIVE TAB (highlight) */
button[data-baseweb="tab"][aria-selected="true"] {
    border-bottom:3px solid #ff4b4b;
}

/* HOVER EFFECT */
button[data-baseweb="tab"]:hover {
    background-color:#f5f5f5;
}

</style>
""", unsafe_allow_html=True)

RAPIDAPI_KEY = ""  # <-- paste your key here
RAPIDAPI_HOST = "cricbuzz-cricket.p.rapidapi.com"
BASE_PLAYER = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player"
SEARCH_URL = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/search?plrN={query}"

HEADERS = {
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": RAPIDAPI_HOST,
}

DEFAULT_QUERY = "Root"
DEFAULT_PLAYER_ID = 8019  # fallback


# =========================
# NETWORK
# =========================
@st.cache_data(show_spinner=False, ttl=600)
def get_json(url: str) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
    except Exception as e:
        st.error(f"Network error: {e}")
        return None

    if r.status_code == 200:
        return r.json()

    st.error(f"{r.status_code} for {url}")
    try:
        st.code(r.text)
    except Exception:
        pass
    return None


# =========================
# PARSERS / BUILDERS (keeps DF structures)
# =========================
def clean_bio(bio: str) -> str:
    bio = (bio or "").replace("<br/>", "\n")
    return re.sub(r"<.*?>", "", bio)


def rankings_block(d: Dict[str, Any]) -> Dict[str, List[Any]]:
    return {
        "Format": ["Test", "ODI", "T20"],
        "Current Rank": [d.get("testRank", "--"), d.get("odiRank", "--"), d.get("t20Rank", "--")],
        "Best Rank": [d.get("testBestRank", "--"), d.get("odiBestRank", "--"), d.get("t20BestRank", "--")],
    }


def recent_form(rows: List[Dict[str, Any]]) -> List[deque]:
    out: List[deque] = []
    for row in rows:
        q = deque(row.get("values", []))
        if q:
            q.popleft()  # keep your original behavior
        out.append(q)
    return out


def career_summary(d: Dict[str, Any]) -> pd.DataFrame:
    cols = d.get("headers", [])
    rows = [x.get("values", []) for x in d.get("values", [])]
    return pd.DataFrame(rows, columns=cols)


def timeline_df(d: Dict[str, Any]) -> pd.DataFrame:
    rows = [[x.get("name", ""), x.get("debut", ""), x.get("lastPlayed", "")] for x in d.get("values", [])]
    return pd.DataFrame(rows, columns=["Format", "Debut", "Last Match"])


def safe_table(df: pd.DataFrame) -> None:
    if df is None or df.empty:
        st.write("--")
        return
    
    st.dataframe(
        df.reset_index(drop=True),
        use_container_width=True,
        hide_index=True
    )


# =========================
# PLAYER SEARCH
# =========================
def search_players(query: str) -> List[Dict[str, Any]]:
    """Return list of {id, name, team} from search endpoint."""
    query = (query or "").strip()
    if not query:
        return []

    url = SEARCH_URL.format(query=requests.utils.quote(query))
    data = get_json(url)
    if not data:
        return []

    candidates: List[Dict[str, Any]] = []

    def walk(x: Any):
        if isinstance(x, dict):
            pid = x.get("id", x.get("playerId", x.get("pid")))
            pname = x.get("name", x.get("playerName", x.get("fullName")))
            if pid is not None and pname:
                candidates.append(
                    {
                        "id": int(pid) if str(pid).isdigit() else pid,
                        "name": str(pname),
                        "team": x.get("teamName", x.get("team", x.get("intlTeam", ""))),
                    }
                )
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)

    walk(data)

    # de-dup by id
    uniq = {}
    for c in candidates:
        uniq[c["id"]] = c
    out = list(uniq.values())

    # light sort: name match first, then alphabetic
    qlow = query.lower()
    out.sort(key=lambda z: (0 if qlow in z["name"].lower() else 1, z["name"].lower()))
    return out


# =========================
# PROFILE LOADER
# =========================
def load_profile(player_id: int) -> Optional[Dict[str, Any]]:
    base = get_json(f"{BASE_PLAYER}/{player_id}")
    if not base:
        return None

    profile = {
        "player_id": player_id,
        "name": base.get("name", ""),
        "country": base.get("intlTeam", ""),
        "role": base.get("role", ""),
        "nickname": base.get("nickName", ""),
        "bat": base.get("bat", ""),
        "bowl": base.get("bowl", ""),
        "dob": base.get("DoB", ""),
        "birth_place": base.get("birthPlace", ""),
        "teams": base.get("teams", ""),
        "bio": clean_bio(base.get("bio", "")),
    }

    # Rankings tables
    r = base.get("rankings", {})
    df1 = pd.DataFrame(rankings_block(r.get("bat", {})))
    df2 = pd.DataFrame(rankings_block(r.get("bowl", {})))
    df3 = pd.DataFrame(rankings_block(r.get("all", {})))

    # Recent form tables
    rb = base.get("recentBatting", {})
    df4 = pd.DataFrame(recent_form(rb.get("rows", [])), columns=rb.get("headers", []))

    rw = base.get("recentBowling", {})
    df5 = pd.DataFrame(recent_form(rw.get("rows", [])), columns=rw.get("headers", []))

    # Career summaries
    bat = get_json(f"{BASE_PLAYER}/{player_id}/batting")
    if not bat:
        return None
    Batting_career_summary = career_summary(bat)

    bowl = get_json(f"{BASE_PLAYER}/{player_id}/bowling")
    if not bowl:
        return None
    Bowling_career_summary = career_summary(bowl)

    # Timeline
    car = get_json(f"{BASE_PLAYER}/{player_id}/career")
    if not car:
        return None
    timeline = timeline_df(car)

    profile.update(
        df1=df1,
        df2=df2,
        df3=df3,
        df4=df4,
        df5=df5,
        Batting_career_summary=Batting_career_summary,
        Bowling_career_summary=Bowling_career_summary,
        timeline=timeline,
    )
    return profile


# =========================
# RENDERERS
# =========================
def render_player_info_box(profile):
    with st.container(border=True):
        st.markdown("## Player Info")
        st.markdown(f"### **{profile.get('name')}**")
        st.write(f"Born: {profile.get('dob')}")
        st.write(f"Country: {profile.get('country')}")
        st.write(f"Birth Place: {profile.get('birth_place')}")
        st.write(f"Nickname: {profile.get('nickname')}")
        st.write(f"Role: {profile.get('role')}")
        st.write(f"Batting Style: {profile.get('bat')}")
        st.write(f"Bowling Style: {profile.get('bowl')}")


def render_rankings_and_teams_box(profile: Dict[str, Any]) -> None:
    with st.container(border=True):
        st.subheader("ICC Rankings")

        t1, t2, t3 = st.tabs(["Batting", "Bowling", "All-rounder"])
        with t1:
            safe_table(profile.get("df1", pd.DataFrame()))
        with t2:
            safe_table(profile.get("df2", pd.DataFrame()))
        with t3:
            safe_table(profile.get("df3", pd.DataFrame()))

        st.markdown("### Teams")
        st.write(profile.get("teams") or "--")


def render_recent_performance(profile: Dict[str, Any]) -> None:
    with st.container(border=True):
        st.subheader("Recent Performance")

        tb, tw = st.tabs(["Batting", "Bowling"])

        with tb:
            st.markdown("### Recent Batting")
            safe_table(profile.get("df4", pd.DataFrame()))

        with tw:
            st.markdown("### Recent Bowling")
            safe_table(profile.get("df5", pd.DataFrame()))


def render_career_summaries(profile: Dict[str, Any]) -> None:
    with st.container(border=True):
        left, right = st.columns(2)
        with left:
            st.subheader("Batting Career Summary")
            safe_table(profile.get("Batting_career_summary", pd.DataFrame()))
        with right:
            st.subheader("Bowling Career Summary")
            safe_table(profile.get("Bowling_career_summary", pd.DataFrame()))


def render_timeline(timeline: pd.DataFrame) -> None:
    with st.container(border=True):
        st.subheader("Timeline")
        safe_table(timeline)


def render_bio(bio: str) -> None:
    with st.container(border=True):
        st.subheader("Summary")
        st.text(bio if bio else "No bio available.")


# =========================
# APP
# =========================
st.markdown("# 🏏 Cricket Player Statistics")

# Sidebar: name -> candidates -> choose -> load
st.sidebar.header("Player Search")
query = st.sidebar.text_input("Enter player name", value=DEFAULT_QUERY)

if "candidates" not in st.session_state:
    st.session_state.candidates = []
if "selected_player_id" not in st.session_state:
    st.session_state.selected_player_id = DEFAULT_PLAYER_ID

if st.sidebar.button("Search"):
    with st.spinner("Searching players..."):
        st.session_state.candidates = search_players(query)

if st.session_state.candidates:
    selected = st.sidebar.selectbox(
        "Select player",
        st.session_state.candidates,
        format_func=lambda x: f"{x['name']} ({x.get('team','')}) - id: {x['id']}",
    )
    if st.sidebar.button("Load Profile"):
        st.session_state.selected_player_id = int(selected["id"])
else:
    st.sidebar.caption("Tip: Click Search to get matching players.")

player_id = int(st.session_state.selected_player_id)

with st.spinner(f"Loading profile for player_id={player_id}..."):
    profile = load_profile(player_id)

if not profile:
    st.stop()

# ===== Layout  =====
top_left, top_right = st.columns([2, 3])

with top_left:
    render_player_info_box(profile)

with top_right:
    render_rankings_and_teams_box(profile)

st.divider()

render_recent_performance(profile)
st.divider()

render_career_summaries(profile)
st.divider()

render_timeline(profile.get("timeline", pd.DataFrame()))
st.divider()

render_bio(profile.get("bio", ""))
