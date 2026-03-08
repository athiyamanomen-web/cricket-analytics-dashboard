import streamlit as st
import runpy

st.set_page_config(
    page_title="Cricket Analytics Dashboard",
    page_icon="🏏",
    layout="wide"
)

st.sidebar.title("🏏 Cricket Dashboard")

page = st.sidebar.radio(
    "Select Section",
    [
        "Player Profile",
        "Live Matches",
        "SQL Operations",
        "CRUD Operations"
    ]
)

if page == "Player Profile":
    runpy.run_path("APP.py")

elif page == "Live Matches":
    runpy.run_path("Live-match.py")

elif page == "SQL Operations":
    runpy.run_path("SQL+CRUD.py", init_globals={"SECTION": "SQL"})

elif page == "CRUD Operations":
    runpy.run_path("SQL+CRUD.py", init_globals={"SECTION": "CRUD"})