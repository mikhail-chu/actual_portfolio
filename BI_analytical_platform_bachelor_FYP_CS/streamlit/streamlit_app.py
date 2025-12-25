import streamlit as st

from pages.auth_page import auth_page
from pages.plots_page import plots_page
from pages.configurator_page import configurator_page

from pages.auth_guard import auth_guard

st.set_page_config(page_title="Analytics", layout="wide")

user = auth_guard()

st.sidebar.caption(f"Logged in as: {user.name} ({user.email})")

pages = {
    "Main Menu": [
        st.Page(auth_page,         title="Authentication", icon=":material/lock:"),
        st.Page(configurator_page, title="Configurator",   icon=":material/tune:"),
        st.Page(plots_page,        title="Plots",          icon=":material/analytics:"),
    ]
}

pg = st.navigation(pages, position="sidebar", expanded=True)
pg.run()