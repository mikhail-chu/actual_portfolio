import streamlit as st
from pages.auth_page import ensure_authenticated

def auth_guard():
    """
    Thin wrapper around the central auth guard.

    Uses ensure_authenticated() from auth_page module so that the
    authentication / TTL / whitelist logic is defined in a single place.
    """
    user = ensure_authenticated()
    return user