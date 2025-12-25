import time
import streamlit as st


def _ensure_authenticated():
    """
    Global auth guard.

    - Triggers Google OAuth via st.login().
    - Enforces session TTL from secrets.
    - Checks user email against whitelist.
    - Stops execution on unauthenticated / unauthorized users.
    - Returns st.user for valid sessions.
    """
    app_cfg = st.secrets["app"]
    allowed_users = [u.lower() for u in app_cfg["allowed_users"]]
    session_ttl_hours = app_cfg.get("session_ttl_hours", 12)

    # 1. First visit: no authenticated user yet.
    if not getattr(st.user, "is_logged_in", False):
        _render_login_screen()
        st.stop()

    # 2. User came back from Google with a token.
    user_email = (getattr(st.user, "email", "") or "").strip().lower()
    user_name = getattr(st.user, "name", "") or ""

    # 3. Session TTL (try to use exp, fallback to manual timer).
    now = time.time()
    token_exp = getattr(st.user, "exp", None)

    if token_exp is not None:
        hours_left = (token_exp - now) / 3600
        if hours_left <= 0:
            _render_expired_screen()
            st.stop()
    else:
        if "login_time" not in st.session_state:
            st.session_state.login_time = now
        elapsed_hours = (now - st.session_state.login_time) / 3600
        if elapsed_hours > session_ttl_hours:
            _render_expired_screen()
            st.stop()

    # 4. Whitelist
    if user_email not in allowed_users:
        _render_forbidden_screen(user_email)
        st.stop()

    # 5. All checks passed
    return st.user


def _render_login_screen():
    """Unauthenticated user: simple welcome + login button."""
    st.title("Analytics Platform Login")
    st.subheader("Sign in with your Google account to continue.")

    st.write(
        "Access to this analytics dashboard is restricted to authorized users. "
        "Use your corporate Google account to sign in."
    )

    col1, col2, col3 = st.columns([1, 2, 1])
    with col1:
        if st.button("Sign in with Google", type="primary", use_container_width=True):
            st.login()


def _render_expired_screen():
    """Session expired view."""
    st.title("Session expired")
    st.write("Your login session is no longer valid. Please sign in again.")

    if st.button("Sign in again", type="primary"):
        st.session_state.clear()
        st.logout()


def _render_forbidden_screen(user_email: str):
    """User is authenticated with Google but not on the whitelist."""
    st.title("Access denied")

    st.error(
        f"The account {user_email or '(unknown email)'} is not allowed to access this dashboard."
    )
    st.write(
        "If you believe this is a mistake, contact the system administrator "
        "to have your email added to the access list."
    )

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Sign out"):
            st.session_state.clear()
            st.logout()
    with col2:
        if st.button("Sign in with another account", type="primary"):
            st.session_state.clear()
            st.logout()
            st.login()


def auth_page():
    """
    Standalone auth page.

    When the user is not authenticated, shows the login form.
    When authenticated and allowed, shows a short welcome block.
    """
    user = _ensure_authenticated()

    st.title("Authentication status")
    st.success(f"You are signed in as {user.name} ({user.email}).")

    if st.button("Sign out"):
        st.session_state.clear()
        st.logout()
        st.rerun()


# helper
ensure_authenticated = _ensure_authenticated