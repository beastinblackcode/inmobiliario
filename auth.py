"""
Authentication module for the Streamlit dashboard.

Replaces the legacy plaintext-comparison check that lived inline in app.py
with a hardened implementation:

    1. Passwords stored as bcrypt hashes in
       ``st.secrets["auth"]["users_hashed"]``.
    2. Constant-time verification (bcrypt.checkpw + secrets.compare_digest
       for the legacy plaintext fallback).
    3. Per-session rate limit: 5 failed attempts → 5-minute lockout.
    4. Session expiry: 12 hours since login → forced re-auth.
    5. Failed attempts logged to stderr with timestamp and intended
       username so brute-force shows up in Streamlit Cloud logs.

Migration path
--------------

Existing deployments using ``[auth.users]`` (plaintext) keep working, but
each successful login emits a deprecation warning to stderr.  To migrate:

    1. Run ``python gen_password_hash.py`` for each user.
    2. Replace the ``[auth.users]`` block in ``.streamlit/secrets.toml``
       with ``[auth.users_hashed]`` containing the bcrypt hashes.
    3. Update Streamlit Cloud secrets with the same content.
    4. Once verified, delete the ``[auth.users]`` block.
"""

from __future__ import annotations

import secrets as _secrets
import sys
from datetime import datetime, timedelta, timezone
from typing import Tuple

import bcrypt
import streamlit as st


# ── Tunables ───────────────────────────────────────────────────────────────
_MAX_ATTEMPTS    = 5
_LOCKOUT_MINUTES = 5
_SESSION_HOURS   = 12


# ── Pure helpers (no Streamlit dependency in their bodies) ────────────────


def hash_password(plain: str) -> str:
    """
    Compute a bcrypt hash for storage in ``secrets.toml``.

    Used by ``gen_password_hash.py`` only — not at request time.
    """
    if not isinstance(plain, str) or not plain:
        raise ValueError("password must be a non-empty string")
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("ascii")


def _verify_bcrypt(plain: str, hashed: str) -> bool:
    """Constant-time bcrypt verification.  Returns False on any error."""
    if not isinstance(plain, str) or not isinstance(hashed, str):
        return False
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("ascii"))
    except (ValueError, TypeError, UnicodeError):
        return False


def _verify_plaintext(plain: str, stored_plain: str) -> bool:
    """
    Constant-time fallback for the legacy ``[auth.users]`` plaintext
    block.  Kept only to avoid breaking deployments mid-migration.
    """
    if not isinstance(plain, str) or not isinstance(stored_plain, str):
        return False
    return _secrets.compare_digest(plain, stored_plain)


# ── Internal helpers (touch session_state) ────────────────────────────────


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _log(username: str, success: bool, reason: str = "") -> None:
    icon = "✅" if success else "❌"
    suffix = f" ({reason})" if reason else ""
    print(
        f"[{_now().isoformat()}] {icon} login attempt: user={username!r}{suffix}",
        file=sys.stderr,
    )


def _is_locked_out() -> Tuple[bool, int]:
    """Return ``(locked, seconds_remaining)``."""
    until = st.session_state.get("_auth_lockout_until")
    if until is None:
        return False, 0
    remaining = (until - _now()).total_seconds()
    if remaining <= 0:
        # Cool-off ended — reset counters
        st.session_state.pop("_auth_lockout_until", None)
        st.session_state["_auth_failed_count"] = 0
        return False, 0
    return True, int(remaining) + 1


def _record_failure(username: str) -> None:
    count = st.session_state.get("_auth_failed_count", 0) + 1
    st.session_state["_auth_failed_count"] = count
    _log(username, success=False, reason=f"attempt {count}/{_MAX_ATTEMPTS}")
    if count >= _MAX_ATTEMPTS:
        st.session_state["_auth_lockout_until"] = (
            _now() + timedelta(minutes=_LOCKOUT_MINUTES)
        )
        _log(username, success=False, reason=f"LOCKED for {_LOCKOUT_MINUTES} min")


def _record_success(username: str) -> None:
    st.session_state["_auth_failed_count"] = 0
    st.session_state.pop("_auth_lockout_until", None)
    st.session_state["password_correct"] = True
    st.session_state["current_user"] = username
    st.session_state["_auth_login_time"] = _now()
    _log(username, success=True)


def _session_expired() -> bool:
    login_time = st.session_state.get("_auth_login_time")
    if not isinstance(login_time, datetime):
        return False
    return _now() - login_time > timedelta(hours=_SESSION_HOURS)


def _try_credentials(username: str, password: str) -> bool:
    """
    Look up *username* in ``st.secrets["auth"]`` and verify *password*.

    Resolution order:
      1. ``[auth.users_hashed]``  — bcrypt (preferred)
      2. ``[auth.users]``         — plaintext (deprecated, warns)
      3. ``[auth] username/password``  — single-user plaintext (deprecated)
    """
    if "auth" not in st.secrets:
        return False
    cfg = st.secrets["auth"]

    # 1. Preferred: bcrypt
    hashed = (cfg.get("users_hashed") or {}).get(username)
    if hashed:
        return _verify_bcrypt(password, hashed)

    # 2. Legacy multi-user plaintext
    plain_users = cfg.get("users") or {}
    if username in plain_users:
        print(
            "⚠️  DEPRECATED: [auth.users] is plaintext. "
            "Migrate to [auth.users_hashed] using gen_password_hash.py.",
            file=sys.stderr,
        )
        return _verify_plaintext(password, plain_users[username])

    # 3. Legacy single-user plaintext
    if "username" in cfg and "password" in cfg and username == cfg["username"]:
        print(
            "⚠️  DEPRECATED: single-user [auth] plaintext. "
            "Migrate to [auth.users_hashed].",
            file=sys.stderr,
        )
        return _verify_plaintext(password, cfg["password"])

    return False


# ── Public API ────────────────────────────────────────────────────────────


def check_password() -> bool:
    """
    Render the login form and return True if the user is authenticated.

    Drop-in replacement for the previous in-app implementation in
    ``app.py``.  Adds bcrypt, rate limit, lockout, session expiry and
    audit logging.
    """
    # Session expiry — force re-auth after 12 h
    if st.session_state.get("password_correct") and _session_expired():
        for k in ("password_correct", "current_user", "_auth_login_time"):
            st.session_state.pop(k, None)
        st.warning("⏱️ Sesión expirada por inactividad. Vuelve a iniciar sesión.")

    if st.session_state.get("password_correct"):
        return True

    locked, remaining = _is_locked_out()

    def _on_submit() -> None:
        # Re-check lockout in case it expired between renders
        is_locked, _ = _is_locked_out()
        if is_locked:
            return
        username = st.session_state.get("username", "").strip()
        password = st.session_state.get("password", "")
        if not username or not password:
            _record_failure(username or "<empty>")
        elif _try_credentials(username, password):
            _record_success(username)
        else:
            _record_failure(username)
        # Always clear the password field
        st.session_state.pop("username", None)
        st.session_state.pop("password", None)

    st.markdown("## 🔐 Acceso al Dashboard")
    if locked:
        st.error(
            f"🔒 Demasiados intentos fallidos. Vuelve a intentarlo en "
            f"{remaining} segundos."
        )
        return False

    st.markdown("Por favor, introduce tus credenciales para acceder.")
    st.text_input("Usuario", key="username", autocomplete="username")
    st.text_input(
        "Contraseña", type="password", key="password",
        autocomplete="current-password",
    )
    st.button("Iniciar Sesión", on_click=_on_submit, type="primary")

    failed = st.session_state.get("_auth_failed_count", 0)
    if failed > 0:
        remaining_attempts = max(0, _MAX_ATTEMPTS - failed)
        st.error(
            f"😕 Usuario o contraseña incorrectos. "
            f"Intentos restantes: {remaining_attempts}."
        )

    return False
