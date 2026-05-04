#!/usr/bin/env python3
"""
CLI helper to generate bcrypt password hashes for the
``[auth.users_hashed]`` block in ``.streamlit/secrets.toml``.

Usage::

    python gen_password_hash.py

    🔐 Generate bcrypt hash for the Streamlit dashboard

    Username: luis
    Password:
    Confirm:

    Add this to .streamlit/secrets.toml:

        [auth.users_hashed]
        luis = "$2b$12$KxJ1r7..."

    Then update Streamlit Cloud secrets with the same content.

The plaintext password is NEVER printed back to the terminal.
``getpass`` reads it without echo.

If you want to add several users in one go, just re-run the script.
"""

from __future__ import annotations

import getpass
import re
import sys

from auth import hash_password


_MIN_LEN          = 12
_RECOMMENDED_LEN  = 16
_USERNAME_RE      = re.compile(r"^[A-Za-z0-9_.-]{2,32}$")


def _read_username() -> str | None:
    raw = input("Username: ").strip()
    if not raw:
        print("✖ Username cannot be empty.", file=sys.stderr)
        return None
    if not _USERNAME_RE.fullmatch(raw):
        print(
            "✖ Username must be 2-32 chars, only letters, digits, '_', '.', '-'.",
            file=sys.stderr,
        )
        return None
    return raw


def _read_password() -> str | None:
    pw = getpass.getpass("Password: ")
    confirm = getpass.getpass("Confirm:  ")
    if pw != confirm:
        print("✖ Passwords do not match.", file=sys.stderr)
        return None
    if len(pw) < _MIN_LEN:
        print(
            f"✖ Password is {len(pw)} chars; minimum is {_MIN_LEN}.",
            file=sys.stderr,
        )
        return None
    if len(pw) < _RECOMMENDED_LEN:
        print(
            f"⚠️  Password is shorter than {_RECOMMENDED_LEN} chars. "
            f"Consider using a passphrase or a generator.",
            file=sys.stderr,
        )
    return pw


def main() -> int:
    print("🔐 Generate bcrypt hash for the Streamlit dashboard")
    print()

    username = _read_username()
    if username is None:
        return 1

    password = _read_password()
    if password is None:
        return 1

    digest = hash_password(password)

    print()
    print("Add this to .streamlit/secrets.toml (and Streamlit Cloud secrets):")
    print()
    print("    [auth.users_hashed]")
    print(f'    {username} = "{digest}"')
    print()
    print("Once verified working, delete the legacy [auth.users] block.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nAborted.", file=sys.stderr)
        sys.exit(130)
