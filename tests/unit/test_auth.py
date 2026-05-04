"""
Unit tests for auth.py — bcrypt hashing + verification + plaintext fallback.

The Streamlit-aware bits (login form, session state, lockout) are not
covered here because they need a Streamlit script context to exercise
properly. The pure crypto helpers ARE covered: they are the security-
critical primitives and we want to know immediately if any of them
silently regresses.
"""

from __future__ import annotations

import pytest

from auth import (
    hash_password,
    _verify_bcrypt,
    _verify_plaintext,
)

pytestmark = pytest.mark.unit


# ──────────────────────────────────────────────────────────────────────────
# hash_password / _verify_bcrypt
# ──────────────────────────────────────────────────────────────────────────


class TestHashPassword:
    def test_hash_does_not_contain_plaintext(self):
        h = hash_password("hunter2")
        assert "hunter2" not in h

    def test_hash_uses_bcrypt_format(self):
        h = hash_password("any-password")
        # bcrypt v2b prefix
        assert h.startswith("$2b$")

    def test_hash_is_ascii_safe(self):
        h = hash_password("a-strong-password")
        h.encode("ascii")  # must not raise

    def test_two_hashes_of_same_password_differ(self):
        h1 = hash_password("samepass")
        h2 = hash_password("samepass")
        assert h1 != h2

    def test_empty_password_rejected(self):
        with pytest.raises(ValueError):
            hash_password("")

    def test_non_string_password_rejected(self):
        with pytest.raises(ValueError):
            hash_password(None)  # type: ignore[arg-type]


class TestVerifyBcrypt:
    def test_correct_password_verifies(self):
        h = hash_password("MyP4ssw0rd!")
        assert _verify_bcrypt("MyP4ssw0rd!", h) is True

    def test_wrong_password_does_not_verify(self):
        h = hash_password("correct")
        assert _verify_bcrypt("wrong", h) is False

    def test_unicode_password_round_trip(self):
        h = hash_password("contraseña-ñ-€")
        assert _verify_bcrypt("contraseña-ñ-€", h) is True

    def test_corrupted_hash_returns_false(self):
        for bad in ("", "not-a-hash", "$2b$wrong", "x" * 60):
            assert _verify_bcrypt("anything", bad) is False

    def test_non_string_inputs_return_false(self):
        h = hash_password("ok")
        # bcrypt raises on non-bytes/str; our wrapper must swallow
        assert _verify_bcrypt(None, h) is False  # type: ignore[arg-type]
        assert _verify_bcrypt("ok", None) is False  # type: ignore[arg-type]


# ──────────────────────────────────────────────────────────────────────────
# _verify_plaintext (legacy fallback)
# ──────────────────────────────────────────────────────────────────────────


class TestVerifyPlaintext:
    def test_match(self):
        assert _verify_plaintext("foo", "foo") is True

    def test_mismatch(self):
        assert _verify_plaintext("foo", "bar") is False

    def test_length_mismatch_does_not_raise(self):
        # secrets.compare_digest accepts unequal-length inputs gracefully
        assert _verify_plaintext("a", "abcdefg") is False

    def test_non_string_rejected(self):
        assert _verify_plaintext(None, "x") is False  # type: ignore[arg-type]
        assert _verify_plaintext("x", None) is False  # type: ignore[arg-type]
        assert _verify_plaintext(42, "x") is False    # type: ignore[arg-type]
