"""
Unit tests for the Oxylabs Web Scraper API tier.

Focus is on the retry-on-empty-payload behaviour that the 2026-05-31
bench surfaced: Oxylabs returns HTTP 200 with content="" when its
internal anti-bot rotation runs out of retries, and we must NOT count
that as a real success.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import scraper

pytestmark = pytest.mark.unit


# Minimal Idealista-shaped HTML that passes the parse-listing pipeline
# and the _is_challenge_page guard.
_HTML_OK = "<html>" + "x" * 3000 + """
  <article class="item" data-element-id="111"><a class="item-link" href="/i/111/">a</a><span class="item-price">300.000</span></article>
  <article class="item" data-element-id="222"><a class="item-link" href="/i/222/">b</a><span class="item-price">400.000</span></article>
""" + "</html>"


def _ok_response(html: str = _HTML_OK):
    """Build a fake requests.Response for Oxylabs' API shape."""
    r = MagicMock()
    r.status_code = 200
    r.content = b"x"  # non-empty so the .json() path is taken
    r.json.return_value = {'results': [{'content': html}]}
    return r


def _empty_response():
    r = MagicMock()
    r.status_code = 200
    r.content = b"x"
    r.json.return_value = {'results': [{'content': ''}]}
    return r


def _status_response(code: int):
    r = MagicMock()
    r.status_code = code
    r.content = b"err"
    r.json.return_value = {}
    return r


# ──────────────────────────────────────────────────────────────────────────
# Successful first attempt
# ──────────────────────────────────────────────────────────────────────────

def test_oxylabs_returns_html_on_first_success():
    scraper.oxylabs_counter.update(successful=0, failed=0, total=0)
    with patch.object(scraper, 'OXYLABS_USER', 'u'), \
         patch.object(scraper, 'OXYLABS_PASS', 'p'), \
         patch.object(scraper.requests, 'post', return_value=_ok_response()) as mock_post:
        html, status = scraper._fetch_via_oxylabs("https://idealista.com/x")

    assert status == 200
    assert html and "data-element-id" in html
    assert mock_post.call_count == 1
    assert scraper.oxylabs_counter['successful'] == 1
    assert scraper.oxylabs_counter['failed'] == 0


# ──────────────────────────────────────────────────────────────────────────
# Empty payload retries — the bench-confirmed failure mode
# ──────────────────────────────────────────────────────────────────────────

def test_oxylabs_retries_on_empty_payload_then_succeeds():
    scraper.oxylabs_counter.update(successful=0, failed=0, total=0)
    sequence = [_empty_response(), _empty_response(), _ok_response()]
    with patch.object(scraper, 'OXYLABS_USER', 'u'), \
         patch.object(scraper, 'OXYLABS_PASS', 'p'), \
         patch.object(scraper, 'OXYLABS_MAX_RETRIES', 3), \
         patch.object(scraper.requests, 'post', side_effect=sequence) as mock_post:
        html, status = scraper._fetch_via_oxylabs("https://idealista.com/x")

    assert status == 200
    assert mock_post.call_count == 3
    assert scraper.oxylabs_counter['successful'] == 1
    assert scraper.oxylabs_counter['failed'] == 2
    assert scraper.oxylabs_counter['total'] == 3


def test_oxylabs_all_empty_payloads_returns_failure():
    """All retries return empty payload → we return failure, not a false 200."""
    scraper.oxylabs_counter.update(successful=0, failed=0, total=0)
    with patch.object(scraper, 'OXYLABS_USER', 'u'), \
         patch.object(scraper, 'OXYLABS_PASS', 'p'), \
         patch.object(scraper, 'OXYLABS_MAX_RETRIES', 3), \
         patch.object(scraper.requests, 'post', return_value=_empty_response()) as mock_post:
        html, status = scraper._fetch_via_oxylabs("https://idealista.com/x")

    assert html is None
    # Last upstream response was 200 (empty), but we report the last_status
    # which is still 200 because Oxylabs technically said OK.
    assert status == 200
    assert mock_post.call_count == 3
    assert scraper.oxylabs_counter['successful'] == 0
    assert scraper.oxylabs_counter['failed'] == 3


# ──────────────────────────────────────────────────────────────────────────
# 404 from Idealista is definitive — no retry
# ──────────────────────────────────────────────────────────────────────────

def test_oxylabs_404_is_terminal_no_retry(tmp_path, monkeypatch):
    scraper.oxylabs_counter.update(successful=0, failed=0, total=0)
    # Redirect the 404 log so we don't touch the real file
    monkeypatch.chdir(tmp_path)
    with patch.object(scraper, 'OXYLABS_USER', 'u'), \
         patch.object(scraper, 'OXYLABS_PASS', 'p'), \
         patch.object(scraper, 'OXYLABS_MAX_RETRIES', 3), \
         patch.object(scraper.requests, 'post', return_value=_status_response(404)) as mock_post:
        html, status = scraper._fetch_via_oxylabs("https://idealista.com/x")

    assert html is None
    assert status == 404
    assert mock_post.call_count == 1  # no retry on 404


# ──────────────────────────────────────────────────────────────────────────
# Missing credentials — clean skip
# ──────────────────────────────────────────────────────────────────────────

def test_oxylabs_returns_immediately_when_creds_missing():
    scraper.oxylabs_counter.update(successful=0, failed=0, total=0)
    with patch.object(scraper, 'OXYLABS_USER', None), \
         patch.object(scraper, 'OXYLABS_PASS', None), \
         patch.object(scraper.requests, 'post') as mock_post:
        html, status = scraper._fetch_via_oxylabs("https://idealista.com/x")

    assert (html, status) == (None, 0)
    mock_post.assert_not_called()
    assert scraper.oxylabs_counter['total'] == 0


# ──────────────────────────────────────────────────────────────────────────
# Cost estimate exposes Oxylabs tier
# ──────────────────────────────────────────────────────────────────────────

def test_cost_estimate_includes_oxylabs_tier():
    scraper.oxylabs_counter.update(successful=10, failed=2, total=12)
    scraper.request_counter.update(successful=0, failed=0, total=0)
    scraper.residential_counter.update(successful=0, failed=0, total=0)
    scraper.direct_counter.update(successful=0, failed=0, total=0)

    est = scraper.get_brightdata_cost_estimate()

    assert est['oxylabs_requests'] == 12
    assert est['oxylabs_successful'] == 10
    expected_cost = 12 * scraper.OXYLABS_COST_PER_REQ
    assert abs(est['oxylabs_cost_usd'] - expected_cost) < 1e-6
    assert abs(est['estimated_cost_usd'] - expected_cost) < 1e-6
