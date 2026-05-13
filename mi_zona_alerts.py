"""
Mi Zona — daily alerts emailer.

Runs as a scheduled job: for each user with Mi Zona criteria stored in
``user_preferences``, finds the listings that

  1. appeared on Idealista since the user's last alert run, AND
  2. match the user's criteria (same filter as the dashboard's Mi Zona
     page — same module is reused so the two stay in sync), AND
  3. score a buyer-side margin ≥ ``--min-margin`` after running them
     through the Offer Engine.

If any survive, sends a Gmail email with the top matches and
advances the user's "watermark" so the same listings don't get
re-alerted on subsequent runs.

State
-----
Two rows in ``user_preferences`` per user:
  * ``mi_zona_criteria``       — the dict the dashboard reads/writes.
  * ``mi_zona_alerts_watermark`` — ``{"date": "YYYY-MM-DD"}`` of the
    newest ``first_seen_date`` already considered.

Bootstrap: on the very first run for a user, the watermark is set to
*today* without sending email.  Otherwise the first run would dump
hundreds of historical matches and looks like spam.

Idempotency: the watermark advances every run that finds *any* new
listings (even if zero matched the criteria) so we never re-query the
same window twice.  An alert email is only sent when matches with
``margin >= --min-margin`` exist.

CLI
---
::

    # Default: alert every user that has criteria configured.
    python mi_zona_alerts.py

    # One specific user.
    python mi_zona_alerts.py --user luis

    # Dry-run: do everything except send the email and advance the watermark.
    python mi_zona_alerts.py --dry-run

    # Threshold tuning.
    python mi_zona_alerts.py --min-margin 7.5 --max-alerts 5
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
from datetime import date, datetime, timedelta
from typing import Any, Optional

import pandas as pd

from db.connection import get_connection
from user_preferences import get_user_pref, set_user_pref


# ──────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────


PREF_CRITERIA_KEY  = "mi_zona_criteria"
PREF_WATERMARK_KEY = "mi_zona_alerts_watermark"

DEFAULT_MIN_MARGIN_PCT = 5.0
DEFAULT_MAX_ALERTS     = 10


# ──────────────────────────────────────────────────────────────────────
# Watermark helpers
# ──────────────────────────────────────────────────────────────────────


def _to_date(v: Any) -> Optional[date]:
    if v is None or v == "":
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        return date.fromisoformat(v.split(" ", 1)[0].split("T", 1)[0])
    return None


def _read_watermark(username: str) -> Optional[date]:
    data = get_user_pref(username, PREF_WATERMARK_KEY) or {}
    return _to_date(data.get("date"))


def _write_watermark(username: str, d: date) -> None:
    set_user_pref(username, PREF_WATERMARK_KEY, {"date": d.isoformat()})


# ──────────────────────────────────────────────────────────────────────
# Data loading
# ──────────────────────────────────────────────────────────────────────


def _load_active_listings_since(watermark: date) -> pd.DataFrame:
    """Active listings whose ``first_seen_date`` is strictly *after* watermark.

    Strict ``>`` so we don't replay the boundary day on each run.
    """
    from db.dialect import current_date, julianday_diff
    days_expr = julianday_diff(
        f"COALESCE(last_seen_date, {current_date()})",
        f"COALESCE(first_seen_date, last_seen_date, {current_date()})",
    )
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT listing_id, title, url, price, distrito, barrio,
                   size_sqm, rooms, floor, seller_type, status,
                   first_seen_date, last_seen_date,
                   {days_expr} AS days_on_market,
                   CASE WHEN size_sqm > 0
                        THEN ROUND(CAST(price * 1.0 / size_sqm AS DECIMAL), 2)
                        ELSE NULL END AS price_per_sqm,
                   (SELECT COUNT(*) FROM price_history ph
                    WHERE ph.listing_id = listings.listing_id AND ph.change_amount < 0
                   ) AS num_drops,
                   COALESCE((SELECT ABS(SUM(change_percent)) FROM price_history ph
                             WHERE ph.listing_id = listings.listing_id AND ph.change_percent < 0
                            ), 0) AS total_drop_pct
            FROM listings
            WHERE status = 'active' AND first_seen_date > ?
            """,
            (watermark.isoformat(),),
        )
        rows = [dict(r) for r in cur.fetchall()]
    return pd.DataFrame(rows)


def _load_all_active_for_comparables() -> pd.DataFrame:
    """Full universe of active listings — needed by ``estimate_fair_price``."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT listing_id, title, url, price, distrito, barrio,
                   size_sqm, rooms, floor, seller_type, status,
                   first_seen_date, last_seen_date
            FROM listings
            WHERE status = 'active'
        """)
        rows = [dict(r) for r in cur.fetchall()]
    df = pd.DataFrame(rows)
    if not df.empty:
        df["price_per_sqm"] = df["price"] / df["size_sqm"]
    return df


def _load_notarial_by_distrito() -> dict[str, float]:
    """Latest-period notarial €/m² per distrito — keyed for fast lookup."""
    try:
        from database import get_notarial_prices
        rows = get_notarial_prices() or []
    except Exception:
        return {}
    latest: dict[str, tuple[int, float]] = {}
    for r in rows:
        d   = r["distrito"]
        per = r.get("periodo", 0) or 0
        if d not in latest or per > latest[d][0]:
            latest[d] = (per, float(r["precio_m2"]))
    return {d: v[1] for d, v in latest.items()}


# ──────────────────────────────────────────────────────────────────────
# Filtering + offer computation (reused from the dashboard module)
# ──────────────────────────────────────────────────────────────────────


def _apply_criteria_and_compute(
    candidates_df:        pd.DataFrame,
    criteria:             dict,
    all_active_df:        pd.DataFrame,
    notarial_by_distrito: dict[str, float],
) -> list[dict]:
    """Filter ``candidates_df`` by criteria and compute one offer per row.

    Delegates to ``tabs.mi_zona_tab`` to keep the alerts pipeline and
    the dashboard's "Mi Zona" page in lockstep — if the filter logic
    or offer computation changes there, the email reflects it
    automatically.
    """
    if candidates_df.empty:
        return []
    from tabs.mi_zona_tab import _apply_criteria, _compute_offers
    matching = _apply_criteria(candidates_df, criteria)
    if matching.empty:
        return []
    return _compute_offers(matching, all_active_df, notarial_by_distrito)


# ──────────────────────────────────────────────────────────────────────
# Email HTML
# ──────────────────────────────────────────────────────────────────────


def _format_factors_inline(factors: list[tuple[str, float]]) -> str:
    """Short pipe-separated factor summary for the email rows."""
    if not factors:
        return "<span style='color:#94a3b8;'>sin factores aplicados</span>"
    bits = [
        f"<span style='color:#475569;'>{lbl}</span> "
        f"<span style='color:#dc2626;font-weight:600;'>{pct:+.1f}%</span>"
        for lbl, pct in factors[:3]
    ]
    return " · ".join(bits)


def _format_match_row(m: dict) -> str:
    margin   = m["margin_pct"]
    accent   = "#16a34a" if margin >= 8 else ("#0891b2" if margin >= 3 else "#94a3b8")
    title    = (m["title"] or "—")[:80]
    url      = m["url"] or "#"
    size     = m["size_sqm"]
    rooms    = m["rooms"]
    seller   = m["seller_type"] or "—"
    days     = m["days"]

    return (
        f"<tr><td style='padding:14px 16px;border-bottom:1px solid #e2e8f0;'>"
        f"<div style='font-weight:600;font-size:15px;color:#0f172a;'>"
        f"<a href='{url}' style='color:#0f172a;text-decoration:none;'>{title}</a>"
        f"</div>"
        f"<div style='font-size:13px;color:#64748b;margin-top:2px;'>"
        f"{m['barrio']} · {m['distrito']} · "
        f"{size:.0f} m² · {int(rooms) if rooms else '—'}h · "
        f"{seller} · DOM {days}d"
        f"</div>"
        f"<div style='margin-top:8px;font-size:14px;'>"
        f"<span style='color:#475569;'>Pedido <b>€{m['price']:,}</b> · </span>"
        f"<span style='color:{accent};font-weight:700;'>"
        f"Oferta sugerida €{m['suggested_mid']:,} ({margin:+.1f}%)"
        f"</span>"
        f"</div>"
        f"<div style='margin-top:4px;font-size:12px;'>"
        f"{_format_factors_inline(m.get('factors') or [])}"
        f"</div>"
        f"</td></tr>"
    )


def _build_email_html(
    matches:      list[dict],
    criteria:     dict,
    username:     str,
    since:        date,
) -> str:
    """Inline-styled HTML — Gmail strips most things from <style>."""
    seller_label = "particular" if not criteria.get("seller_any", True) else "cualquier vendedor"
    n_barrios    = len(criteria.get("barrios") or [])
    barrios_chip = ", ".join(criteria.get("barrios") or [])

    rows = "".join(_format_match_row(m) for m in matches)

    return (
        "<html><body style='font-family:-apple-system,Segoe UI,Roboto,sans-serif;"
        "background:#f8fafc;margin:0;padding:24px;'>"
        "<table style='max-width:680px;margin:0 auto;background:white;border-radius:12px;"
        "border:1px solid #e2e8f0;border-collapse:collapse;width:100%;'>"
        # Header
        "<tr><td style='padding:20px 24px;background:#0f172a;border-radius:12px 12px 0 0;color:white;'>"
        f"<div style='font-size:13px;color:#94a3b8;letter-spacing:0.5px;text-transform:uppercase;'>Mi Zona</div>"
        f"<div style='font-size:22px;font-weight:800;margin-top:4px;'>"
        f"{len(matches)} oportunidad{'es' if len(matches) != 1 else ''} nueva{'s' if len(matches) != 1 else ''} en tus barrios"
        "</div>"
        f"<div style='font-size:13px;color:#cbd5e1;margin-top:8px;'>"
        f"Desde {since.isoformat()} · {n_barrios} barrios · ≤ €{criteria.get('max_price', 0):,} · "
        f"≥ {criteria.get('min_size', 0)} m² · "
        f"{criteria.get('min_rooms', 0)}-{criteria.get('max_rooms', 99)} habitaciones · {seller_label}"
        "</div>"
        f"<div style='font-size:12px;color:#94a3b8;margin-top:6px;'>{barrios_chip}</div>"
        "</td></tr>"
        # Matches
        f"{rows}"
        # Footer
        "<tr><td style='padding:16px 24px;background:#f1f5f9;border-radius:0 0 12px 12px;"
        "color:#64748b;font-size:12px;text-align:center;'>"
        "Generado por el motor de Mi Zona · "
        "Cada match incluye el rango sugerido por el offer engine basado en "
        "comparables del barrio + tu palanca de negociación (DOM, bajadas, vendedor)."
        "</td></tr>"
        "</table>"
        "</body></html>"
    )


# ──────────────────────────────────────────────────────────────────────
# Sending
# ──────────────────────────────────────────────────────────────────────


def _send(html: str, subject: str, recipient_override: Optional[str]) -> bool:
    """Use ``email_report.send_report`` for SMTP plumbing."""
    import email_report
    if recipient_override:
        # send_report reads the module-level constant; override it for
        # the call.  Restore after so the daily-scraper email keeps
        # going to its hardcoded recipient.
        original = email_report.RECIPIENT_EMAIL
        email_report.RECIPIENT_EMAIL = recipient_override
        try:
            return email_report.send_report(html, subject)
        finally:
            email_report.RECIPIENT_EMAIL = original
    return email_report.send_report(html, subject)


# ──────────────────────────────────────────────────────────────────────
# Orchestration
# ──────────────────────────────────────────────────────────────────────


def _discover_users() -> list[str]:
    """All usernames with a stored ``mi_zona_criteria`` row in user_preferences."""
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT DISTINCT username FROM user_preferences WHERE key = ?",
                (PREF_CRITERIA_KEY,),
            )
            return [r[0] for r in cur.fetchall()]
    except Exception:
        # Table missing (fresh deploy, migration not run) — nothing to
        # do.  Don't crash the workflow over it.
        return []


def run_alerts_for_user(
    username:           str,
    *,
    min_margin:         float,
    max_alerts:         int,
    dry_run:            bool,
    recipient_override: Optional[str],
) -> int:
    """Process one user.  Returns the number of matches alerted on."""
    criteria = get_user_pref(username, PREF_CRITERIA_KEY)
    if not criteria or not criteria.get("barrios"):
        print(f"  · [{username}] no criteria configured — skipping")
        return 0

    wm = _read_watermark(username)

    # First-run bootstrap: silently set the watermark to today.  Without
    # this every existing matching active listing would be flagged as
    # "new" and the user would get a flood of historical hits.
    if wm is None:
        today = date.today()
        if not dry_run:
            _write_watermark(username, today)
        print(f"  · [{username}] first run — watermark set to {today}, no email sent")
        return 0

    candidates = _load_active_listings_since(wm)
    if candidates.empty:
        print(f"  · [{username}] no new listings since {wm}")
        return 0

    # Always advance the watermark *before* filtering, so an empty
    # criteria match doesn't make us re-query the same window forever.
    new_wm = max(
        _to_date(d) for d in candidates["first_seen_date"]
        if _to_date(d) is not None
    )

    all_active = _load_all_active_for_comparables()
    notarial   = _load_notarial_by_distrito()
    offers     = _apply_criteria_and_compute(candidates, criteria, all_active, notarial)

    matches = [m for m in offers if m["margin_pct"] >= min_margin]
    matches.sort(key=lambda m: -m["margin_pct"])
    matches = matches[:max_alerts]

    if not matches:
        print(
            f"  · [{username}] {len(candidates)} new listings since {wm}, "
            f"0 with margin ≥ {min_margin}% — silent, watermark → {new_wm}"
        )
        if not dry_run:
            _write_watermark(username, new_wm)
        return 0

    html    = _build_email_html(matches, criteria, username, since=wm)
    subject = f"🎯 Mi Zona: {len(matches)} oportunidad{'es' if len(matches) != 1 else ''} en tus barrios"

    if dry_run:
        print(f"  · [{username}] DRY-RUN: would send {len(matches)} matches "
              f"(threshold {min_margin}% margin), watermark stays at {wm}")
        for m in matches:
            print(f"      {m['margin_pct']:+5.1f}%  €{m['price']:,} → €{m['suggested_mid']:,}  "
                  f"{m['barrio']}  {m['size_sqm']:.0f}m²  {m['rooms']}h")
        return len(matches)

    ok = _send(html, subject, recipient_override)
    if ok:
        _write_watermark(username, new_wm)
        print(f"  · [{username}] ✅ {len(matches)} matches enviados · watermark → {new_wm}")
    else:
        print(f"  · [{username}] ❌ envío falló · watermark NO avanzado (reintentará)")
    return len(matches) if ok else 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--user",        default=None,
                   help="alert one user only (default: all users with criteria configured)")
    p.add_argument("--min-margin",  type=float, default=DEFAULT_MIN_MARGIN_PCT,
                   help=f"minimum offer margin %% to include (default {DEFAULT_MIN_MARGIN_PCT})")
    p.add_argument("--max-alerts",  type=int, default=DEFAULT_MAX_ALERTS,
                   help=f"maximum matches per email (default {DEFAULT_MAX_ALERTS})")
    p.add_argument("--recipient",   default=os.environ.get("MI_ZONA_ALERT_EMAIL"),
                   help="override email recipient (default: env MI_ZONA_ALERT_EMAIL "
                        "or email_report.RECIPIENT_EMAIL)")
    p.add_argument("--dry-run",     action="store_true",
                   help="walk the pipeline and print what would be sent — don't send, "
                        "don't advance any watermarks")
    args = p.parse_args()

    users = [args.user] if args.user else _discover_users()
    if not users:
        print("No Mi Zona criteria configured for any user. Nothing to alert.")
        return 0

    print(f"Mi Zona alerts: {len(users)} user(s) · "
          f"threshold {args.min_margin}% margin · max {args.max_alerts} matches each"
          + (" · DRY RUN" if args.dry_run else ""))

    sent_total = 0
    for u in users:
        try:
            sent_total += run_alerts_for_user(
                u,
                min_margin         = args.min_margin,
                max_alerts         = args.max_alerts,
                dry_run            = args.dry_run,
                recipient_override = args.recipient,
            )
        except Exception as exc:
            print(f"  · [{u}] uncaught error: {exc}")
            traceback.print_exc()
            continue

    print(f"\nTotal matches alerted: {sent_total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
