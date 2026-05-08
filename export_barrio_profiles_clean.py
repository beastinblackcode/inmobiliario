"""
CLI wrapper for the Phase-2 clean ``barrios_profiles.json`` exporter.

Usage::

    python export_barrio_profiles_clean.py                         # stdout
    python export_barrio_profiles_clean.py -o barrios_profiles.json
    python export_barrio_profiles_clean.py -o barrios_profiles.json --verify

Replaces the ``barrios_profiles.json`` stub generated inline in
``.github/workflows/export-metrics.yml`` (Phase 1).  The new exporter
uses only Notarial CIEN + Open Data Madrid (no listings table).

The ``--verify`` flag fails the run with exit code 1 if any of the
following invariants are broken:

* the ``profiles`` dict is empty
* ``madrid_baseline.median_price_per_sqm`` is null
* the verdict mix collapses to a single label (i.e. no per-barrio
  differentiation reached the output)
* a profile carries any banned listing-derived key (``top_opportunities``,
  ``distribution``, ``avg_days_market``, ``active_count`` …)

These are the same kind of safety nets ``export_clean_metrics.py``
puts around its own output.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, List


_BANNED_PROFILE_KEYS = {
    "top_opportunities",
    "distribution",
    "active_count",
    "avg_days_market",
    "avg_size_sqm",
    "avg_rooms",
    "pct_with_drops",
    "avg_drops",
    "median_price",
}

_BANNED_KPI_KEYS = {
    "active_count",
    "avg_days_market",
    "avg_size_sqm",
    "avg_rooms",
    "pct_with_drops",
    "avg_drops",
    "median_price",
}


def _verify(payload: Dict) -> List[str]:
    """Return a list of human-readable problems (empty list = OK)."""
    issues: List[str] = []

    profiles = payload.get("profiles") or {}
    if not profiles:
        issues.append("profiles dict is empty")

    baseline = payload.get("madrid_baseline") or {}
    if not baseline.get("median_price_per_sqm"):
        issues.append("madrid_baseline.median_price_per_sqm is null")

    # Banned keys check
    for barrio, profile in profiles.items():
        bad = _BANNED_PROFILE_KEYS & set(profile.keys())
        if bad:
            issues.append(f"profile {barrio!r} carries banned keys: {sorted(bad)}")
        kpis = profile.get("kpis") or {}
        bad_kpi = _BANNED_KPI_KEYS & set(kpis.keys())
        if bad_kpi:
            issues.append(f"kpis for {barrio!r} carry banned fields: {sorted(bad_kpi)}")

    # Verdict variety — at least 3 distinct labels expected
    labels = {
        (p.get("verdict") or {}).get("label")
        for p in profiles.values()
    }
    labels.discard(None)
    if len(labels) < 3:
        issues.append(
            f"verdict mix collapsed to {len(labels)} label(s): {sorted(labels)} "
            "(thresholds may need tuning)"
        )

    return issues


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export Phase-2 clean barrios_profiles.json"
    )
    parser.add_argument(
        "-o", "--output",
        help="Path to write JSON. If omitted, prints to stdout.",
    )
    parser.add_argument(
        "--db",
        help="Path to real_estate.db (defaults to the one in repo root).",
    )
    parser.add_argument(
        "--opendata",
        help="Path to district_opendata.json (defaults to "
             "market-thermometer/public/district_opendata.json).",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="After building, run safety checks and exit non-zero on failure.",
    )
    args = parser.parse_args()

    from barrio_profiles_clean import build_clean_barrio_profiles

    payload = build_clean_barrio_profiles(
        db_path=args.db,
        opendata_path=args.opendata,
    )

    if args.verify:
        issues = _verify(payload)
        if issues:
            print("❌ verification failed:", file=sys.stderr)
            for i in issues:
                print(f"  - {i}", file=sys.stderr)
            return 1
        print(
            f"✓ verification OK — {payload['metadata']['barrio_count']} barrios, "
            f"{len({(p.get('verdict') or {}).get('label') for p in payload['profiles'].values()})} "
            "distinct verdicts",
            file=sys.stderr,
        )

    pretty = json.dumps(payload, ensure_ascii=False, indent=2, default=str)

    if args.output:
        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(pretty)
        size_kb = os.path.getsize(args.output) / 1024
        print(
            f"📁 Written to {args.output} ({size_kb:.0f} KB)",
            file=sys.stderr,
        )
    else:
        print(pretty)

    return 0


if __name__ == "__main__":
    sys.exit(main())
