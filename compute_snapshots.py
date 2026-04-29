#!/usr/bin/env python3
"""
compute_snapshots.py — Pre-compute daily market metrics.

Run after the scraper finishes (inside the GitHub Actions workflow).
Populates the ``market_snapshots`` table so that the Streamlit dashboard
can display KPIs and trends with simple SELECT lookups instead of heavy
aggregation queries on every user interaction.

Usage:
    python compute_snapshots.py            # today's date
    python compute_snapshots.py 2026-03-10 # specific date

Metrics computed at three scopes:
    city     — whole Madrid
    distrito — per district
    barrio   — per neighbourhood

Metric catalogue:
    median_price        median asking price (€)
    median_price_sqm    median €/m²
    avg_price_sqm       mean €/m²
    active_count        number of active listings
    sold_count          sold/removed in last 7 days
    sold_count_30d      sold/removed in last 30 days   (lag-shifted by 14 d)
    sold_count_90d      sold/removed in last 90 days   (lag-shifted by 14 d)
    absorption_rate     sold_count_30d / active_count × 100   (%, monthly)
    months_of_supply    active_count / (sold_count_90d / 3)   (months, 3-mo avg)
    new_listings        first_seen_date == today
    avg_days_on_market  mean days since first seen
    price_drops_count   listings with ≥1 price drop in last 7 days
    median_size_sqm     median property size
"""

import sys
import sqlite3
from datetime import datetime, timedelta
from typing import List, Tuple

from db.connection import get_db, set_database_path

DATABASE_PATH = "real_estate.db"


def _upsert_snapshot(
    conn: sqlite3.Connection,
    date_computed: str,
    scope_type: str,
    scope_value: str | None,
    metric_name: str,
    metric_value: float | None,
) -> None:
    """Insert or replace a single snapshot row."""
    if metric_value is None:
        return
    conn.execute(
        """
        INSERT INTO market_snapshots
            (date_computed, scope_type, scope_value, metric_name, metric_value)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(date_computed, scope_type, scope_value, metric_name)
        DO UPDATE SET metric_value = excluded.metric_value
        """,
        (date_computed, scope_type, scope_value, metric_name, metric_value),
    )


def _compute_scope_metrics(
    conn: sqlite3.Connection,
    date_str: str,
    scope_type: str,
    scope_value: str | None,
    where_clause: str,
    params: list,
) -> int:
    """
    Compute all metrics for a given scope and insert into market_snapshots.
    Returns number of metrics written.
    """
    count = 0

    # ── Active listings metrics ──────────────────────────────────────────
    row = conn.execute(
        f"""
        SELECT
            COUNT(*)                                                 AS active_count,
            ROUND(AVG(price), 0)                                     AS avg_price,
            ROUND(AVG(CASE WHEN size_sqm > 0 THEN price * 1.0 / size_sqm END), 2) AS avg_price_sqm,
            ROUND(AVG(
                CAST(julianday(COALESCE(last_seen_date, date('now')))
                     - julianday(COALESCE(first_seen_date, last_seen_date, date('now')))
                AS INTEGER)
            ), 1)                                                    AS avg_days_on_market
        FROM listings
        WHERE status = 'active' AND {where_clause}
        """,
        params,
    ).fetchone()

    if row:
        _upsert_snapshot(conn, date_str, scope_type, scope_value, "active_count", row[0])
        _upsert_snapshot(conn, date_str, scope_type, scope_value, "avg_price", row[1])
        _upsert_snapshot(conn, date_str, scope_type, scope_value, "avg_price_sqm", row[2])
        _upsert_snapshot(conn, date_str, scope_type, scope_value, "avg_days_on_market", row[3])
        count += 4

    # Medians (SQLite doesn't have a MEDIAN aggregate, so we compute manually)
    prices = conn.execute(
        f"SELECT price FROM listings WHERE status = 'active' AND price > 0 AND {where_clause} ORDER BY price",
        params,
    ).fetchall()
    if prices:
        vals = [r[0] for r in prices]
        median_price = vals[len(vals) // 2]
        _upsert_snapshot(conn, date_str, scope_type, scope_value, "median_price", median_price)
        count += 1

    sqm_prices = conn.execute(
        f"""
        SELECT ROUND(price * 1.0 / size_sqm, 2)
        FROM listings
        WHERE status = 'active' AND size_sqm > 0 AND price > 0 AND {where_clause}
        ORDER BY price * 1.0 / size_sqm
        """,
        params,
    ).fetchall()
    if sqm_prices:
        vals = [r[0] for r in sqm_prices]
        median_sqm = vals[len(vals) // 2]
        _upsert_snapshot(conn, date_str, scope_type, scope_value, "median_price_sqm", median_sqm)
        count += 1

    sizes = conn.execute(
        f"SELECT size_sqm FROM listings WHERE status = 'active' AND size_sqm > 0 AND {where_clause} ORDER BY size_sqm",
        params,
    ).fetchall()
    if sizes:
        vals = [r[0] for r in sizes]
        _upsert_snapshot(conn, date_str, scope_type, scope_value, "median_size_sqm", vals[len(vals) // 2])
        count += 1

    # ── Sold counts (lag-shifted by 14 d) ────────────────────────────────
    # mark_stale_as_sold() uses a 14-day threshold and does NOT update
    # last_seen_date when marking sold, so the effective detection date is
    # last_seen_date + 14 days. We shift each window back 14 days to count
    # properties that were detected as sold within that window.
    base = datetime.strptime(date_str, "%Y-%m-%d")
    LAG = 14

    def _sold_in_window(window_days: int) -> int | None:
        win_end   = (base - timedelta(days=LAG)).strftime("%Y-%m-%d")
        win_start = (base - timedelta(days=LAG + window_days)).strftime("%Y-%m-%d")
        row = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM listings
            WHERE status = 'sold_removed'
              AND last_seen_date >= ? AND last_seen_date < ?
              AND {where_clause}
            """,
            [win_start, win_end] + params,
        ).fetchone()
        return row[0] if row else None

    sold_7d  = _sold_in_window(7)
    sold_30d = _sold_in_window(30)
    sold_90d = _sold_in_window(90)

    if sold_7d is not None:
        _upsert_snapshot(conn, date_str, scope_type, scope_value, "sold_count", sold_7d)
        count += 1
    if sold_30d is not None:
        _upsert_snapshot(conn, date_str, scope_type, scope_value, "sold_count_30d", sold_30d)
        count += 1
    if sold_90d is not None:
        _upsert_snapshot(conn, date_str, scope_type, scope_value, "sold_count_90d", sold_90d)
        count += 1

    # Absorption rate (monthly) and months of supply (3-month avg).
    # `row[0]` is active_count from the active-listings query above.
    # Conventions match market_indicators.get_absorption_rate() and
    # get_months_of_supply(lookback_months=3).
    if row and row[0]:
        active_count = row[0]
        if active_count > 0 and sold_30d is not None:
            absorption = round(sold_30d / active_count * 100, 2)
            _upsert_snapshot(
                conn, date_str, scope_type, scope_value,
                "absorption_rate", absorption,
            )
            count += 1
        if active_count > 0 and sold_90d and sold_90d > 0:
            # 3-month average monthly sales rate
            monthly_rate = sold_90d / 3.0
            months = round(min(active_count / monthly_rate, 36.0), 1)
            _upsert_snapshot(
                conn, date_str, scope_type, scope_value,
                "months_of_supply", months,
            )
            count += 1

    # ── New listings (first_seen == today) ────────────────────────────────
    new_row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM listings
        WHERE first_seen_date = ? AND {where_clause}
        """,
        [date_str] + params,
    ).fetchone()
    if new_row:
        _upsert_snapshot(conn, date_str, scope_type, scope_value, "new_listings", new_row[0])
        count += 1

    # ── Price drops (last 7 days) ────────────────────────────────────────
    week_ago = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
    try:
        drops_row = conn.execute(
            f"""
            SELECT COUNT(DISTINCT ph.listing_id)
            FROM price_history ph
            JOIN listings l ON l.listing_id = ph.listing_id
            WHERE ph.change_amount < 0
              AND ph.date_recorded >= ?
              AND l.status = 'active'
              AND {where_clause.replace('1=1', 'l.' + '1=1' if where_clause == '1=1' else where_clause)}
            """.replace(
                # Prefix table for joined queries
                "distrito ", "l.distrito "
            ).replace(
                "barrio ", "l.barrio "
            ),
            [week_ago] + params,
        ).fetchone()
        if drops_row:
            _upsert_snapshot(conn, date_str, scope_type, scope_value, "price_drops_count", drops_row[0])
            count += 1
    except sqlite3.OperationalError:
        # price_history table might not exist yet
        pass

    return count


def compute_all_snapshots(date_str: str | None = None) -> int:
    """
    Compute snapshots for all scopes (city, distrito, barrio).
    Returns total number of metric rows written.
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    conn = get_db()

    # Ensure market_snapshots table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS market_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date_computed   TEXT NOT NULL,
            scope_type      TEXT NOT NULL,
            scope_value     TEXT,
            metric_name     TEXT NOT NULL,
            metric_value    REAL,
            UNIQUE(date_computed, scope_type, scope_value, metric_name)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_snapshots_lookup
        ON market_snapshots(scope_type, scope_value, metric_name, date_computed)
    """)

    total = 0

    # ── City-level ───────────────────────────────────────────────────────
    print(f"  📊 Computing city-level snapshots for {date_str}...")
    total += _compute_scope_metrics(conn, date_str, "city", None, "1=1", [])

    # ── Distrito-level ───────────────────────────────────────────────────
    distritos = conn.execute(
        "SELECT DISTINCT distrito FROM listings WHERE distrito IS NOT NULL"
    ).fetchall()
    print(f"  📊 Computing snapshots for {len(distritos)} distritos...")
    for (distrito,) in distritos:
        total += _compute_scope_metrics(
            conn, date_str, "distrito", distrito, "distrito = ?", [distrito]
        )

    # ── Barrio-level ─────────────────────────────────────────────────────
    barrios = conn.execute(
        "SELECT DISTINCT barrio FROM listings WHERE barrio IS NOT NULL"
    ).fetchall()
    print(f"  📊 Computing snapshots for {len(barrios)} barrios...")
    for (barrio,) in barrios:
        total += _compute_scope_metrics(
            conn, date_str, "barrio", barrio, "barrio = ?", [barrio]
        )

    conn.commit()
    print(f"  ✓ Snapshots computed: {total} metrics for {date_str}")
    return total


def main():
    set_database_path(DATABASE_PATH)

    date_str = sys.argv[1] if len(sys.argv) > 1 else None
    total = compute_all_snapshots(date_str)
    print(f"✓ Done — {total} snapshot metrics written.")


if __name__ == "__main__":
    main()
