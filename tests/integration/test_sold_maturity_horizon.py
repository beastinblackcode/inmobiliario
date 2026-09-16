"""
Integration tests for the maturity horizon shared by the flow indicators.

mark_stale_as_sold only marks a listing `sold_removed` once it has gone
unseen for the scraper's stale threshold (21 days), and a *week* of listings
matures over the following six days on top of that. Measured against live
data on 2026-09-16, classification is a step rather than a ramp, and it falls
between 22 and 29 days:

    week of last_seen    age    classified    still 'active'
    2026-08-24           22 d        0.0 %          1 522
    2026-08-17           29 d       99.8 %              2

Every indicator that counts departures — sales speed, supply/demand,
absorption, months of supply, rotation — used to report right up to the last
scrape, i.e. straight into the zone where nothing has been classified yet.
That read as a market that had stopped moving: supply/demand pinned at its
cap of 10, rotation at ~1 %, absorption down a third.

Locks down:
  - none of the five reports past the horizon,
  - departures inside the unclassified zone are not counted as "nothing sold",
  - active counts exclude listings that have gone but are not yet marked,
  - supply/demand compares the same calendar week on both sides,
  - the horizon degrades safely on an empty DB.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

import market_indicators as mi

pytestmark = pytest.mark.integration


def _insert(conn, listing_id: str, first_seen: date, last_seen: date,
            status: str = "active", price: int = 300_000) -> None:
    conn.execute(
        """
        INSERT INTO listings
            (listing_id, title, price, distrito, barrio, rooms, size_sqm,
             seller_type, first_seen_date, last_seen_date, status)
        VALUES (?, 'x', ?, 'Centro', 'Sol', 2, 80, 'Agencia', ?, ?, ?)
        """,
        (listing_id, price, first_seen.isoformat(), last_seen.isoformat(), status),
    )


# Departures per day in the fixture; x7 is the weekly flow.
_PER_DAY = 2
_DAYS = 140
_LIVE = 300


def _seed_market(conn, today: date) -> None:
    """A market with a steady stream of departures, classified realistically.

    Anything that left more than _MATURITY_LAG_DAYS ago has been marked.
    Anything more recent has not — it still carries status 'active', which is
    exactly the state that misleads a count taken as of today.

    Departures land on every day rather than one weekday, because
    _weekly_anchors samples the Mondays that actually appear in the data: a
    fixture that only ever writes one weekday yields a single anchor and the
    indicators degrade to a one-point series.
    """
    lag = mi._MATURITY_LAG_DAYS

    # Live stock, seen on the last scrape.
    for i in range(_LIVE):
        _insert(conn, f"LIVE{i:03d}", today - timedelta(days=150), today)

    for day in range(1, _DAYS + 1):
        gone_on = today - timedelta(days=day)
        classified = day > lag
        for i in range(_PER_DAY):
            _insert(
                conn, f"GONE{day:03d}_{i}",
                first_seen=gone_on - timedelta(days=60),
                last_seen=gone_on,
                status="sold_removed" if classified else "active",
            )
    conn.commit()


@pytest.fixture
def seeded(tmp_db: Path):
    from db.connection import get_db

    conn = get_db()
    today = datetime.now().date()
    _seed_market(conn, today)
    return today


def test_horizon_trails_the_last_scrape(seeded):
    """The horizon is the last scrape minus the full maturation period."""
    from db.connection import get_db

    cursor = get_db().cursor()
    horizon = mi._sold_horizon(cursor)

    assert horizon == seeded - timedelta(days=mi._MATURITY_LAG_DAYS)


def test_no_flow_indicator_reports_past_the_horizon(seeded):
    """All five must stamp the same as_of, and it must be the horizon.

    Without this they each reported to the last scrape, where departures are
    invisible, and disagreed with each other about what "now" meant.
    """
    horizon = (seeded - timedelta(days=mi._MATURITY_LAG_DAYS)).isoformat()

    for name, fn in [
        ("sales_speed", mi.get_weekly_sales_speed),
        ("supply_demand", mi.get_supply_demand_ratio),
        ("absorption", mi.get_absorption_rate),
        ("months_of_supply", mi.get_months_of_supply),
        ("rotation", mi.get_rotation_rate),
    ]:
        result = fn()
        assert result["as_of"] == horizon, f"{name} reported as_of {result['as_of']}"
        assert result["lag_days"] == mi._MATURITY_LAG_DAYS


def test_supply_demand_sees_the_departures(seeded):
    """10 in, 10 out per week → a ratio near 1, not the no-absorption cap.

    Reporting into the unclassified zone found zero departures, so the ratio
    hit MAX_RATIO. On live data that is how it came to read 9.36.
    """
    result = mi.get_supply_demand_ratio()

    assert result["current"] is not None
    assert result["current"] < 5.0, f"ratio pinned high: {result['current']}"
    for point in result["series"]:
        assert not point["capped"], f"week {point['week']} found no departures"
        assert point["sold_count"] > 0


def test_supply_demand_compares_one_calendar_week(seeded):
    """New and gone must describe the same seven days.

    The sold side used to be shifted forward by the stale threshold to
    compensate for late marking, which meant week W's new listings were being
    divided by week W-3's departures — not a ratio over any single period.
    """
    from db.connection import get_db
    from db.dialect import iso_week

    result = mi.get_supply_demand_ratio()
    assert result["series"], "expected a series"

    cursor = get_db().cursor()
    for point in result["series"]:
        cursor.execute(
            f"SELECT COUNT(*) FROM listings WHERE status = 'sold_removed' "
            f"AND {iso_week('last_seen_date')} = ?",
            (point["week"],),
        )
        assert point["sold_count"] == cursor.fetchone()[0]


def test_rotation_counts_real_departures(seeded):
    """Rotation anchored on the last scrape saw ~nothing leave.

    14 of a few hundred leave each week, so a 4-week rate must be clearly
    non-trivial; anchored at the last scrape it collapsed towards zero
    (live: 1.0 %).
    """
    result = mi.get_rotation_rate()

    assert result["sold_window"] > 0, "no departures counted at all"
    assert result["current"] > 2.0, f"rotation collapsed to {result['current']}"


def test_active_count_is_the_stock_as_it_stood_at_the_anchor(seeded):
    """The denominator must be the stock at the anchor, not today's label.

    A listing that has gone but is not yet marked still carries status
    'active' — on live data around 4 000 of them, ~18 % of the stock. The old
    query admitted anything with that status regardless of the anchor date,
    so a four-week-old numerator was divided by a present-day denominator.

    The anchor is what fixes this: as of the horizon those listings genuinely
    had not left, so they belong in the count, while everything that left
    earlier does not.
    """
    from db.connection import get_db

    conn = get_db()
    cursor = conn.cursor()
    horizon = (seeded - timedelta(days=mi._MATURITY_LAG_DAYS)).isoformat()

    cursor.execute("""
        SELECT COUNT(*) FROM listings
        WHERE first_seen_date <= ? AND last_seen_date >= ?
    """, (horizon, horizon))
    stock_at_horizon = cursor.fetchone()[0]

    absorption = mi.get_absorption_rate()

    assert absorption["active"] == stock_at_horizon
    # Everything that left before the horizon is out, so this is well short
    # of the table.
    cursor.execute("SELECT COUNT(*) FROM listings")
    assert absorption["active"] < cursor.fetchone()[0]


def test_stale_unmarked_listings_stay_out_of_the_denominator(seeded):
    """A listing that left long ago but was never marked must not count.

    mark_stale_as_sold's Tier 1 also requires the barrio to have been swept
    to full depth, so a handful of listings sit unmarked well past the
    threshold. `OR status = 'active'` used to wave those straight into the
    active count at every anchor.
    """
    from db.connection import get_db

    conn = get_db()
    before = mi.get_absorption_rate()["active"]

    # Left 90 days ago; never marked because its barrio was never swept.
    _insert(
        conn, "NEVER_SWEPT",
        first_seen=seeded - timedelta(days=150),
        last_seen=seeded - timedelta(days=90),
        status="active",
    )
    conn.commit()

    assert mi.get_absorption_rate()["active"] == before


def test_empty_db_degrades_safely(tmp_db: Path):
    """No data must yield no horizon rather than an exception."""
    from db.connection import get_db

    assert mi._sold_horizon(get_db().cursor()) is None

    result = mi.get_rotation_rate()
    assert result["current"] is None or result["current"] == 0
