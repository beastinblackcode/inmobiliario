"""
Integration tests for market_indicators.get_weekly_price_evolution — the
composition-controlled weekly price index.

The indicator used to bucket listings by ``first_seen_date``, so it measured
the listings that *entered* the market each week rather than the market. Since
each scrape sweeps a different set of districts, a week heavy on the cheap
periphery read as a crash and the next week read as a spike: the live series
swung between €298k and €492k, reporting +15.0 % over a stretch where a
repeat-listing index showed roughly 0 %. That figure carries 20 % of
``calculate_market_score``, so the public thermometer inherited the error.

Locks down:
  - a coverage swing towards one district leaves the index flat (the
    regression that motivated the rewrite),
  - a genuine, across-the-board price cut still comes through,
  - the reported level does not depend on the ``weeks`` lookback,
  - price_history is what dates a price, not the listing's current price,
  - districts too thin for a stable median stay out of the aggregate,
  - an empty DB degrades to an empty series rather than raising,
  - change_pct_horizon (what the market score reads) ignores an in-progress
    final week and does not move with the lookback.

Fixtures use three districts with a clear middle, because a weighted median
over two equally weighted groups is genuinely undefined — any value between
them qualifies — and would lock in whichever way the implementation happens to
break the tie.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

import market_indicators as mi

pytestmark = pytest.mark.integration


def _monday(weeks_ago: int) -> date:
    """The Monday *weeks_ago* weeks back.

    The indicator anchors its weeks on MAX(last_seen_date), so fixtures work
    backwards from a Monday rather than from a fixed date. Week 1 is the most
    recent *complete* week: the indicator treats a week that started under
    7 days ago as still open and excludes it from the trend math.
    """
    today = datetime.now().date()
    this_monday = today - timedelta(days=today.weekday())
    return this_monday - timedelta(days=7 * weeks_ago)


def _sunday(weeks_ago: int) -> date:
    return _monday(weeks_ago) + timedelta(days=6)


def _block(prefix: str, distrito: str, price: int, count: int,
           first_seen: date, last_seen: date) -> list[tuple]:
    """A block of near-identical listings priced around *price*.

    Spread a little so the IQR trim and the median have a distribution to work
    with rather than one repeated value.
    """
    return [
        (f"{prefix}{i:04d}", price + (i % 10) * 1000, distrito, 100.0,
         first_seen, last_seen)
        for i in range(count)
    ]


def _seed(rows: list[tuple]) -> None:
    """Insert listings plus the baseline price_history row for each.

    Mirrors production, where price_history carries a baseline row written at
    first_seen (NULL change_amount) and one row per subsequent change.
    """
    from db.connection import get_db

    conn = get_db()
    conn.executemany(
        """
        INSERT INTO listings
            (listing_id, title, price, distrito, barrio, rooms, size_sqm,
             seller_type, first_seen_date, last_seen_date, status)
        VALUES (?, 'x', ?, ?, 'b', 2, ?, 'Agencia', ?, ?, 'active')
        """,
        [(r[0], r[1], r[2], r[3], r[4].isoformat(), r[5].isoformat()) for r in rows],
    )
    conn.executemany(
        "INSERT INTO price_history (listing_id, price, date_recorded) VALUES (?, ?, ?)",
        [(r[0], r[1], r[4].isoformat()) for r in rows],
    )
    conn.commit()


def _reprice(rows: list[tuple], factor: float, when: date) -> None:
    """Reprice every listing in *rows* by *factor*, effective *when*."""
    from db.connection import get_db

    conn = get_db()
    conn.executemany(
        "INSERT INTO price_history (listing_id, price, date_recorded) VALUES (?, ?, ?)",
        [(r[0], int(r[1] * factor), when.isoformat()) for r in rows],
    )
    conn.executemany(
        "UPDATE listings SET price = ? WHERE listing_id = ?",
        [(int(r[1] * factor), r[0]) for r in rows],
    )
    conn.commit()


def _three_districts(first_seen: date, last_seen: date) -> list[tuple]:
    """Salamanca €900k / Centro €600k / Villaverde €300k at a 25/50/25 mix.

    The weighted median sits inside Centro, well clear of either edge.
    """
    return (
        _block("SAL", "Salamanca", 900_000, 50, first_seen, last_seen)
        + _block("CEN", "Centro", 600_000, 100, first_seen, last_seen)
        + _block("VIL", "Villaverde", 300_000, 50, first_seen, last_seen)
    )


def test_coverage_swing_does_not_move_the_index(tmp_db: Path):
    """
    Three districts whose prices never change, over three weeks. In the middle
    week the sweep goes deep on the cheapest district — 250 of the week's 400
    listings are Villaverde instead of 50 of 200.

    That is the coverage swing that used to crash the reported median. Prices
    did not move, so the index must not either.
    """
    stable = _three_districts(_monday(3), _sunday(1))
    flood = _block("FLD", "Villaverde", 300_000, 200, _monday(2), _sunday(2))
    _seed(stable + flood)

    result = mi.get_weekly_price_evolution(weeks=3)
    series = result["series"]

    assert len(series) == 3, f"expected 3 weeks, got {[p['week'] for p in series]}"
    levels = [p["median_price"] for p in series]
    assert len(set(levels)) == 1, f"composition leaked into the index: {levels}"
    # The flood week really is dominated by Villaverde …
    assert series[1]["count"] > series[0]["count"] * 1.5
    # … yet the index still reports the middle district.
    assert 600_000 <= levels[0] < 610_000
    assert result["change_pct"] == pytest.approx(0.0, abs=0.01)
    assert result["trend"] == "stable"


def test_real_price_cut_is_reported(tmp_db: Path):
    """A 10 % cut across every district, with the mix held constant, must come
    through — the flatness above is composition control, not a flat index."""
    rows = _three_districts(_monday(3), _sunday(1))
    _seed(rows)
    _reprice(rows, 0.9, _monday(1))

    result = mi.get_weekly_price_evolution(weeks=3)

    assert result["trend"] == "down"
    assert result["change_pct"] < -5, f"a 10 % cut read as {result['change_pct']} %"
    assert result["change_pct_eur"] < -5


def test_level_is_independent_of_lookback(tmp_db: Path):
    """The latest week's level must not depend on how far back we look.

    District weights were once averaged over the whole window, so a widening
    of scraper coverage *inside* that window re-weighted the city: the same
    week reported €485k on a 4-week lookback and €525k on a 12-week one.
    """
    rows = _three_districts(_monday(8), _sunday(1))
    # Coverage of the cheap district widens midway through the long window.
    rows += _block("VIL_B", "Villaverde", 300_000, 100, _monday(4), _sunday(1))
    _seed(rows)

    short = mi.get_weekly_price_evolution(weeks=3)
    long = mi.get_weekly_price_evolution(weeks=8)

    assert short["current"] == long["current"]
    assert short["current_sqm"] == long["current_sqm"]


def test_price_at_week_comes_from_history(tmp_db: Path):
    """A listing since repriced must count in older weeks at the price it
    carried *then*, not at today's price."""
    rows = _three_districts(_monday(3), _sunday(1))
    _seed(rows)
    _reprice(rows, 0.5, _monday(1))

    series = mi.get_weekly_price_evolution(weeks=3)["series"]

    assert series[0]["median_price"] > 550_000, "old week priced at today's price"
    assert series[-1]["median_price"] < 350_000, "latest week ignored the reprice"


def test_thin_districts_are_ignored(tmp_db: Path):
    """A district below _MIN_DISTRICT_SAMPLE is too thin for a stable median
    and must not reach the aggregate."""
    rows = _block("CEN", "Centro", 600_000, 60, _monday(2), _sunday(1))
    # Five listings at an absurd price — enough to move a naive median.
    rows += _block("TIN", "Barajas", 5_000_000, 5, _monday(2), _sunday(1))
    _seed(rows)

    series = mi.get_weekly_price_evolution(weeks=3)["series"]

    assert series, "expected a series"
    for point in series:
        assert point["districts"] == 1, "thin district leaked into the aggregate"
        assert point["median_price"] < 1_000_000


def test_empty_db_returns_empty_series(tmp_db: Path):
    """No data must degrade to an empty series, not raise."""
    result = mi.get_weekly_price_evolution(weeks=4)

    assert result["series"] == []
    assert result["current"] is None
    assert result["trend"] == "stable"


def test_horizon_change_ignores_the_in_progress_week(tmp_db: Path):
    """change_pct_horizon must exclude a half-scraped final week.

    The latest week is normally still being swept, so it holds a partial and
    unrepresentative slice: the sweep reaches different listings *within* each
    district, which district weighting cannot correct for. On live data,
    including such a week put the horizon change at +0.9 % where the complete
    weeks said 0.0 % — two buckets of the score's price component.
    """
    # Nine flat weeks of stable stock …
    _seed(_three_districts(_monday(10), _sunday(2)))
    # … then a thin final week that happened to catch the expensive end of
    # every district (half the usual count, 1.5x the usual price).
    partial = (
        _block("PSAL", "Salamanca", 1_350_000, 25, _monday(1), _sunday(1))
        + _block("PCEN", "Centro", 900_000, 25, _monday(1), _sunday(1))
        + _block("PVIL", "Villaverde", 450_000, 25, _monday(1), _sunday(1))
    )
    _seed(partial)

    result = mi.get_weekly_price_evolution(weeks=10)

    assert result["incomplete_latest_week"] is True, "partial week not detected"
    assert result["series"][-1]["median_price"] > 800_000, "fixture not biased"
    assert result["change_pct_horizon"] == pytest.approx(0.0, abs=0.01), (
        "the partial week leaked into the horizon change"
    )


def _stepped_market(cut_at_week: int, factor: float) -> None:
    """Stable stock over 16 weeks that reprices by *factor* at *cut_at_week*."""
    rows = _three_districts(_monday(16), _sunday(1))
    _seed(rows)
    _reprice(rows, factor, _monday(cut_at_week))


def test_horizon_change_is_stable_across_lookbacks(tmp_db: Path):
    """The score's input must be a property of the market, not of `weeks`.

    Blocks are capped at 4 weeks and anchored at the recent end, so every
    lookback long enough to fill them must agree. This is why the default
    widened from 8 to 12: at 8 the blocks shrank to 3 weeks and the score
    moved with the lookback (38.4 at 8 weeks, 36.4 at 12).
    """
    _stepped_market(cut_at_week=4, factor=0.98)

    values = {
        weeks: mi.get_weekly_price_evolution(weeks=weeks)["change_pct_horizon"]
        for weeks in (10, 12, 14, 16)
    }

    assert len(set(values.values())) == 1, f"horizon moved with lookback: {values}"
    assert next(iter(values.values())) != 0, "fixture did not move"


def test_horizon_change_reports_a_real_move(tmp_db: Path):
    """A sustained 2 % cut must reach change_pct_horizon.

    change_pct alone would show it for one week and then forget it; the
    horizon measure is what the score reads precisely so a move that plays
    out over a month is not mistaken for noise.
    """
    _stepped_market(cut_at_week=4, factor=0.98)

    result = mi.get_weekly_price_evolution(weeks=12)

    assert result["horizon_weeks"] == 4
    assert result["change_pct_horizon"] < -1, (
        f"a 2 % cut across the recent block read as "
        f"{result['change_pct_horizon']} %"
    )
