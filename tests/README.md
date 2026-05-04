# Tests

Pytest suite for the Madrid Real Estate Tracker.

## Quick start

```bash
pip install -r requirements-dev.txt
pytest
```

Runs in well under a second on a temp SQLite DB — no network, no cloud, no real `real_estate.db`.

## Layout

```
tests/
├── conftest.py          shared fixtures (tmp_db, tmp_db_seeded, sample data)
├── unit/                pure functions, no DB or network
│   ├── test_analytics.py        quality_score / negotiability_score / labels
│   ├── test_nlp_analyzer.py     analyze_description / extract_amenities
│   └── test_barrio_profiles.py  compute_verdict / distribution / neighbours
├── integration/         exercises a temporary SQLite DB
│   ├── test_database.py         CRUD, mark_stale_as_sold (14d / 21d), pagination
│   └── test_compute_snapshots.py  absorption_rate, months_of_supply formulas
└── regression/          schema invariants — break-on-silent-migration guards
    └── test_db_schema.py        required tables, columns, composite indexes
```

## Markers

```bash
pytest -m unit          # only unit tests
pytest -m integration   # only DB integration tests
pytest -m regression    # only schema regression tests
```

## Coverage

```bash
pytest --cov=. --cov-report=term-missing --cov-report=html
open htmlcov/index.html
```

## Adding new tests

1. Pick a file under `unit/`, `integration/` or `regression/` based on whether
   the test needs a DB or just pure Python.
2. Use the existing fixtures whenever possible:
   - `tmp_db` for an empty fresh DB.
   - `tmp_db_seeded` for a DB pre-populated with deterministic listings.
   - `sample_listings`, `sample_distrito_stats`, `sample_barrio_stats` for unit tests.
3. Add `pytestmark = pytest.mark.<unit|integration|regression>` at the top.
4. Run `pytest -x` to fail fast while iterating.

## Known issues guarded by tests

- `mark_stale_as_sold(14)` must NOT mark recent listings as sold (test_database).
- `mark_stale_as_sold` 21-day fallback must catch barrios with no fresh scrape signal.
- `absorption_rate` formula: `sold_30d / active * 100`, lag-shifted 14 d.
- `months_of_supply`: `active / (sold_90d/3)`, capped at 36.
- Composite indexes (`idx_active_distrito_price`, etc.) must survive migrations.

## Out of scope (manual / network-bound)

The legacy scripts at the project root are NOT run by pytest:

- `test_direct_fetch.py`, `test_playwright_fetch.py`, `test_scraperapi_fetch.py`
  exercise the live scraping stack (Bright Data / ScraperAPI).
- `test_description.py` runs the real scraper against Idealista.
- `test_sold_logic.py` predates pytest — its functionality is covered by
  `tests/integration/test_database.py::TestMarkStaleAsSold`.
