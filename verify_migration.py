"""One-off helper: compare row counts between OLD and NEW Postgres deployments.

Run from the repo root with both URLs exported:

    export OLD_DATABASE_URL=...
    export NEW_DATABASE_URL=...
    python verify_migration.py

Safe to delete after the migration verifies green.
"""
import os
import psycopg
from db.connection_pg import _normalise_url

TABLES = [
    'listings', 'price_history', 'rental_prices', 'watchlist',
    'notarial_prices', 'market_snapshots', 'scraping_log',
    'custom_alerts', 'listing_signals', 'listing_amenities',
    'cgpj_lanzamientos', 'property_fingerprints',
    'listing_property_map', 'user_preferences', 'alembic_version',
]

OLD = _normalise_url(os.environ['OLD_DATABASE_URL'])
NEW = _normalise_url(os.environ['NEW_DATABASE_URL'])

mismatch = 0
with psycopg.connect(OLD) as old, psycopg.connect(NEW) as new:
    for t in TABLES:
        try:
            n_old = old.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
            n_new = new.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
            flag = '✓' if n_old == n_new else '✗'
            if flag == '✗':
                mismatch += 1
            print(f'  {t:24s} old={n_old:>7,}  new={n_new:>7,}  {flag}')
        except Exception as e:
            print(f'  {t:24s} ERROR: {e}')
            mismatch += 1

print()
print('ALL MATCH ✓' if mismatch == 0 else f'{mismatch} MISMATCHES — DO NOT PROCEED')
