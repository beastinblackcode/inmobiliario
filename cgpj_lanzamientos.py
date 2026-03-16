"""
cgpj_lanzamientos.py
====================
Parse and store CGPJ quarterly evictions data (lanzamientos practicados)
for the Comunidad de Madrid from the official CGPJ series Excel file.

Source
------
Consejo General del Poder Judicial — Estadística Judicial
"Series - Efecto de la crisis en los órganos judiciales por TSJ"
URL: https://www.poderjudicial.es/stfls/ESTADISTICA/FICHEROS/Crisis/
     Series%20-%20Efecto%20de%20la%20crisis%20en%20los%20organos%20judiciales%20por%20TSJ.xlsx

File structure (confirmed 2024-11 edition)
------------------------------------------
Relevant sheets:
  'Lanzamientos practic. total TSJ'  — header row 6  (col 2 = '13-T1' …)
  'Lanzamientos E.hipotecaria TSJ'   — header row 5
  'Lanzamientos L.A.U  TSJ'          — header row 5
  'Lanzamientos. Otros TSJ'          — header row 4

Quarter header format: 'YY-TQ'  e.g. '24-T3' = 2024 Q3
Madrid TSJ label: 'MADRID, COMUNIDAD'
Annual totals in trailing columns (skipped — only quarterly cols imported).

Usage
-----
    python cgpj_lanzamientos.py --file path/to/CGPJ.xlsx   # import
    python cgpj_lanzamientos.py --show                      # print stored data
    python cgpj_lanzamientos.py --show --csv                # CSV output
"""

import argparse
import re
import sys
from pathlib import Path

import openpyxl
from database import get_connection


# ── Constants ──────────────────────────────────────────────────────────────────

TSJ_MADRID    = "MADRID, COMUNIDAD"
QUARTER_RE    = re.compile(r"^(\d{2})-T(\d)$")   # matches '24-T3'

SHEETS = {
    "total":    ("Lanzamientos practic. total TSJ", 6),
    "hipoteca": ("Lanzamientos E.hipotecaria TSJ",  5),
    "alquiler": ("Lanzamientos L.A.U  TSJ",         5),
    "otros":    ("Lanzamientos. Otros TSJ",          4),
}


# ── Parse ──────────────────────────────────────────────────────────────────────

def _madrid_series(ws, header_row_idx: int) -> dict[tuple[int, int], int]:
    """
    Extract {(year, quarter): value} for MADRID, COMUNIDAD from a sheet.
    Only includes quarterly columns (skips 'Total YYYY' columns).
    """
    rows = list(ws.iter_rows(values_only=True))
    headers = rows[header_row_idx]

    # Build mapping: col_index → (year, quarter)
    col_map = {}
    for i, h in enumerate(headers):
        if h is None:
            continue
        m = QUARTER_RE.match(str(h).strip())
        if m:
            year    = 2000 + int(m.group(1))
            quarter = int(m.group(2))
            col_map[i] = (year, quarter)

    # Find Madrid data row — TSJ label may be in col 0 or col 1
    madrid_row = None
    for row in rows:
        for col_idx in (0, 1):
            if len(row) > col_idx and row[col_idx] and \
               str(row[col_idx]).strip().upper() == TSJ_MADRID:
                madrid_row = row
                break
        if madrid_row is not None:
            break

    if madrid_row is None:
        raise ValueError(f"Row '{TSJ_MADRID}' not found in sheet '{ws.title}'")

    series = {}
    for col_idx, (year, quarter) in col_map.items():
        v = madrid_row[col_idx]
        if v is not None:
            try:
                series[(year, quarter)] = int(v)
            except (TypeError, ValueError):
                pass

    return series


def parse_file(path: Path) -> list[dict]:
    """
    Parse all 4 lanzamientos sheets and join them by (year, quarter).
    Returns list of dicts ready for DB insertion.
    """
    wb = openpyxl.load_workbook(str(path), data_only=True)

    series = {}
    for key, (sheet_name, header_row) in SHEETS.items():
        ws = wb[sheet_name]
        series[key] = _madrid_series(ws, header_row)

    # Union of all (year, quarter) keys present in any sheet
    all_periods = set()
    for s in series.values():
        all_periods.update(s.keys())

    records = []
    for (year, quarter) in sorted(all_periods):
        total    = series["total"].get((year, quarter))
        hipoteca = series["hipoteca"].get((year, quarter))
        alquiler = series["alquiler"].get((year, quarter))
        otros    = series["otros"].get((year, quarter))
        alq_pct  = round(alquiler / total * 100, 1) if total and alquiler else None

        records.append({
            "year":         year,
            "quarter":      quarter,
            "tsj":          "Madrid",
            "provincia":    "Madrid",
            "total":        total,
            "alquiler":     alquiler,
            "hipoteca":     hipoteca,
            "otros":        otros,
            "alquiler_pct": alq_pct,
        })

    print(f"   Parsed {len(records)} quarterly records for Madrid "
          f"({records[0]['year']}-T{records[0]['quarter']} → "
          f"{records[-1]['year']}-T{records[-1]['quarter']})")
    return records


# ── Store ──────────────────────────────────────────────────────────────────────

def upsert_records(records: list[dict]) -> int:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.executemany("""
            INSERT INTO cgpj_lanzamientos
                (year, quarter, tsj, provincia, total, alquiler, hipoteca, otros, alquiler_pct)
            VALUES
                (:year, :quarter, :tsj, :provincia, :total, :alquiler, :hipoteca, :otros, :alquiler_pct)
            ON CONFLICT(year, quarter, tsj) DO UPDATE SET
                provincia    = excluded.provincia,
                total        = excluded.total,
                alquiler     = excluded.alquiler,
                hipoteca     = excluded.hipoteca,
                otros        = excluded.otros,
                alquiler_pct = excluded.alquiler_pct
        """, records)
        conn.commit()
        return len(records)


# ── Show ───────────────────────────────────────────────────────────────────────

def show_stored(as_csv: bool = False):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT year, quarter, total, alquiler, hipoteca, otros, alquiler_pct
            FROM cgpj_lanzamientos
            WHERE tsj = 'Madrid'
            ORDER BY year, quarter
        """)
        rows = cur.fetchall()

    if not rows:
        print("No data stored yet.")
        return

    if as_csv:
        print("year,quarter,total,alquiler,hipoteca,otros,alquiler_pct")
        for r in rows:
            print(",".join(str(v or "") for v in tuple(r)))
        return

    print(f"{'Year':>6} {'Q':>2}  {'Total':>7}  {'Alquiler':>9}  {'Hipoteca':>9}  {'Otros':>6}  {'Alq%':>6}")
    print("-" * 58)
    for r in rows:
        year, q, total, alq, hip, otros, pct = tuple(r)
        print(f"{year:>6} {q:>2}  {(total or '-'):>7}  {(alq or '-'):>9}  "
              f"{(hip or '-'):>9}  {(otros or '-'):>6}  {(pct or '-'):>6}")


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="CGPJ lanzamientos importer for Madrid")
    p.add_argument("--file", help="Path to CGPJ series Excel file")
    p.add_argument("--show", action="store_true", help="Print stored series")
    p.add_argument("--csv",  action="store_true", help="Output as CSV (with --show)")
    args = p.parse_args()

    if args.show:
        show_stored(as_csv=args.csv)
        return

    if not args.file:
        p.print_help()
        return

    path = Path(args.file)
    if not path.exists():
        print(f"❌ File not found: {path}")
        sys.exit(1)

    print(f"📂 Parsing {path.name} …")
    records = parse_file(path)
    n = upsert_records(records)
    print(f"✅ Imported {n} records into cgpj_lanzamientos")
    print()
    show_stored()


if __name__ == "__main__":
    main()
