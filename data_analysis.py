"""
Comprehensive data quality + coverage report for the inmobiliario dataset.

Reads from the SQLite snapshot to give a quick preview.  Re-run against
Neon by setting ``DB_BACKEND=postgres`` if you want current numbers.
Designed to inform the scraping cadence decision.
"""
import sqlite3
from collections import Counter
from datetime import date, datetime, timedelta
from statistics import median, mean
import os, sys

# Allow running against Postgres if DB_BACKEND is set.
USE_PG = os.environ.get("DB_BACKEND", "sqlite").lower() == "postgres"

if USE_PG:
    
    import psycopg
    from db.connection_pg import _normalise_url
    conn = psycopg.connect(_normalise_url(os.environ["DATABASE_URL"]))
    Q_DATE_DIFF = "EXTRACT(DAY FROM (%s::date - %s::date))::int"
    Q_DATE_CMP_GE = "%s::date"
else:
    conn = sqlite3.connect('real_estate.db')
    conn.row_factory = sqlite3.Row

TODAY = date.today()


def query(sql, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchall()


def header(title):
    print()
    print("=" * 72)
    print(f"  {title}")
    print("=" * 72)


# ──────────────────────────────────────────────────────────────────────
header("1. COBERTURA TEMPORAL DEL SCRAPING")
# ──────────────────────────────────────────────────────────────────────

logs = query("SELECT start_time, properties_processed, new_listings, status FROM scraping_log ORDER BY start_time")
if logs:
    def to_date(v):
        if isinstance(v, (date, datetime)):
            return v if isinstance(v, date) and not isinstance(v, datetime) else v.date()
        return date.fromisoformat(str(v)[:10])

    first = to_date(logs[0][0])
    last  = to_date(logs[-1][0])
    print(f"  Total runs registrados:        {len(logs)}")
    print(f"  Primer run:                    {first}")
    print(f"  Último run:                    {last}")
    print(f"  Span:                          {(last-first).days} días")
    print(f"  Gap desde último run a hoy:    {(TODAY-last).days} días")

    dates_ = [to_date(l[0]) for l in logs]
    gaps = [(dates_[i+1] - dates_[i]).days for i in range(len(dates_)-1)]
    if gaps:
        print(f"  Gap mediano entre runs:        {median(gaps):.1f} días")
        print(f"  Gap medio:                     {mean(gaps):.1f} días")
        print(f"  Gap máximo (silencio):         {max(gaps)} días")
    ps = [(l[1] or 0) for l in logs]
    print(f"  Properties processed por run: median {median(ps):,.0f}  máx {max(ps):,}")


# ──────────────────────────────────────────────────────────────────────
header("2. UNIVERSO DE LISTINGS")
# ──────────────────────────────────────────────────────────────────────

total = query("SELECT COUNT(*) FROM listings")[0][0]
active = query("SELECT COUNT(*) FROM listings WHERE status='active'")[0][0]
sold   = query("SELECT COUNT(*) FROM listings WHERE status='sold_removed'")[0][0]
print(f"  Total listings (lifetime):     {total:,}")
print(f"  Activos hoy:                   {active:,}  ({active*100/total:.1f}%)")
print(f"  Sold/removed (lifetime):       {sold:,}  ({sold*100/total:.1f}%)")
print()
for days, label in [(7,"últimos 7 días"), (30,"últimos 30 días"), (90,"últimos 90 días")]:
    cutoff = (TODAY - timedelta(days=days)).isoformat()
    nlh = "?" if not USE_PG else "%s"
    n = query(f"SELECT COUNT(*) FROM listings WHERE first_seen_date >= {nlh}", (cutoff,))[0][0]
    sn = query(
        f"SELECT COUNT(*) FROM listings WHERE status='sold_removed' AND last_seen_date >= {nlh}",
        (cutoff,)
    )[0][0]
    print(f"  {label:>20s}:  {n:>5,} nuevos / {sn:>5,} vendidos+retirados")


# ──────────────────────────────────────────────────────────────────────
header("3. COBERTURA GEOGRÁFICA — top 10 distritos por stock activo")
# ──────────────────────────────────────────────────────────────────────

d = query("SELECT COUNT(DISTINCT distrito) FROM listings WHERE status='active'")[0][0]
b = query("SELECT COUNT(DISTINCT barrio) FROM listings WHERE status='active'")[0][0]
print(f"  Distritos con activos:         {d}")
print(f"  Barrios con activos:           {b}")
print()
rows = query("""
    SELECT distrito, COUNT(*) n,
           ROUND(AVG(CASE WHEN size_sqm BETWEEN 30 AND 250
                          THEN price*1.0/size_sqm END)) ppsqm
    FROM listings WHERE status='active' GROUP BY distrito
    ORDER BY n DESC LIMIT 10
""")
for r in rows:
    distrito, n, pps = r[0], r[1], r[2]
    print(f"    {distrito:25s}  {n:>5,}  €{pps:>6,.0f}/m² avg (30-250m²)")


# ──────────────────────────────────────────────────────────────────────
header("4. €/M² POR BARRIO — top 20 con mayor stock activo")
# ──────────────────────────────────────────────────────────────────────

rows = query("""
    SELECT barrio, distrito, COUNT(*) n,
           ROUND(AVG(price*1.0/size_sqm)) avg_pps,
           ROUND(MIN(price*1.0/size_sqm)) min_pps,
           ROUND(MAX(price*1.0/size_sqm)) max_pps
    FROM listings
    WHERE status='active' AND size_sqm BETWEEN 30 AND 250
    GROUP BY barrio, distrito
    HAVING COUNT(*) >= 30
    ORDER BY n DESC LIMIT 20
""")
print(f"  {'Barrio':28s} {'Distrito':22s} {'N':>5s}  €/m² (min — avg — max)")
for r in rows:
    barrio, distrito, n, avg_p, min_p, max_p = r
    print(f"  {barrio:28s} {distrito:22s} {n:>5,}  "
          f"€{min_p:>5,.0f} — €{avg_p:>5,.0f} — €{max_p:>6,.0f}")


# ──────────────────────────────────────────────────────────────────────
header("5. DAYS ON MARKET — distribución de listings activos hoy")
# ──────────────────────────────────────────────────────────────────────

rows = query("SELECT first_seen_date FROM listings WHERE status='active' AND first_seen_date IS NOT NULL")
def to_date(v):
    if isinstance(v, (date, datetime)):
        return v if isinstance(v, date) and not isinstance(v, datetime) else v.date()
    return date.fromisoformat(str(v)[:10])
doms = sorted((TODAY - to_date(r[0])).days for r in rows)

if doms:
    def p(x): return doms[int(len(doms)*x/100)]
    print(f"  N:                             {len(doms):,}")
    print(f"  P10 (más fresco):              {p(10)} días")
    print(f"  P25:                           {p(25)} días")
    print(f"  P50 (mediana):                 {p(50)} días")
    print(f"  P75:                           {p(75)} días")
    print(f"  P90 (más quemado):             {p(90)} días")
    print(f"  Máximo:                        {max(doms)} días")
    print(f"  Buckets:")
    for lo, hi in [(0,7),(7,30),(30,90),(90,180),(180,365),(365,99999)]:
        n = sum(1 for x in doms if lo <= x < hi)
        bar = "█" * int(n*50/len(doms))
        hi_s = "∞" if hi > 999 else str(hi)
        print(f"    [{lo:>3d}-{hi_s:>3}d):  {n:>5,}  {bar}")


# ──────────────────────────────────────────────────────────────────────
header("6. DÍAS HASTA VENTA/RETIRADA — listings con status=sold_removed")
# ──────────────────────────────────────────────────────────────────────

rows = query("""
    SELECT first_seen_date, last_seen_date FROM listings
    WHERE status='sold_removed' AND first_seen_date IS NOT NULL AND last_seen_date IS NOT NULL
""")
sold_times = []
for r in rows:
    try:
        fs, ls = to_date(r[0]), to_date(r[1])
        delta = (ls - fs).days
        if 0 <= delta <= 1000:
            sold_times.append(delta)
    except Exception:
        pass
sold_times.sort()
if sold_times:
    def p(x): return sold_times[int(len(sold_times)*x/100)]
    print(f"  N:                             {len(sold_times):,}")
    print(f"  P10:                           {p(10)} días")
    print(f"  P25:                           {p(25)} días")
    print(f"  P50 (mediana hasta venta):     {p(50)} días")
    print(f"  P75:                           {p(75)} días")
    print(f"  P90:                           {p(90)} días")


# ──────────────────────────────────────────────────────────────────────
header("7. CAMBIOS DE PRECIO REGISTRADOS")
# ──────────────────────────────────────────────────────────────────────

ph_total = query("SELECT COUNT(*) FROM price_history")[0][0]
drops    = query("SELECT COUNT(*) FROM price_history WHERE change_amount < 0")[0][0]
rises    = query("SELECT COUNT(*) FROM price_history WHERE change_amount > 0")[0][0]
print(f"  Entradas en price_history:     {ph_total:,}")
print(f"  Bajadas:                       {drops:,}  ({drops*100/max(ph_total,1):.1f}%)")
print(f"  Subidas:                       {rises:,}  ({rises*100/max(ph_total,1):.1f}%)")
print(f"  Ratio bajadas/subidas:         {drops/max(rises,1):.1f}x")
print()
n_per = query("""
    SELECT n, COUNT(*) c FROM (
        SELECT listing_id, COUNT(*) n FROM price_history
        WHERE change_amount < 0 GROUP BY listing_id
    ) AS t GROUP BY n ORDER BY n
""")
total_drops_listings = sum(r[1] for r in n_per)
print(f"  Listings con ≥1 bajada:        {total_drops_listings:,}  ({total_drops_listings*100/max(active,1):.1f}% de activos)")
print(f"  Distribución por nº de bajadas:")
for r in n_per[:8]:
    n, c = r
    bar = "█" * int(c*50/max(total_drops_listings,1))
    print(f"    {n} bajada{'s' if n!=1 else ''}: {c:>5,}  {bar}")


# ──────────────────────────────────────────────────────────────────────
header("8. CADENCIA DE SCRAPING — informe para decisión")
# ──────────────────────────────────────────────────────────────────────

cutoff_60 = (TODAY - timedelta(days=60)).isoformat()
nlh = "?" if not USE_PG else "%s"
new_60  = query(f"SELECT COUNT(*) FROM listings WHERE first_seen_date >= {nlh}", (cutoff_60,))[0][0]
sold_60 = query(f"SELECT COUNT(*) FROM listings WHERE status='sold_removed' AND last_seen_date >= {nlh}", (cutoff_60,))[0][0]
print(f"  Nuevos listings últimos 60d:   {new_60:>5,}  → {new_60/60:.1f}/día promedio")
print(f"  Sold/retirados últimos 60d:    {sold_60:>5,}  → {sold_60/60:.1f}/día promedio")
print(f"  Tasa de cambio del inventario: {(new_60+sold_60)/60:.1f} eventos/día sobre {active:,} activos")
print(f"                                 = {(new_60+sold_60)*100/60/max(active,1):.2f}% del stock cambia cada día")

print()
print("  ─────────────────────────────────────────────────────────────────")
print("  REFERENCIA: trade-offs por frecuencia")
print("  ─────────────────────────────────────────────────────────────────")
print()
print("    DIARIO (7x/semana):")
print("      + Captura todo movimiento <24h")
print("      − Coste BrightData ~3-4x vs Mon/Thu actual (~$1.50/mes)")
print("      − Mucho ruido: 'updated' sin cambio real → fatiga del log")
print()
print("    Mon/Wed/Fri (3x/semana):")
print("      + Captura cambios <72h, fino para velocidad real de venta")
print("      + Mejor reparto temporal de carga")
print("      − ~50% más coste vs cadencia actual")
print()
print("    Mon/Thu (2x/semana — CADENCIA ACTUAL):")
print("      + Coste bajo, robusto operacionalmente")
print("      − Floor de 3-4 días para detectar bajadas (suficiente p/ tendencias semanales)")
print("      − Quizá pierdes ofertas flash de <7 días")
print()
print("    Semanal (1x/semana):")
print("      + Coste mínimo, casi gratis")
print("      − Pobre para reaccionar rápido a chollos nuevos")
print("      − Resolución demasiado baja si DOM mediano de venta es <30d")
