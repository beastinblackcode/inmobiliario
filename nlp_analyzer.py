"""
NLP Analyzer — keyword-based signal detection for property descriptions.

Detects 5 categories of signals from Spanish listing descriptions:

    🔴 urgency      — seller motivation / time pressure  (+15 bonus)
    💼 direct       — direct owner, no agency fee        (+10 bonus)
    🟡 negotiable   — price negotiable or reduced        (+10 bonus)
    🟢 renovated    — recently renovated / move-in ready (+5 bonus)
    🔧 needs_work   — needs reform (risk / discount opp) (+5 bonus)

Each category returns matched keywords and a boolean.
A combined nlp_bonus (0-35) is added to the opportunity score.

Usage:
    from nlp_analyzer import analyze_description, run_nlp_batch

    signals = analyze_description("Venta urgente, propietario directo, precio negociable")
    # → { "urgency": True, "direct": True, "negotiable": True, ... , "nlp_bonus": 35 }

    run_nlp_batch()   # processes all listings missing NLP signals in DB
"""

import re
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional

DB_PATH = "real_estate.db"

# ── Signal dictionaries ────────────────────────────────────────────────────────
# Each entry: (pattern_string, bonus_weight)
# Patterns are matched case-insensitively against the full description text.

SIGNALS: Dict[str, List[tuple]] = {
    # 🔴 Seller urgency / motivation
    "urgency": [
        (r"\bventa urgente\b",            3),
        (r"\burgente\b",                  2),
        (r"\bnecesito vender\b",          3),
        (r"\bnecesita vender\b",          3),
        (r"\bpor traslado\b",             2),
        (r"\bpor motivos (familiares|laborales|personales|econ[oó]micos)\b", 2),
        (r"\bliquidaci[oó]n\b",           3),
        (r"\boportunidad [uú]nica\b",     1),
        (r"\bvender r[aá]pido\b",         3),
        (r"\baceptar[ií]a ofertas\b",     2),
        (r"\babierto a ofertas\b",        2),
        (r"\bherencia\b",                 2),
        (r"\bdivorcio\b",                 2),
        (r"\bembargo\b",                  3),
    ],
    # 💼 Direct owner / no agency
    "direct": [
        (r"\bpropietario directo\b",      3),
        (r"\bparticular\b",               2),
        (r"\bsin agencia\b",              3),
        (r"\bsin comisi[oó]n\b",          3),
        (r"\bvendo directamente\b",       3),
        (r"\bno somos agencia\b",         3),
        (r"\bdirectamente del due[ñn]o\b",3),
        (r"\bdue[ñn]o vende\b",           3),
        (r"\bventa directa\b",            2),
    ],
    # 🟡 Negotiable price
    "negotiable": [
        (r"\bprecio negociable\b",        3),
        (r"\bnegociable\b",               2),
        (r"\bprecio reducido\b",          2),
        (r"\bprecio rebajado\b",          2),
        (r"\bprecio a convenir\b",        2),
        (r"\bdescuento\b",                1),
        (r"\boferta especial\b",          1),
        (r"\bpor debajo de mercado\b",    3),
        (r"\bprecio de ocasi[oó]n\b",     2),
        (r"\bganga\b",                    2),
    ],
    # 🟢 Good condition / recently renovated
    "renovated": [
        (r"\breci[eé]n reformado\b",      3),
        (r"\bcompletamente reformado\b",  3),
        (r"\breforma (total|integral|completa|reciente)\b", 3),
        (r"\ba estrenar\b",               3),
        (r"\bnuevo a estrenar\b",         3),
        (r"\bcocina nueva\b",             2),
        (r"\bba[ñn]o nuevo\b",           2),
        (r"\btarima nueva\b",             2),
        (r"\binstalaci[oó]n el[eé]ctrica nueva\b", 2),
        (r"\bdise[ñn]o moderno\b",        1),
        (r"\bde lujo\b",                  1),
        (r"\balto standing\b",            2),
        (r"\bllave en mano\b",            2),
    ],
    # 🔧 Needs work (risk but potential discount)
    "needs_work": [
        (r"\ba reformar\b",               3),
        (r"\bpara reformar\b",            3),
        (r"\bnecesita reforma\b",         3),
        (r"\bpendiente de reforma\b",     3),
        (r"\bpara rehabilitar\b",         3),
        (r"\bhabitable pero\b",           2),
        (r"\bcon potencial\b",            1),
        (r"\bprecio seg[uú]n estado\b",   2),
        (r"\bpara actualizar\b",          2),
        (r"\bantigua\b",                  1),
    ],
}

# Bonus points per category when at least one match is found
CATEGORY_BONUS = {
    "urgency":    15,
    "direct":     10,
    "negotiable": 10,
    "renovated":   5,
    "needs_work":  5,
}


# ── Core analysis ──────────────────────────────────────────────────────────────

def analyze_description(text: Optional[str]) -> Dict:
    """
    Analyse a single listing description and return detected signals.

    Returns:
        {
          "urgency":      bool,
          "direct":       bool,
          "negotiable":   bool,
          "renovated":    bool,
          "needs_work":   bool,
          "matched_keywords": List[str],   # all matched pattern strings
          "nlp_bonus":    int,             # 0-45 additive bonus for opp. score
          "signal_count": int,
        }
    """
    result = {cat: False for cat in SIGNALS}
    matched = []

    if not text or not isinstance(text, str):
        return {**result, "matched_keywords": [], "nlp_bonus": 0, "signal_count": 0}

    text_lower = text.lower()

    for category, patterns in SIGNALS.items():
        for pattern, _ in patterns:
            if re.search(pattern, text_lower):
                result[category] = True
                # Collect human-readable keyword (first capturing group or pattern core)
                keyword = re.sub(r"\\b|[\\()?+]", "", pattern).strip()
                matched.append(keyword)
                break  # one match per category is enough

    bonus = sum(CATEGORY_BONUS[cat] for cat, found in result.items() if found)
    signal_count = sum(1 for found in result.values() if found)

    return {
        **result,
        "matched_keywords": matched,
        "nlp_bonus":        min(bonus, 45),   # cap at 45
        "signal_count":     signal_count,
    }


def signals_to_badges(signals: Dict) -> str:
    """Return a compact emoji string for display in the UI."""
    badges = []
    if signals.get("urgency"):    badges.append("🔴 Urgente")
    if signals.get("direct"):     badges.append("💼 Directo")
    if signals.get("negotiable"): badges.append("🟡 Negociable")
    if signals.get("renovated"):  badges.append("🟢 Reformado")
    if signals.get("needs_work"): badges.append("🔧 A reformar")
    return "  ·  ".join(badges)


# ── Database storage ───────────────────────────────────────────────────────────

def _get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_signals_table():
    """Create listing_signals table if it doesn't exist."""
    with _get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS listing_signals (
                listing_id    TEXT PRIMARY KEY,
                urgency       INTEGER NOT NULL DEFAULT 0,
                direct        INTEGER NOT NULL DEFAULT 0,
                negotiable    INTEGER NOT NULL DEFAULT 0,
                renovated     INTEGER NOT NULL DEFAULT 0,
                needs_work    INTEGER NOT NULL DEFAULT 0,
                nlp_bonus     INTEGER NOT NULL DEFAULT 0,
                signal_count  INTEGER NOT NULL DEFAULT 0,
                analyzed_at   TEXT    NOT NULL DEFAULT (datetime('now'))
            )
        """)
        conn.commit()
    print("✓ listing_signals table ready")


def upsert_signals(listing_id: str, signals: Dict) -> None:
    """Insert or replace NLP signals for a listing."""
    with _get_connection() as conn:
        conn.execute("""
            INSERT INTO listing_signals
                (listing_id, urgency, direct, negotiable, renovated,
                 needs_work, nlp_bonus, signal_count, analyzed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(listing_id) DO UPDATE SET
                urgency      = excluded.urgency,
                direct       = excluded.direct,
                negotiable   = excluded.negotiable,
                renovated    = excluded.renovated,
                needs_work   = excluded.needs_work,
                nlp_bonus    = excluded.nlp_bonus,
                signal_count = excluded.signal_count,
                analyzed_at  = excluded.analyzed_at
        """, (
            listing_id,
            int(signals.get("urgency", False)),
            int(signals.get("direct", False)),
            int(signals.get("negotiable", False)),
            int(signals.get("renovated", False)),
            int(signals.get("needs_work", False)),
            signals.get("nlp_bonus", 0),
            signals.get("signal_count", 0),
        ))
        conn.commit()


def get_signals_for_listings(listing_ids: List[str]) -> Dict[str, Dict]:
    """
    Return NLP signals dict keyed by listing_id for a list of IDs.
    Missing IDs get an empty signals dict.
    """
    if not listing_ids:
        return {}
    placeholders = ",".join("?" * len(listing_ids))
    with _get_connection() as conn:
        rows = conn.execute(f"""
            SELECT listing_id, urgency, direct, negotiable,
                   renovated, needs_work, nlp_bonus, signal_count
            FROM listing_signals
            WHERE listing_id IN ({placeholders})
        """, tuple(listing_ids)).fetchall()

    result = {}
    for row in rows:
        result[row["listing_id"]] = {
            "urgency":     bool(row["urgency"]),
            "direct":      bool(row["direct"]),
            "negotiable":  bool(row["negotiable"]),
            "renovated":   bool(row["renovated"]),
            "needs_work":  bool(row["needs_work"]),
            "nlp_bonus":   row["nlp_bonus"],
            "signal_count": row["signal_count"],
        }
    return result


# ── Batch processing ───────────────────────────────────────────────────────────

def run_nlp_batch(force_reanalyze: bool = False, batch_size: int = 500) -> Dict:
    """
    Analyze all listings with descriptions that haven't been processed yet.

    Args:
        force_reanalyze: If True, reprocess all listings (even already analyzed).
        batch_size:      Number of listings to process per DB round-trip.

    Returns:
        { "processed": int, "with_signals": int, "skipped": int }
    """
    init_signals_table()

    with _get_connection() as conn:
        if force_reanalyze:
            rows = conn.execute("""
                SELECT listing_id, description FROM listings
                WHERE description IS NOT NULL AND description != ''
            """).fetchall()
        else:
            rows = conn.execute("""
                SELECT l.listing_id, l.description
                FROM listings l
                LEFT JOIN listing_signals s ON s.listing_id = l.listing_id
                WHERE l.description IS NOT NULL
                  AND l.description != ''
                  AND s.listing_id IS NULL
            """).fetchall()

    if not rows:
        print("✓ No new descriptions to analyze")
        return {"processed": 0, "with_signals": 0, "skipped": 0}

    print(f"🔍 Analyzing {len(rows):,} descriptions...")
    processed = 0
    with_signals = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        for row in batch:
            signals = analyze_description(row["description"])
            upsert_signals(row["listing_id"], signals)
            processed += 1
            if signals["signal_count"] > 0:
                with_signals += 1

        pct = min(100, round((i + len(batch)) / len(rows) * 100))
        print(f"  [{pct:>3}%] {i + len(batch):,}/{len(rows):,} procesados", end="\r")

    print(f"\n✅ NLP completo: {processed:,} analizados, {with_signals:,} con señales")
    return {"processed": processed, "with_signals": with_signals, "skipped": 0}


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    force = "--force" in sys.argv
    stats = run_nlp_batch(force_reanalyze=force)

    print(f"\n📊 Resumen NLP:")
    print(f"  Procesados:    {stats['processed']:,}")
    print(f"  Con señales:   {stats['with_signals']:,}")
    if stats["processed"] > 0:
        pct = stats["with_signals"] / stats["processed"] * 100
        print(f"  Tasa señales:  {pct:.1f}%")

    # Quick preview
    with _get_connection() as conn:
        top = conn.execute("""
            SELECT l.listing_id, l.barrio, l.price, s.nlp_bonus,
                   s.urgency, s.direct, s.negotiable, s.renovated, s.needs_work
            FROM listing_signals s
            JOIN listings l ON l.listing_id = s.listing_id
            WHERE s.signal_count > 0 AND l.status = 'active'
            ORDER BY s.nlp_bonus DESC, s.signal_count DESC
            LIMIT 10
        """).fetchall()

    print(f"\n🏆 Top 10 propiedades con más señales NLP:")
    for r in top:
        badges = []
        if r["urgency"]:    badges.append("🔴")
        if r["direct"]:     badges.append("💼")
        if r["negotiable"]: badges.append("🟡")
        if r["renovated"]:  badges.append("🟢")
        if r["needs_work"]: badges.append("🔧")
        print(f"  {' '.join(badges)} {r['barrio']} — €{r['price']:,} — bonus +{r['nlp_bonus']}")
