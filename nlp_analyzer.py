"""
NLP Analyzer — keyword-based extraction from property descriptions.

Two parallel extractors run on the same description text:

  1. SELLER SIGNALS (table: listing_signals)

       🔴 urgency      — seller motivation / time pressure  (+15 bonus)
       💼 direct       — direct owner, no agency fee        (+10 bonus)
       🟡 negotiable   — price negotiable or reduced        (+10 bonus)
       🟢 renovated    — recently renovated / move-in ready (+5 bonus)
       🔧 needs_work   — needs reform (risk / discount opp) (+5 bonus)

     Used by the opportunity-scoring pipeline.

  2. PHYSICAL AMENITIES (table: listing_amenities)

       Booleans:        has_terraza, has_balcon, has_garaje, has_trastero,
                        has_piscina, has_ascensor, has_portero,
                        has_aire_acondicionado, has_calefaccion,
                        has_armarios_empotrados
       Proximity:       near_metro, near_parque, near_colegio, near_hospital
       Year (nullable): construction_year   (1800-2030)

     Negative-mention support for the most-often-negated amenities
     (`sin ascensor`, `sin garaje`, `no dispone de calefacción`, …) so
     that "sin ascensor" returns has_ascensor = False, not True.

     Used by detail_tab and (later) the predictive model as features.

Usage:
    from nlp_analyzer import analyze_description, extract_amenities, run_nlp_batch

    signals   = analyze_description("Venta urgente, propietario directo")
    amenities = extract_amenities("Piso reformado con terraza y plaza de garaje")
    run_nlp_batch()        # both signals AND amenities for missing listings
    run_nlp_batch(force_reanalyze=True)   # re-run on the whole table
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


# ── Amenity dictionaries ──────────────────────────────────────────────────────
# Each amenity has:
#   pos:  positive patterns — match means "has it"
#   neg:  negative patterns (optional) — match means "explicitly does not have it",
#         and overrides any positive match.
#
# Ordered loosely by how common explicit negation is in Spanish listings.

AMENITIES: Dict[str, Dict[str, List[str]]] = {
    # ── Physical features ──────────────────────────────────────────────────
    "has_terraza": {
        "pos": [
            r"\bterrazas?\b",
            r"\bterraza\s+de\s+\d+",
        ],
        "neg": [r"\bsin\s+terrazas?\b"],
    },
    "has_balcon": {
        "pos": [r"\bbalc[oó]n(es)?\b"],
        "neg": [r"\bsin\s+balc[oó]n\b"],
    },
    "has_garaje": {
        "pos": [
            r"\bgaraje\b",
            r"\bplaza\s+de\s+(garaje|aparcamiento|parking)\b",
            r"\bparking\b",
            r"\bcochera\b",
        ],
        "neg": [
            r"\bsin\s+garaje\b",
            r"\bno\s+(tiene|dispone\s+de|incluye|hay)\s+garaje\b",
            r"\bsin\s+plaza\s+de\s+(garaje|aparcamiento)\b",
        ],
    },
    "has_trastero": {
        "pos": [r"\btrastero\b"],
        "neg": [r"\bsin\s+trastero\b"],
    },
    "has_piscina": {
        "pos": [r"\bpiscinas?\b"],
        "neg": [r"\bsin\s+piscina\b"],
    },
    "has_ascensor": {
        "pos": [r"\bascensor(es)?\b", r"\bcon\s+ascensor\b"],
        "neg": [
            r"\bsin\s+ascensor\b",
            r"\bno\s+(tiene|dispone\s+de|cuenta\s+con|hay)\s+ascensor\b",
        ],
    },
    "has_portero": {
        "pos": [r"\bportero\s+autom[aá]tico\b", r"\bportero\b", r"\bportería\b", r"\bconserje\b"],
        "neg": [r"\bsin\s+portero\b"],
    },
    "has_aire_acondicionado": {
        "pos": [
            r"\baire\s+acondicionado\b",
            r"\bclimatizaci[oó]n\b",
            r"\bbomba\s+de\s+calor\b",
        ],
        "neg": [r"\bsin\s+aire\s+acondicionado\b"],
    },
    "has_calefaccion": {
        "pos": [
            r"\bcalefacci[oó]n\b",
            r"\bcaldera\s+de\s+gas\b",
            r"\bsuelo\s+radiante\b",
        ],
        "neg": [
            r"\bsin\s+calefacci[oó]n\b",
            r"\bno\s+(tiene|dispone\s+de)\s+calefacci[oó]n\b",
        ],
    },
    "has_armarios_empotrados": {
        "pos": [r"\barmarios?\s+empotrad[oa]s?\b"],
        "neg": [],
    },
    # ── Proximity (must be specific enough to avoid false positives) ───────
    "near_metro": {
        "pos": [
            r"\b(estaci[oó]n|parada)\s+de\s+metro\b",
            r"\bmetro\s+(L\d|línea\s+\d+)\b",
            r"\b(cerca|junto|próxim[oa]|al\s+lado)\s+(de|del|al)\s+metro\b",
            r"\bmetro\s+(cercan[oa]|próxim[oa])\b",
            r"\ba\s+\d+\s+min(uto)?s?\s+(del|en)\s+metro\b",
        ],
        "neg": [],
    },
    "near_parque": {
        "pos": [
            r"\b(parque\s+de(l)?)\s+\w+",  # e.g. "parque del Retiro"
            r"\b(junto|cerca|próxim[oa]|frente|al\s+lado)\s+(de|del|al)\s+parque\b",
            r"\bvistas?\s+al\s+parque\b",
            r"\bparques?\s+cercanos?\b",
        ],
        "neg": [],
    },
    "near_colegio": {
        "pos": [
            r"\bcolegios?\s+(cercanos?|próxim[oa]s?|al\s+lado)\b",
            r"\b(cerca|junto|próxim[oa])\s+(de|al|a)\s+colegios?\b",
            r"\bzona\s+de\s+colegios?\b",
        ],
        "neg": [],
    },
    "near_hospital": {
        "pos": [
            r"\b(cerca|junto|próxim[oa])\s+(de|del|al)\s+hospital\b",
            r"\bhospital\s+(cercano|próxim[oa])\b",
        ],
        "neg": [],
    },
}


# Construction year — accept several common phrasings.  Validate the
# captured year is in [1800, 2030] (Madrid's oldest residential buildings
# are mid-19th century; everything earlier is almost certainly an OCR
# artefact or a postal code mistakenly captured).
YEAR_PATTERNS = [
    # Verb-based ("construido en 1985", "edificado en 1960")
    re.compile(
        r"\b(?:construid[oa]|edificad[oa]|levantad[oa]|reformad[oa])\s+"
        r"(?:en\s+(?:el\s+)?(?:a[ñn]o\s+)?)?(\d{4})\b",
        re.IGNORECASE,
    ),
    # Labelled ("año construcción: 1985", "fecha de construcción 1960")
    re.compile(
        r"\b(?:a[ñn]o|fecha)\s+(?:de\s+)?construcci[oó]n\s*[:\-]?\s*(\d{4})\b",
        re.IGNORECASE,
    ),
    # Building referenced ("edificio del 1985", "edificio de 1965")
    re.compile(
        r"\bedificio\s+(?:del?\s+)?(?:a[ñn]o\s+)?(\d{4})\b",
        re.IGNORECASE,
    ),
    # Property referenced ("vivienda del 2010")
    re.compile(
        r"\b(?:vivienda|piso|inmueble|finca)\s+(?:del?\s+)?(?:a[ñn]o\s+)?(\d{4})\b",
        re.IGNORECASE,
    ),
    # "construcción: 1985" / "construcción 1985"
    re.compile(
        r"\bconstrucci[oó]n\s*[:\-]?\s*(\d{4})\b",
        re.IGNORECASE,
    ),
    # "promoción de 2010", "promoción del año 2010"
    re.compile(
        r"\bpromoci[oó]n\s+(?:del?\s+)?(?:a[ñn]o\s+)?(\d{4})\b",
        re.IGNORECASE,
    ),
    # "del año 1960", "de los años 60" → only catch 4-digit, "años 60"
    # cases are too imprecise to commit to a specific year.
    re.compile(
        r"\bdel\s+a[ñn]o\s+(\d{4})\b",
        re.IGNORECASE,
    ),
]


# ── Energy certification (A-G) ─────────────────────────────────────────────────
#
# Spanish listings use the Real Decreto 235/2013 letter scale.  Common
# phrasings:
#   "certificación energética: B"
#   "calificación energética E"
#   "consumo: A · emisiones: A"  (we take the consumption letter)
#   "etiqueta energética: C"
#   "en trámite" / "exento" — non-letter outcomes we also surface
#
# The patterns capture the first single A-G letter that follows the
# energy-related label.  We refuse to match a bare letter without a
# label since it would collide with floor numbers ("planta B").

_ENERGY_LETTER_RE = re.compile(
    r"\b(?:certificad[oa]|certificaci[oó]n|calificaci[oó]n|consumo|etiqueta|"
    r"clasificaci[oó]n)\s+"
    r"(?:energ[eé]tic[oa]\s*)?(?:de\s+)?(?:consumo\s*)?[:\-]?\s*"
    r"\(?([A-G])\)?\b",
    re.IGNORECASE,
)
_ENERGY_LABELLED_RE = re.compile(
    r"\benerg[eé]tic[oa]\s*[:\-]?\s*\(?([A-G])\)?\b",
    re.IGNORECASE,
)
_ENERGY_EXEMPT_RE = re.compile(
    r"\b(?:certificad[oa]|certificaci[oó]n|calificaci[oó]n|etiqueta)\s+"
    r"(?:energ[eé]tic[oa]\s*[:\-]?\s*)?"
    r"(?:en\s+tr[aá]mite|exento|exenta|pendiente)\b",
    re.IGNORECASE,
)


# ── Condition / state ──────────────────────────────────────────────────────────
#
# A single categorical condition per listing.  Categories chosen so the
# offer engine can map them to discount/boost factors directly:
#
#   obra_nueva     — strong positive (new build, no immediate works)
#   reformado      — positive (recent renovation, move-in ready)
#   buen_estado    — neutral (habitable, no major works)
#   a_reformar     — negative (needs work)
#   para_reformar  — strongly negative (gut renovation / "para reformar
#                    integral" / "necesita reforma completa")
#
# Patterns are listed in priority order: stronger phrasings beat weaker
# ones inside the same listing.

_CONDITION_PATTERNS = [
    ("obra_nueva", [
        r"\bobra\s+nueva\b",
        r"\bvivienda\s+nueva\b",
        r"\ba\s+estrenar\b",
        r"\bsin\s+estrenar\b",
        r"\bnueva\s+construcci[oó]n\b",
    ]),
    ("para_reformar", [
        r"\breforma\s+integral\b",
        r"\breforma\s+completa\b",
        # Negative-lookbehind for "no" so "no necesita reforma" — a
        # *positive* description — doesn't get misclassified as
        # needing one.
        r"(?<!no\s)\bnecesita\s+reforma\b",
        r"\bpara\s+reformar\s+(?:integral|completa)",
    ]),
    ("a_reformar", [
        # Same negative-lookbehind discipline so "no a reformar" /
        # "no necesita..." don't fire.
        r"(?<!no\s)\ba\s+reformar\b",
        r"(?<!no\s)\bpara\s+reformar\b",
        r"\bpiso\s+a\s+reformar\b",
        r"(?<!no\s)\bnecesita\s+actualizaci[oó]n\b",
    ]),
    ("reformado", [
        r"\b(?:totalmente|completamente|integralmente)\s+reformad[oa]\b",
        r"\brecientemente\s+reformad[oa]\b",
        r"\breci[eé]n\s+reformad[oa]\b",
        r"\breformad[oa]\s+(?:en|el\s+a[ñn]o)\s+20\d{2}\b",
        r"\bvivienda\s+reformad[oa]\b",
        r"\bpiso\s+reformad[oa]\b",
    ]),
    ("buen_estado", [
        r"\bbuen\s+estado\b",
        r"\bperfecto\s+estado\b",
        r"\bmuy\s+buen\s+estado\b",
        r"\bestado\s+impecable\b",
        r"\blistos?\s+para\s+entrar\b",
        r"\bpara\s+entrar\s+a\s+vivir\b",
    ]),
]


def _extract_energy_certification(text: str) -> Optional[str]:
    """Return ``'A'`` … ``'G'``, ``'exento'``, ``'en_tramite'``, or None.

    Matches in priority order: explicit label + letter > generic
    "energético: X" > exempt/pending phrasings.  Falsey on missing.
    """
    for rx in (_ENERGY_LETTER_RE, _ENERGY_LABELLED_RE):
        m = rx.search(text)
        if m:
            return m.group(1).upper()
    m = _ENERGY_EXEMPT_RE.search(text)
    if m:
        return "exento" if "exento" in m.group(0).lower() or "exenta" in m.group(0).lower() else "en_tramite"
    return None


def _extract_condition(text: str) -> Optional[str]:
    """Return one of the five condition labels or None.

    Walks the pattern table in priority order so a description with both
    "obra nueva" and "a reformar" (e.g. quoting comparables) gets the
    stronger primary signal.
    """
    text_lower = text.lower()
    for label, patterns in _CONDITION_PATTERNS:
        if any(re.search(p, text_lower) for p in patterns):
            return label
    return None


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


# ── Amenity extraction ────────────────────────────────────────────────────────

def extract_amenities(text: Optional[str]) -> Dict:
    """
    Extract physical amenities, proximity flags, construction year,
    energy certification and condition from a Spanish listing description.

    Returns a dict of:
      - 14 boolean flags  (has_*, near_*)
      - construction_year:    int | None  (1800-2030)
      - energy_certification: str | None  ('A'..'G' | 'exento' | 'en_tramite')
      - condition:            str | None  (one of the categorical labels
                                            from ``_CONDITION_PATTERNS``)
      - amenities_count:      int — how many of the boolean flags are True
    """
    result = {name: False for name in AMENITIES}
    result["construction_year"]    = None
    result["energy_certification"] = None
    result["condition"]            = None
    result["amenities_count"]      = 0

    if not text or not isinstance(text, str):
        return result

    text_lower = text.lower()

    for name, patterns in AMENITIES.items():
        # Negative patterns win — explicit "sin X" or "no tiene X" overrides.
        if any(re.search(p, text_lower) for p in patterns.get("neg", [])):
            result[name] = False
            continue
        result[name] = any(re.search(p, text_lower) for p in patterns["pos"])

    # Construction year — first valid match wins
    for pat in YEAR_PATTERNS:
        m = pat.search(text)
        if m:
            try:
                year = int(m.group(1))
                if 1800 <= year <= 2030:
                    result["construction_year"] = year
                    break
            except (ValueError, IndexError):
                pass

    result["energy_certification"] = _extract_energy_certification(text)
    result["condition"]            = _extract_condition(text)

    result["amenities_count"] = sum(
        1 for k, v in result.items() if k.startswith(("has_", "near_")) and v
    )

    return result


# Pretty-display helpers
_AMENITY_LABELS = [
    ("has_terraza",            "🌅 Terraza"),
    ("has_balcon",             "🪟 Balcón"),
    ("has_garaje",             "🚗 Garaje"),
    ("has_trastero",           "📦 Trastero"),
    ("has_piscina",            "🏊 Piscina"),
    ("has_ascensor",           "🛗 Ascensor"),
    ("has_portero",            "🛎️ Portero"),
    ("has_aire_acondicionado", "❄️ A/C"),
    ("has_calefaccion",        "🔥 Calefacción"),
    ("has_armarios_empotrados","🚪 Armarios empotrados"),
    ("near_metro",             "🚇 Metro cerca"),
    ("near_parque",            "🌳 Parque cerca"),
    ("near_colegio",           "🏫 Colegios cerca"),
    ("near_hospital",          "🏥 Hospital cerca"),
]


def amenities_to_badges(amenities: Dict) -> List[str]:
    """Return a list of emoji+label strings ready to render as Streamlit pills."""
    return [label for key, label in _AMENITY_LABELS if amenities.get(key)]


# ── Database storage ───────────────────────────────────────────────────────────

def _get_connection():
    """Backend-dispatching connection (shim).

    Pre-cutover this module hardcoded ``sqlite3.connect("real_estate.db")``
    which silently broke on Streamlit Cloud after Phase D: the cloud
    container has no SQLite file, so the call created an empty one and
    every read returned zero rows.  The detail page papered over this
    with an in-Python ``extract_amenities`` fallback on the raw
    description, but the actual ``listing_amenities`` rows on Supabase
    (≈19k after backfill) were never consulted.

    Now routed through the same shim every other module uses, so reads
    and writes both hit the live Postgres on prod / SQLite locally.
    Returns a context-manager-style connection from ``db.connection``.
    """
    from db.connection import get_connection
    return get_connection()


def init_signals_table():
    """Create listing_signals table if it doesn't exist.

    No-op on Postgres — Alembic owns the schema (see
    ``alembic/versions/0001_initial_schema.py``).  SQLite branches keep
    the inline CREATE so local dev workflows that bootstrap a fresh DB
    don't need alembic.
    """
    from db.dialect import is_postgres
    if is_postgres():
        return
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
    print("✓ listing_signals table ready")


def init_amenities_table():
    """Create listing_amenities table if it doesn't exist.

    No-op on Postgres — Alembic owns the schema (initial in 0001,
    plus ``energy_certification`` / ``condition`` columns from 0005).
    """
    from db.dialect import is_postgres
    if is_postgres():
        return
    with _get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS listing_amenities (
                listing_id              TEXT PRIMARY KEY,
                has_terraza             INTEGER NOT NULL DEFAULT 0,
                has_balcon              INTEGER NOT NULL DEFAULT 0,
                has_garaje              INTEGER NOT NULL DEFAULT 0,
                has_trastero            INTEGER NOT NULL DEFAULT 0,
                has_piscina             INTEGER NOT NULL DEFAULT 0,
                has_ascensor            INTEGER NOT NULL DEFAULT 0,
                has_portero             INTEGER NOT NULL DEFAULT 0,
                has_aire_acondicionado  INTEGER NOT NULL DEFAULT 0,
                has_calefaccion         INTEGER NOT NULL DEFAULT 0,
                has_armarios_empotrados INTEGER NOT NULL DEFAULT 0,
                near_metro              INTEGER NOT NULL DEFAULT 0,
                near_parque             INTEGER NOT NULL DEFAULT 0,
                near_colegio            INTEGER NOT NULL DEFAULT 0,
                near_hospital           INTEGER NOT NULL DEFAULT 0,
                construction_year       INTEGER,
                amenities_count         INTEGER NOT NULL DEFAULT 0,
                energy_certification    TEXT,
                condition               TEXT,
                analyzed_at             TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        # Useful for "find me listings with garage AND elevator"-type queries
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_amenities_count
            ON listing_amenities(amenities_count DESC)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_amenities_year
            ON listing_amenities(construction_year)
            WHERE construction_year IS NOT NULL
        """)
    print("✓ listing_amenities table ready")


def upsert_signals(listing_id: str, signals: Dict) -> None:
    """Insert or replace NLP signals for a listing."""
    from db.dialect import current_timestamp
    now = current_timestamp()
    # ``urgency`` and friends are BOOLEAN in Postgres (per Alembic 0001)
    # and INTEGER-acting-as-bool in SQLite.  psycopg accepts Python
    # ``bool`` for BOOLEAN, and SQLite happily stores it as 0/1 — so
    # passing native bools works for both backends.  ``int(...)`` blew
    # up Postgres with ``smallint vs boolean`` type mismatches.
    with _get_connection() as conn:
        conn.execute(f"""
            INSERT INTO listing_signals
                (listing_id, urgency, direct, negotiable, renovated,
                 needs_work, nlp_bonus, signal_count, analyzed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, {now})
            ON CONFLICT(listing_id) DO UPDATE SET
                urgency      = EXCLUDED.urgency,
                direct       = EXCLUDED.direct,
                negotiable   = EXCLUDED.negotiable,
                renovated    = EXCLUDED.renovated,
                needs_work   = EXCLUDED.needs_work,
                nlp_bonus    = EXCLUDED.nlp_bonus,
                signal_count = EXCLUDED.signal_count,
                analyzed_at  = EXCLUDED.analyzed_at
        """, (
            listing_id,
            bool(signals.get("urgency",    False)),
            bool(signals.get("direct",     False)),
            bool(signals.get("negotiable", False)),
            bool(signals.get("renovated",  False)),
            bool(signals.get("needs_work", False)),
            int(signals.get("nlp_bonus",    0)),
            int(signals.get("signal_count", 0)),
        ))


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


_AMENITY_BOOL_KEYS = [name for name in AMENITIES]

# Order matters — columns lined up with the upsert / select code.
# ``analyzed_at`` is set in SQL via ``current_timestamp()`` so the
# dialect helper handles SQLite vs Postgres.
_AMENITY_EXTRA_COLS = (
    "construction_year",
    "amenities_count",
    "energy_certification",        # added in 0005
    "condition",                   # added in 0005
)


def upsert_amenities(listing_id: str, amenities: Dict) -> None:
    """Insert or replace amenity flags for a listing.

    Uses the shared ``ON CONFLICT`` upsert syntax which works on both
    SQLite and Postgres.  ``analyzed_at`` is set via the dialect's
    ``current_timestamp()`` helper so the same SQL string works on
    both backends.
    """
    from db.dialect import current_timestamp

    cols = _AMENITY_BOOL_KEYS + list(_AMENITY_EXTRA_COLS)
    # All ``has_*`` / ``near_*`` columns are BOOLEAN in Postgres
    # (per Alembic 0001) — pass Python bools so the binary protocol
    # doesn't trip a ``smallint vs boolean`` type mismatch.  SQLite
    # stores bool as 0/1 transparently.
    values: list = [bool(amenities.get(k, False)) for k in _AMENITY_BOOL_KEYS]
    values += [amenities.get(c) for c in _AMENITY_EXTRA_COLS]
    # ``amenities_count`` should default to 0 — never NULL.
    idx = _AMENITY_BOOL_KEYS.__len__() + 1  # construction_year=0, amenities_count=1
    if values[idx] is None:
        values[idx] = 0

    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols)
    placeholders = ", ".join(["?"] * (len(cols) + 1))
    now = current_timestamp()

    with _get_connection() as conn:
        conn.execute(f"""
            INSERT INTO listing_amenities
                (listing_id, {", ".join(cols)}, analyzed_at)
            VALUES ({placeholders}, {now})
            ON CONFLICT(listing_id) DO UPDATE SET
                {set_clause},
                analyzed_at = {now}
        """, (listing_id, *values))


def get_amenities_for_listings(listing_ids: List[str]) -> Dict[str, Dict]:
    """Return amenity dict keyed by listing_id for a list of IDs.

    Output dict includes every flag, ``construction_year``,
    ``amenities_count``, plus the v2 fields ``energy_certification``
    and ``condition`` (both nullable).
    """
    if not listing_ids:
        return {}
    placeholders = ",".join(["?"] * len(listing_ids))
    cols = _AMENITY_BOOL_KEYS + list(_AMENITY_EXTRA_COLS)
    select_cols = "listing_id, " + ", ".join(cols)
    with _get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT {select_cols}
            FROM listing_amenities
            WHERE listing_id IN ({placeholders})
        """, tuple(listing_ids))
        rows = cur.fetchall()

    result: Dict[str, Dict] = {}
    for row in rows:
        d = {k: bool(row[k]) for k in _AMENITY_BOOL_KEYS}
        d["construction_year"]    = row["construction_year"]
        d["amenities_count"]      = row["amenities_count"] or 0
        d["energy_certification"] = row["energy_certification"]
        d["condition"]            = row["condition"]
        result[row["listing_id"]] = d
    return result


# ── Batch processing ───────────────────────────────────────────────────────────

def run_nlp_batch(force_reanalyze: bool = False, batch_size: int = 500) -> Dict:
    """
    Analyze all listings with descriptions that haven't been processed yet,
    populating BOTH listing_signals (seller signals) and listing_amenities
    (physical features + proximity + construction year) in a single pass.

    A listing is considered "to process" if it lacks a row in EITHER table —
    so adding amenities later automatically picks up listings that already
    have signals from a previous run.

    Args:
        force_reanalyze: If True, reprocess all listings (even already analyzed).
        batch_size:      Number of listings to process per DB round-trip.

    Returns:
        { processed, with_signals, with_amenities, skipped }
    """
    init_signals_table()
    init_amenities_table()

    with _get_connection() as conn:
        if force_reanalyze:
            rows = conn.execute("""
                SELECT listing_id, description FROM listings
                WHERE description IS NOT NULL AND description != ''
            """).fetchall()
        else:
            # Pick up any listing missing EITHER signals OR amenities — this
            # is what lets a fresh deployment with the new amenities table
            # backfill cleanly without --force on listings already in
            # listing_signals.
            rows = conn.execute("""
                SELECT l.listing_id, l.description
                FROM listings l
                LEFT JOIN listing_signals  s ON s.listing_id = l.listing_id
                LEFT JOIN listing_amenities a ON a.listing_id = l.listing_id
                WHERE l.description IS NOT NULL
                  AND l.description != ''
                  AND (s.listing_id IS NULL OR a.listing_id IS NULL)
            """).fetchall()

    if not rows:
        print("✓ No new descriptions to analyze")
        return {"processed": 0, "with_signals": 0, "with_amenities": 0, "skipped": 0}

    print(f"🔍 Analyzing {len(rows):,} descriptions (signals + amenities)...")
    processed = 0
    with_signals = 0
    with_amenities = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        for row in batch:
            desc = row["description"]
            signals   = analyze_description(desc)
            amenities = extract_amenities(desc)
            upsert_signals(row["listing_id"], signals)
            upsert_amenities(row["listing_id"], amenities)
            processed += 1
            if signals["signal_count"] > 0:
                with_signals += 1
            if amenities["amenities_count"] > 0 or amenities["construction_year"]:
                with_amenities += 1

        pct = min(100, round((i + len(batch)) / len(rows) * 100))
        print(f"  [{pct:>3}%] {i + len(batch):,}/{len(rows):,} procesados", end="\r")

    print(f"\n✅ NLP completo: {processed:,} analizados — "
          f"{with_signals:,} con señales · {with_amenities:,} con amenities")
    return {
        "processed":      processed,
        "with_signals":   with_signals,
        "with_amenities": with_amenities,
        "skipped":        0,
    }


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
