#!/usr/bin/env python3
"""
fetch_opendata.py — Descarga el Panel de Indicadores de Distritos y Barrios
de datos.madrid.es y genera district_opendata.json para market-thermometer.

Uso:
    python fetch_opendata.py

Genera:
    market-thermometer/public/district_opendata.json

Fuente:
    https://datos.madrid.es/dataset/300087-0-indicadores-distritos
"""

import csv
import io
import json
import os
import sys
from collections import defaultdict
from datetime import datetime

import requests

# ── URL del CSV consolidado (todas las ediciones) ──────────────────────────
CSV_URL = (
    "https://datos.madrid.es/dataset/300087-0-indicadores-distritos/"
    "resource/300087-0-indicadores-distritos-csv/"
    "download/300087-0-indicadores-distritos-csv"
)

# ── Output path ────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(
    SCRIPT_DIR, "market-thermometer", "public", "district_opendata.json"
)

# ── Indicadores que queremos extraer ──────────────────────────────────────
# Mapa: substring del campo "indicador_completo" → clave JSON de salida
INDICATOR_MAP = {
    # Demografía
    "Número de habitantes": "poblacion",
    "Edad media de la población": "edad_media",
    "Población extranjera (%)": "pct_extranjeros",
    "Densidad de población (hab./Ha.)": "densidad_hab_ha",
    # Renta
    "Renta neta media anual de los hogares": "renta_media_hogar",
    "Renta disponible media por persona": "renta_media_persona",
    # Educación — buscar la versión % explícita primero
    "Estudios superiores (%)": "pct_estudios_superiores",
    "% estudios superiores": "pct_estudios_superiores",
    "Porcentaje de población con estudios superiores": "pct_estudios_superiores",
    # Tamaño hogar
    "Tamaño medio del hogar": "tamano_medio_hogar",
    # Natalidad / Mortalidad
    "Tasa bruta de natalidad": "tasa_natalidad",
}

# Indicadores alternativos (si el nombre exacto no coincide, probar estos)
INDICATOR_FALLBACKS = {
    "Número habitantes": "poblacion",
    "Habitantes": "poblacion",
    "Edad media": "edad_media",
    "Porcentaje de población extranjera": "pct_extranjeros",
    "% Población extranjera": "pct_extranjeros",
    "Densidad": "densidad_hab_ha",
    "Renta neta media": "renta_media_hogar",
    "Renta media por hogar": "renta_media_hogar",
    "Renta disponible per cápita": "renta_media_persona",
    "Renta media por persona": "renta_media_persona",
    "% Estudios superiores": "pct_estudios_superiores",
    "Estudios superiores": "pct_estudios_superiores",
    "Tamaño medio hogar": "tamano_medio_hogar",
    "Tasa natalidad": "tasa_natalidad",
}

# ── Mapping nombres oficiales → nombres del dashboard ─────────────────────
# Los nombres de distritos en datos.madrid.es pueden diferir ligeramente.
DISTRICT_NAME_MAP = {
    # datos.madrid.es name → nombre en el dashboard
    "Arganzuela": "Arganzuela",
    "Barajas": "Barajas",
    "Carabanchel": "Carabanchel",
    "Centro": "Centro",
    "Chamartín": "Chamartín",
    "Chamberí": "Chamberí",
    "Ciudad Lineal": "Ciudad Lineal",
    "Fuencarral-El Pardo": "Fuencarral-El Pardo",
    "Hortaleza": "Hortaleza",
    "Latina": "Latina",
    "Moncloa-Aravaca": "Moncloa-Aravaca",
    "Moratalaz": "Moratalaz",
    "Puente de Vallecas": "Puente de Vallecas",
    "Retiro": "Retiro",
    "Salamanca": "Salamanca",
    "San Blas-Canillejas": "San Blas-Canillejas",
    "Tetuán": "Tetuán",
    "Usera": "Usera",
    "Vicálvaro": "Vicálvaro",
    "Villa de Vallecas": "Villa de Vallecas",
    "Villaverde": "Villaverde",
}

# Mapping barrios oficiales → barrios del dashboard
# Los barrios en el dashboard usan nombres combinados (Chueca-Justicia, etc.)
# mientras datos.madrid.es usa los nombres oficiales (Justicia, Embajadores, etc.)
# Este mapping se aplica a nivel de barrio.
BARRIO_NAME_MAP = {
    # Centro
    "Justicia": "Chueca-Justicia",
    "Cortes": "Huertas-Cortes",
    "Embajadores": "Lavapiés-Embajadores",
    "Universidad": "Malasaña-Universidad",
    "Palacio": "Palacio",
    "Sol": "Sol",
    # Chamartín
    "Hispanoamérica": "Bernabéu-Hispanoamérica",
    "Castilla": "Castilla",
    "Ciudad Jardín": "Ciudad Jardín",
    "El Viso": "El Viso",
    "Nueva España": "Nueva España",
    "Prosperidad": "Prosperidad",
    # Chamberí
    "Almagro": "Almagro",
    "Arapiles": "Arapiles",
    "Gaztambide": "Gaztambide",
    "Ríos Rosas": "Nuevos Ministerios-Ríos Rosas",
    "Trafalgar": "Trafalgar",
    "Vallehermoso": "Vallehermoso",
    # Hortaleza
    "Piovera": "Conde Orgaz-Piovera",
    "Valdefuentes": "Valdebebas-Valdefuentes",
    # Fuencarral-El Pardo
    "Valverde": "Tres Olivos-Valverde",
    # Carabanchel
    "Buenavista": "Buena Vista",
    "Pau de Carabanchel": "PAU de Carabanchel",
}


def download_csv() -> str:
    """Descarga el CSV consolidado de datos.madrid.es."""
    print(f"Descargando CSV de datos.madrid.es...")
    resp = requests.get(CSV_URL, timeout=60)
    resp.raise_for_status()

    # Detect encoding
    encoding = resp.apparent_encoding or "utf-8"
    text = resp.content.decode(encoding)
    print(f"  → {len(text)} caracteres, encoding={encoding}")
    return text


def parse_csv(text: str) -> list[dict]:
    """Parsea el CSV semicolon-separated y devuelve lista de dicts."""
    # datos.madrid.es usa ';' como separador
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    rows = list(reader)
    print(f"  → {len(rows)} filas, columnas: {list(rows[0].keys()) if rows else 'N/A'}")
    return rows


def match_indicator(indicador_completo: str) -> str | None:
    """Intenta mapear un nombre de indicador a nuestra clave JSON."""
    # Primer intento: coincidencia exacta con INDICATOR_MAP
    for pattern, key in INDICATOR_MAP.items():
        if pattern.lower() in indicador_completo.lower():
            return key
    # Segundo intento: fallbacks
    for pattern, key in INDICATOR_FALLBACKS.items():
        if pattern.lower() in indicador_completo.lower():
            return key
    return None


def parse_value(val_str: str) -> float | None:
    """Convierte string numérico español (1.234,56) a float."""
    if not val_str or val_str.strip() in ("", "-", "...", "N/D"):
        return None
    try:
        # Quitar puntos de miles, cambiar coma decimal a punto
        clean = val_str.strip().replace(".", "").replace(",", ".")
        return float(clean)
    except ValueError:
        return None


def normalize_district_name(name: str) -> str:
    """Normaliza el nombre de distrito al formato del dashboard."""
    name = name.strip()
    return DISTRICT_NAME_MAP.get(name, name)


def normalize_barrio_name(name: str) -> str:
    """Normaliza el nombre de barrio al formato del dashboard."""
    name = name.strip()
    return BARRIO_NAME_MAP.get(name, name)


def process_data(rows: list[dict]) -> dict:
    """Procesa las filas del CSV y genera la estructura JSON final."""
    # Agrupar por (distrito, barrio, indicador) quedándonos con el año más reciente
    # Estructura: data[geo_type][geo_name][indicator_key] = value
    distritos = defaultdict(dict)  # distrito_name → {key: value}
    barrios = defaultdict(dict)    # barrio_name → {key: value, distrito: str}

    # Track años para metadata
    max_year = 0
    unmatched_indicators = set()
    matched_count = 0

    # Ordenar por año descendente para quedarnos con el dato más reciente
    rows_sorted = sorted(rows, key=lambda r: r.get("año", "0"), reverse=True)

    seen_distrito = set()  # (distrito, key) — para dedup
    seen_barrio = set()    # (barrio, key) — para dedup

    for row in rows_sorted:
        indicador = row.get("indicador_completo", "")
        key = match_indicator(indicador)
        if key is None:
            unmatched_indicators.add(indicador[:80])
            continue

        value = parse_value(row.get("valor_indicador", ""))
        if value is None:
            continue

        year = int(row.get("año", "0") or "0")
        if year > max_year:
            max_year = year

        distrito_raw = row.get("distrito", "").strip()
        barrio_raw = row.get("barrio", "").strip()
        cod_barrio = row.get("cod_barrio", "").strip()

        if not distrito_raw:
            continue

        distrito_name = normalize_district_name(distrito_raw)

        # ¿Es dato de barrio o de distrito?
        if barrio_raw and cod_barrio and cod_barrio != "0":
            # Dato a nivel de barrio
            barrio_name = normalize_barrio_name(barrio_raw)
            bkey = (barrio_name, key)
            if bkey not in seen_barrio:
                seen_barrio.add(bkey)
                barrios[barrio_name][key] = value
                barrios[barrio_name]["distrito"] = distrito_name
                matched_count += 1
        else:
            # Dato a nivel de distrito
            dkey = (distrito_name, key)
            if dkey not in seen_distrito:
                seen_distrito.add(dkey)
                distritos[distrito_name][key] = value
                matched_count += 1

    print(f"  → {matched_count} valores extraídos")
    print(f"  → {len(distritos)} distritos, {len(barrios)} barrios")
    print(f"  → Año más reciente: {max_year}")

    if unmatched_indicators:
        print(f"\n  ℹ️ Indicadores no mapeados ({len(unmatched_indicators)} únicos):")
        for ind in sorted(unmatched_indicators)[:20]:
            print(f"    - {ind}")
        if len(unmatched_indicators) > 20:
            print(f"    ... y {len(unmatched_indicators) - 20} más")

    return {
        "metadata": {
            "source": "datos.madrid.es — Panel de Indicadores de Distritos y Barrios",
            "url": "https://datos.madrid.es/dataset/300087-0-indicadores-distritos",
            "year": max_year,
            "generated_at": datetime.now().isoformat(),
        },
        "distritos": dict(distritos),
        "barrios": dict(barrios),
    }


def main():
    # 1. Descargar CSV
    try:
        text = download_csv()
    except Exception as e:
        print(f"❌ Error descargando CSV: {e}")
        sys.exit(1)

    # 2. Parsear
    rows = parse_csv(text)
    if not rows:
        print("❌ CSV vacío o mal formateado")
        sys.exit(1)

    # 3. Procesar
    result = process_data(rows)

    # 4. Validar que tenemos los 21 distritos
    expected_districts = [
        "Arganzuela", "Barajas", "Carabanchel", "Centro", "Chamartín",
        "Chamberí", "Ciudad Lineal", "Fuencarral-El Pardo", "Hortaleza",
        "Latina", "Moncloa-Aravaca", "Moratalaz", "Puente de Vallecas",
        "Retiro", "Salamanca", "San Blas-Canillejas", "Tetuán", "Usera",
        "Vicálvaro", "Villa de Vallecas", "Villaverde",
    ]
    found = set(result["distritos"].keys())
    missing = [d for d in expected_districts if d not in found]
    extra = [d for d in found if d not in expected_districts]

    if missing:
        print(f"\n⚠️  Distritos no encontrados ({len(missing)}): {missing}")
        print("   Puede que el nombre en datos.madrid.es sea diferente.")
        print("   Revisa DISTRICT_NAME_MAP en este script.")
    if extra:
        print(f"\n⚠️  Distritos extra no esperados: {extra}")

    # 5. Escribir JSON
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\n✅ Generado: {OUTPUT_PATH}")
    print(f"   Tamaño: {os.path.getsize(OUTPUT_PATH) / 1024:.1f} KB")

    # 6. Preview
    print("\n📊 Preview (Centro):")
    centro = result["distritos"].get("Centro", {})
    for k, v in centro.items():
        print(f"   {k}: {v}")


if __name__ == "__main__":
    main()
