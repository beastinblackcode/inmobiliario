"""
Macro economic data module for Market Surveillance.
Fetches indicators from INE (Instituto Nacional de Estadística) and ECB (European Central Bank).
"""

import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import json


# ============================================================================
# Constants
# ============================================================================

INE_BASE_URL = "https://servicios.ine.es/wstempus/js/es"

# INE Table IDs
INE_TABLE_IPC = "50902"        # IPC - Índice de Precios de Consumo
INE_TABLE_IPV = "25171"        # IPV - Índice de Precios de Vivienda
INE_TABLE_COMPRAVENTAS = "6150" # Compraventas de vivienda
INE_TABLE_PARO = "4247"        # Tasa de paro EPA
INE_TABLE_HIPOTECAS = "3205"   # Hipotecas constituidas

# ECB API for Euribor
ECB_EURIBOR_URL = (
    "https://data-api.ecb.europa.eu/service/data/FM/"
    "M.U2.EUR.RT.MM.EURIBOR1YD_.HSTA"
    "?format=jsondata&lastNObservations=12"
)

REQUEST_TIMEOUT = 15  # seconds


# ============================================================================
# Helper Functions
# ============================================================================

def _parse_ine_timestamp(ts_ms: int) -> str:
    """Convert INE timestamp (milliseconds) to YYYY-MM string."""
    dt = datetime.fromtimestamp(ts_ms / 1000)
    return dt.strftime("%Y-%m")


def _parse_ine_date(ts_ms: int) -> datetime:
    """Convert INE timestamp (milliseconds) to datetime."""
    return datetime.fromtimestamp(ts_ms / 1000)


def _fetch_ine_table(table_id: str, nult: int = 12) -> Optional[list]:
    """
    Fetch data from INE API.
    
    Args:
        table_id: INE table identifier
        nult: Number of last data points to retrieve
        
    Returns:
        List of series data or None on error
    """
    try:
        url = f"{INE_BASE_URL}/DATOS_TABLA/{table_id}?nult={nult}"
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"⚠️ Error fetching INE table {table_id}: {e}")
        return None


def _find_ine_series(data: list, filters: Dict[str, str]) -> Optional[dict]:
    """
    Find a specific series in INE data by matching name filters.
    
    Args:
        data: List of INE series
        filters: Dict of substrings that must ALL be present in the series name
        
    Returns:
        Matching series or None
    """
    if not data:
        return None
    
    for entry in data:
        name = entry.get("Nombre", "")
        if all(f in name for f in filters.values()):
            return entry
    return None


def _extract_ine_timeseries(series: dict) -> List[Dict]:
    """
    Extract time series data from an INE series entry.
    
    Returns:
        List of dicts with 'date', 'date_str', 'value'
    """
    if not series or "Data" not in series:
        return []
    
    result = []
    for d in series["Data"]:
        if d.get("Valor") is not None:
            dt = _parse_ine_date(d["Fecha"])
            result.append({
                "date": dt,
                "date_str": dt.strftime("%Y-%m"),
                "value": d["Valor"]
            })
    
    # Sort by date
    result.sort(key=lambda x: x["date"])
    return result


# ============================================================================
# Euribor (ECB)
# ============================================================================

def get_euribor_data() -> Dict:
    """
    Fetch Euribor 12M data from ECB.
    
    Returns:
        Dict with keys:
        - 'current': latest value
        - 'previous': previous month value
        - 'change': change from previous
        - 'trend': 'up', 'down', or 'stable'
        - 'series': list of {'date_str', 'value'} dicts
        - 'source': data source description
        - 'error': error message if any
    """
    result = {
        "name": "Euríbor 12M",
        "unit": "%",
        "source": "Banco Central Europeo (BCE)",
        "frequency": "Mensual",
        "series": [],
        "current": None,
        "previous": None,
        "change": None,
        "trend": "stable",
        "error": None
    }
    
    try:
        resp = requests.get(
            ECB_EURIBOR_URL,
            timeout=REQUEST_TIMEOUT,
            headers={"Accept": "application/json"}
        )
        resp.raise_for_status()
        data = resp.json()
        
        datasets = data["dataSets"][0]
        series_data = list(datasets["series"].values())[0]
        observations = series_data["observations"]
        time_periods = data["structure"]["dimensions"]["observation"][0]["values"]
        
        points = []
        for idx, obs in observations.items():
            period = time_periods[int(idx)]
            points.append({
                "date_str": period["id"],
                "value": round(obs[0], 3)
            })
        
        # Sort chronologically
        points.sort(key=lambda x: x["date_str"])
        result["series"] = points
        
        if len(points) >= 2:
            result["current"] = points[-1]["value"]
            result["previous"] = points[-2]["value"]
            result["change"] = round(result["current"] - result["previous"], 3)
            
            if result["change"] < -0.05:
                result["trend"] = "down"
            elif result["change"] > 0.05:
                result["trend"] = "up"
            else:
                result["trend"] = "stable"
        elif len(points) == 1:
            result["current"] = points[0]["value"]
            
    except Exception as e:
        result["error"] = str(e)
        # Fallback data
        result["current"] = 2.25
        result["trend"] = "down"
    
    return result


# ============================================================================
# IPC (INE)
# ============================================================================

def get_ipc_data() -> Dict:
    """
    Fetch IPC (Consumer Price Index) annual variation from INE.
    """
    result = {
        "name": "IPC (Variación Anual)",
        "unit": "%",
        "source": "INE - Índice de Precios de Consumo",
        "frequency": "Mensual",
        "series": [],
        "current": None,
        "previous": None,
        "change": None,
        "trend": "stable",
        "error": None
    }
    
    try:
        data = _fetch_ine_table(INE_TABLE_IPC, nult=12)
        series = _find_ine_series(data, {
            "scope": "Total Nacional",
            "type": "Índice general",
            "metric": "Variación anual"
        })
        
        points = _extract_ine_timeseries(series)
        result["series"] = [{"date_str": p["date_str"], "value": p["value"]} for p in points]
        
        if len(points) >= 2:
            result["current"] = points[-1]["value"]
            result["previous"] = points[-2]["value"]
            result["change"] = round(result["current"] - result["previous"], 2)
            
            if result["change"] < -0.2:
                result["trend"] = "down"
            elif result["change"] > 0.2:
                result["trend"] = "up"
            else:
                result["trend"] = "stable"
        elif len(points) == 1:
            result["current"] = points[0]["value"]
            
    except Exception as e:
        result["error"] = str(e)
        result["current"] = 2.9
    
    return result


# ============================================================================
# IPV - Índice de Precios de Vivienda (INE)
# ============================================================================

def get_ipv_data() -> Dict:
    """
    Fetch Housing Price Index for Madrid from INE.
    Returns both the index value and the annual variation.
    """
    result = {
        "name": "Índice Precios Vivienda Madrid",
        "unit": "%",
        "source": "INE - Índice de Precios de Vivienda (IPV)",
        "frequency": "Trimestral",
        "series": [],
        "series_index": [],
        "current": None,
        "current_index": None,
        "previous": None,
        "change": None,
        "trend": "stable",
        "error": None
    }
    
    try:
        data = _fetch_ine_table(INE_TABLE_IPV, nult=8)
        
        # Annual variation for Madrid
        series_var = _find_ine_series(data, {
            "region": "Madrid, Comunidad de",
            "type": "General",
            "metric": "Variación anual"
        })
        
        points_var = _extract_ine_timeseries(series_var)
        result["series"] = [{"date_str": p["date_str"], "value": p["value"]} for p in points_var]
        
        if len(points_var) >= 2:
            result["current"] = points_var[-1]["value"]
            result["previous"] = points_var[-2]["value"]
            result["change"] = round(result["current"] - result["previous"], 2)
        elif len(points_var) == 1:
            result["current"] = points_var[0]["value"]
        
        # Index value for Madrid
        series_idx = _find_ine_series(data, {
            "region": "Madrid, Comunidad de",
            "type": "General",
            "metric": "Índice"
        })
        
        # Ensure we don't match "Variación" entries
        if series_idx:
            name = series_idx.get("Nombre", "")
            if "Variación" in name:
                # Try again filtering more specifically
                for entry in data:
                    n = entry.get("Nombre", "")
                    if ("Madrid" in n and "General" in n and 
                        "Índice" in n and "Variación" not in n):
                        series_idx = entry
                        break
        
        points_idx = _extract_ine_timeseries(series_idx)
        result["series_index"] = [{"date_str": p["date_str"], "value": p["value"]} for p in points_idx]
        
        if points_idx:
            result["current_index"] = points_idx[-1]["value"]
        
        # Trend based on annual variation
        if result["current"] is not None:
            if result["current"] > 5:
                result["trend"] = "up"
            elif result["current"] < 0:
                result["trend"] = "down"
            else:
                result["trend"] = "stable"
                
    except Exception as e:
        result["error"] = str(e)
        result["current"] = 13.3
        result["trend"] = "up"
    
    return result


# ============================================================================
# Compraventas de Vivienda (INE)
# ============================================================================

def get_compraventas_data() -> Dict:
    """
    Fetch housing transactions for Madrid from INE.
    """
    result = {
        "name": "Compraventas Vivienda Madrid",
        "unit": "unidades",
        "source": "INE - Estadística de Transmisiones de Derechos de la Propiedad",
        "frequency": "Mensual",
        "series": [],
        "current": None,
        "previous": None,
        "change": None,
        "change_pct": None,
        "trend": "stable",
        "error": None
    }
    
    try:
        data = _fetch_ine_table(INE_TABLE_COMPRAVENTAS, nult=12)
        series = _find_ine_series(data, {
            "region": "Madrid, Comunidad de",
            "type": "General",
            "metric": "Compraventa",
            "measure": "Número"
        })
        
        points = _extract_ine_timeseries(series)
        result["series"] = [{"date_str": p["date_str"], "value": int(p["value"])} for p in points]
        
        if len(points) >= 2:
            result["current"] = int(points[-1]["value"])
            result["previous"] = int(points[-2]["value"])
            result["change"] = result["current"] - result["previous"]
            result["change_pct"] = round(
                (result["change"] / result["previous"] * 100) if result["previous"] > 0 else 0, 1
            )
            
            if result["change_pct"] < -10:
                result["trend"] = "down"
            elif result["change_pct"] > 10:
                result["trend"] = "up"
            else:
                result["trend"] = "stable"
        elif len(points) == 1:
            result["current"] = int(points[0]["value"])
            
    except Exception as e:
        result["error"] = str(e)
        result["current"] = 6000
    
    return result


# ============================================================================
# Tasa de Paro EPA (INE)
# ============================================================================

def get_paro_data() -> Dict:
    """
    Fetch unemployment rate for Madrid from INE (EPA).
    Uses national data as primary, Madrid-specific when available.
    """
    result = {
        "name": "Tasa de Paro",
        "unit": "%",
        "source": "INE - Encuesta de Población Activa (EPA)",
        "frequency": "Trimestral",
        "series": [],
        "current": None,
        "previous": None,
        "change": None,
        "trend": "stable",
        "error": None,
        "scope": "Total Nacional"
    }
    
    try:
        data = _fetch_ine_table(INE_TABLE_PARO, nult=8)
        
        # Try Madrid first
        series = _find_ine_series(data, {
            "sex": "Ambos sexos",
            "region": "Madrid, Comunidad de",
            "age": "Todas las edades"
        })
        
        if series:
            result["scope"] = "Comunidad de Madrid"
        else:
            # Fallback to national
            series = _find_ine_series(data, {
                "sex": "Ambos sexos",
                "region": "Total Nacional",
                "age": "Todas las edades"
            })
        
        points = _extract_ine_timeseries(series)
        result["series"] = [{"date_str": p["date_str"], "value": p["value"]} for p in points]
        
        if len(points) >= 2:
            result["current"] = points[-1]["value"]
            result["previous"] = points[-2]["value"]
            result["change"] = round(result["current"] - result["previous"], 2)
            
            if result["change"] < -0.3:
                result["trend"] = "down"
            elif result["change"] > 0.3:
                result["trend"] = "up"
            else:
                result["trend"] = "stable"
        elif len(points) == 1:
            result["current"] = points[0]["value"]
            
    except Exception as e:
        result["error"] = str(e)
        result["current"] = 10.5
    
    return result


# ============================================================================
# Hipotecas Constituidas (INE)
# ============================================================================

def get_hipotecas_data() -> Dict:
    """
    Fetch mortgage data for Madrid from INE.
    """
    result = {
        "name": "Hipotecas Vivienda Madrid",
        "unit": "unidades",
        "source": "INE - Estadística de Hipotecas",
        "frequency": "Mensual",
        "series": [],
        "current": None,
        "previous": None,
        "change": None,
        "change_pct": None,
        "trend": "stable",
        "error": None
    }
    
    try:
        data = _fetch_ine_table(INE_TABLE_HIPOTECAS, nult=12)
        
        # Try Viviendas Madrid specifically
        series = _find_ine_series(data, {
            "type": "Viviendas",
            "region": "Madrid"
        })
        
        if not series:
            # Fallback to Total fincas Madrid
            series = _find_ine_series(data, {
                "type": "Total fincas",
                "region": "Madrid"
            })
        
        points = _extract_ine_timeseries(series)
        result["series"] = [{"date_str": p["date_str"], "value": int(p["value"])} for p in points]
        
        if len(points) >= 2:
            result["current"] = int(points[-1]["value"])
            result["previous"] = int(points[-2]["value"])
            result["change"] = result["current"] - result["previous"]
            result["change_pct"] = round(
                (result["change"] / result["previous"] * 100) if result["previous"] > 0 else 0, 1
            )
            
            if result["change_pct"] < -10:
                result["trend"] = "down"
            elif result["change_pct"] > 10:
                result["trend"] = "up"
            else:
                result["trend"] = "stable"
        elif len(points) == 1:
            result["current"] = int(points[0]["value"])
            
    except Exception as e:
        result["error"] = str(e)
        result["current"] = 1600
    
    return result


# ============================================================================
# Aggregated fetch
# ============================================================================

def get_all_macro_data() -> Dict[str, Dict]:
    """
    Fetch all macro indicators at once.
    
    Returns:
        Dict with keys: euribor, ipc, ipv, compraventas, paro, hipotecas
    """
    return {
        "euribor": get_euribor_data(),
        "ipc": get_ipc_data(),
        "ipv": get_ipv_data(),
        "compraventas": get_compraventas_data(),
        "paro": get_paro_data(),
        "hipotecas": get_hipotecas_data()
    }


# ============================================================================
# Test
# ============================================================================

if __name__ == "__main__":
    print("🏛️ Testing macro data fetching...\n")
    
    all_data = get_all_macro_data()
    
    for key, data in all_data.items():
        status = "✅" if not data.get("error") else "⚠️"
        current = data.get("current", "N/A")
        unit = data.get("unit", "")
        trend_emoji = {"up": "📈", "down": "📉", "stable": "➡️"}.get(data.get("trend", ""), "❓")
        series_len = len(data.get("series", []))
        
        print(f"{status} {data['name']:35} | {current} {unit:>8} {trend_emoji} | {series_len} puntos | {data['source']}")
        
        if data.get("error"):
            print(f"   ⚠️ Error: {data['error']}")
        if data.get("series"):
            last = data["series"][-1]
            print(f"   Último dato: {last['date_str']} = {last['value']} {unit}")
