"""
FastAPI server — exposes the PricePredictor model as a REST API.

Run:  uvicorn api_server:app --host 0.0.0.0 --port 8000

Endpoints:
  POST /api/valorar   → property valuation
  GET  /api/health     → health check
"""

from __future__ import annotations

import os
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from predictive_model import PricePredictor

# ── App ───────────────────────────────────────────────────────
app = FastAPI(
    title="madridhome.tech Valuation API",
    version="1.0.0",
    docs_url="/api/docs",
)

# CORS — allow the frontend origin(s)
ALLOWED_ORIGINS = os.getenv(
    "CORS_ORIGINS",
    "https://madridhome.tech,http://localhost:3000",
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["POST", "GET", "OPTIONS"],
    allow_headers=["*"],
)

# Singleton predictor (auto-trains on first call if needed)
predictor = PricePredictor()


# ── Pydantic schemas ─────────────────────────────────────────
class PropertyInput(BaseModel):
    distrito: str = Field(..., description="Distrito de Madrid (ej: 'Centro')")
    barrio: str = Field(..., description="Barrio (ej: 'Sol')")
    size_sqm: float = Field(..., ge=10, le=1000, description="Superficie en m²")
    rooms: int = Field(..., ge=0, le=15, description="Número de habitaciones")
    floor: str = Field(
        "1ª Planta",
        description="Altura: Bajo, Entreplanta, 1ª-7ª Planta, Ático",
    )
    has_elevator: bool = Field(True, description="¿Tiene ascensor?")
    is_exterior: bool = Field(True, description="¿Es exterior?")
    # ── Extra features (not in the RF model — applied as post-adjustments)
    has_terrace: bool = Field(False, description="¿Tiene terraza?")
    has_garage: bool = Field(False, description="¿Tiene garaje?")
    condition: str = Field(
        "sin_reformar",
        description="Estado: sin_reformar | reformado | obra_nueva",
    )
    energy_cert: Optional[str] = Field(
        None, description="Certificado energético: A-G o null"
    )


class AdjustmentDetail(BaseModel):
    label: str
    pct: float
    eur: int


class ValuationResponse(BaseModel):
    estimated_price: int
    lower_bound: int
    upper_bound: int
    price_per_sqm: int
    confidence_pct: float
    adjustments: List[AdjustmentDetail]
    base_price: int
    model_info: Dict


# ── Floor label → numeric mapping ────────────────────────────
FLOOR_MAP = {
    "Bajo": 0,
    "Entreplanta": 0.5,
    "1ª Planta": 1,
    "2ª Planta": 2,
    "3ª Planta": 3,
    "4ª Planta": 4,
    "5ª Planta": 5,
    "6ª Planta": 6,
    "7ª+ Planta": 7,
    "Ático": 10,
}

# ── Hedonic adjustments for extra features ────────────────────
# These percentages are applied AFTER the RF prediction.
# Based on typical Madrid market premiums (conservative estimates).
EXTRA_ADJUSTMENTS = {
    "has_terrace": {"label": "Terraza", "pct": 0.06},           # +6%
    "has_garage": {"label": "Garaje", "pct": 0.04},             # +4%
    "condition_reformado": {"label": "Reformado", "pct": 0.12}, # +12%
    "condition_obra_nueva": {"label": "Obra nueva", "pct": 0.18},  # +18%
    "energy_A": {"label": "Cert. energética A", "pct": 0.05},
    "energy_B": {"label": "Cert. energética B", "pct": 0.03},
    "energy_C": {"label": "Cert. energética C", "pct": 0.01},
    # D is neutral (0%), E-G are slight negatives
    "energy_E": {"label": "Cert. energética E", "pct": -0.02},
    "energy_F": {"label": "Cert. energética F", "pct": -0.04},
    "energy_G": {"label": "Cert. energética G", "pct": -0.06},
}


def _compute_extra_adjustments(
    prop: PropertyInput, base_price: float
) -> tuple[float, list[AdjustmentDetail]]:
    """
    Compute hedonic adjustments for features not in the RF model.
    Returns (total_adjustment_eur, details_list).
    """
    total = 0.0
    details: list[AdjustmentDetail] = []

    # Terrace
    if prop.has_terrace:
        adj = EXTRA_ADJUSTMENTS["has_terrace"]
        eur = int(base_price * adj["pct"])
        total += eur
        details.append(AdjustmentDetail(label=adj["label"], pct=adj["pct"] * 100, eur=eur))

    # Garage
    if prop.has_garage:
        adj = EXTRA_ADJUSTMENTS["has_garage"]
        eur = int(base_price * adj["pct"])
        total += eur
        details.append(AdjustmentDetail(label=adj["label"], pct=adj["pct"] * 100, eur=eur))

    # Condition (only if not sin_reformar, which is the baseline)
    if prop.condition == "reformado":
        adj = EXTRA_ADJUSTMENTS["condition_reformado"]
        eur = int(base_price * adj["pct"])
        total += eur
        details.append(AdjustmentDetail(label=adj["label"], pct=adj["pct"] * 100, eur=eur))
    elif prop.condition == "obra_nueva":
        adj = EXTRA_ADJUSTMENTS["condition_obra_nueva"]
        eur = int(base_price * adj["pct"])
        total += eur
        details.append(AdjustmentDetail(label=adj["label"], pct=adj["pct"] * 100, eur=eur))

    # Energy certificate
    if prop.energy_cert:
        key = f"energy_{prop.energy_cert.upper()}"
        if key in EXTRA_ADJUSTMENTS:
            adj = EXTRA_ADJUSTMENTS[key]
            eur = int(base_price * adj["pct"])
            total += eur
            details.append(
                AdjustmentDetail(label=adj["label"], pct=adj["pct"] * 100, eur=eur)
            )

    return total, details


# ── Endpoints ─────────────────────────────────────────────────

@app.get("/api/health")
async def health():
    info = predictor.get_model_info()
    return {
        "status": "ok",
        "model_trained": info["is_trained"],
        "training_date": info.get("training_date"),
        "training_samples": info.get("training_samples", 0),
    }


@app.post("/api/valorar", response_model=ValuationResponse)
async def valorar(prop: PropertyInput):
    """Estimate property price based on RF model + hedonic adjustments."""

    # Map floor label to numeric
    floor_level = FLOOR_MAP.get(prop.floor, 1.0)

    # Build features dict for the RF model
    features = {
        "distrito": prop.distrito,
        "barrio": prop.barrio,
        "size_sqm": prop.size_sqm,
        "rooms": prop.rooms,
        "floor_level": floor_level,
        "has_lift": 1 if prop.has_elevator else 0,
        "is_exterior": 1 if prop.is_exterior else 0,
    }

    try:
        result = predictor.predict(features)
    except Exception as exc:
        raise HTTPException(
            status_code=422,
            detail=f"Error en la predicción: {str(exc)}",
        )

    base_price = result["estimated_price"]

    # Apply extra hedonic adjustments
    extra_eur, adjustments = _compute_extra_adjustments(prop, base_price)

    final_price = base_price + extra_eur

    # Scale confidence bands proportionally
    ratio = final_price / base_price if base_price > 0 else 1.0
    lower = int(result["lower_bound"] * ratio)
    upper = int(result["upper_bound"] * ratio)

    # Model info summary
    info = predictor.get_model_info()
    model_summary = {
        "r2": info["metrics"].get("r2"),
        "mae": info["metrics"].get("mae"),
        "mape": info["metrics"].get("mape"),
        "training_samples": info.get("training_samples", 0),
        "training_date": info.get("training_date"),
    }

    return ValuationResponse(
        estimated_price=int(final_price),
        lower_bound=lower,
        upper_bound=upper,
        price_per_sqm=int(final_price / prop.size_sqm) if prop.size_sqm > 0 else 0,
        confidence_pct=result["confidence_pct"],
        adjustments=adjustments,
        base_price=int(base_price),
        model_info=model_summary,
    )
