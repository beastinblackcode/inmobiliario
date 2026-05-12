"""
Offer Engine — suggest a concrete bid range for a listing.

The dashboard already has:
  * ``estimate_fair_price()`` — what the property is *worth* given
    comparables, notarial anchor and district trend.
  * ``quality_score`` / ``negotiability_score`` — how attractive or
    pliable the listing is.

What was missing: a single buyer-actionable output that fuses those
into "offer between €X and €Y".  That's what this module does.

Mental model
------------

``fair_value`` (from ``estimate_fair_price``) is the market-clearing
price for an equivalent property today.  ``asking_price`` is what the
seller wants.  Our suggested offer sits **at or below** ``fair_value``,
discounted by the buyer-leverage signals the listing already exposes:

  * Days on market — the longer it sits, the more the seller hurts.
  * Price drops already applied — the seller has admitted the original
    price was wrong; another haircut is plausible.
  * Republication count — same property re-published after delisting
    is a strong fatigue signal.  Falsely-positive parallel listings
    (multi-agency, simultaneous) only nudge the discount slightly,
    so a bad cluster doesn't blow up the suggestion.
  * Private seller — anecdotally accepts ~2-3% more discount than
    agencies (no agency commission to defend).
  * NLP signals — explicit "negociable" / "venta directa" /
    "necesita reforma" in the description.

The function returns the breakdown of factors so the UI can render
*why* a suggested offer is what it is, not just the number.

Notes on limits
---------------
* Total discount is capped at ``MAX_DISCOUNT_PCT`` (default 25%) so
  data anomalies can't produce a lowball suggestion the seller would
  laugh at.
* The suggested mid never exceeds ``fair_value`` — if every factor
  is neutral, you still offer at fair value, not above.
* The suggested range is ``mid × [1 − band, 1 + band]`` with
  ``band`` shrinking on a high-confidence fair value (more comps =
  tighter range).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd


# ──────────────────────────────────────────────────────────────────────
# Tunables
# ──────────────────────────────────────────────────────────────────────

MAX_DISCOUNT_PCT = 25.0          # absolute floor: never suggest below fair × 0.75
MIN_DISCOUNT_PCT = 0.0           # never suggest above fair_value

# Per-factor caps so any single noisy signal can't dominate the result.
CAP_DAYS_ON_MARKET   =  6.0
CAP_REPUBLICATIONS   =  4.0
CAP_DROPS            =  6.0
CAP_SELLER_TYPE      =  3.0
CAP_NLP_SIGNALS      =  4.0
CAP_OVERPRICED       =  6.0


# ──────────────────────────────────────────────────────────────────────
# Data types
# ──────────────────────────────────────────────────────────────────────


@dataclass
class OfferFactor:
    """One contributing reason behind the suggested offer."""
    label:            str        # short tag, e.g. "Días en mercado"
    discount_pct:     float      # negative number, the discount this factor adds
    why:              str        # human-readable explanation


@dataclass
class OfferSuggestion:
    """Output of ``suggest_offer``."""
    asking_price:        int
    fair_value:          int
    fair_value_method:   str                # "barrio_comps" | "distrito_comps" | "barrio_median" | "notarial"
    fair_confidence:     str                # "high" | "medium" | "low"
    suggested_low:       int
    suggested_mid:       int
    suggested_high:      int
    total_discount_pct:  float              # sum of factor discounts, capped
    discount_vs_asking_pct: float           # (asking − suggested_mid) / asking × 100
    factors:             list[OfferFactor] = field(default_factory=list)

    @property
    def is_above_fair_value(self) -> bool:
        """True when the listing is asking above the fair market price.

        The single best heuristic for "this is overpriced and there's
        plenty of room to negotiate down".
        """
        return self.asking_price > self.fair_value


# ──────────────────────────────────────────────────────────────────────
# Factor functions — each returns ``OfferFactor`` or ``None``.
# ──────────────────────────────────────────────────────────────────────


def _factor_days_on_market(listing: dict) -> Optional[OfferFactor]:
    """Long-on-market = seller hurts more.

    The function is piecewise-linear with a 30-day grace period:
      *  0-30 d → 0 %     (just listed, no leverage yet)
      * 30-90 d → -0.5 % per 30 d above 30 d
      * 90-180 d → -1.5 % per 30 d above 90 d
      * 180+ d → -1 % per 30 d above 180 d  (diminishing returns —
                                              by then the listing is
                                              already stale and the
                                              other factors capture it)

    Capped at ``CAP_DAYS_ON_MARKET`` so a stuck listing from 2 years
    ago doesn't single-handedly produce a -15 % suggestion.
    """
    dom = int(listing.get("days_on_market") or 0)
    if dom <= 30:
        return None
    if dom <= 90:
        disc = -0.5 * ((dom - 30) / 30)
    elif dom <= 180:
        disc = -1.0 + -1.5 * ((dom - 90) / 30)
    else:
        disc = -1.0 + -4.5 + -1.0 * ((dom - 180) / 30)
    disc = max(disc, -CAP_DAYS_ON_MARKET)
    return OfferFactor(
        label        = "Días en mercado",
        discount_pct = round(disc, 2),
        why          = f"{dom} días publicado — el vendedor lleva tiempo sin cerrar la venta.",
    )


def _factor_price_drops(listing: dict) -> Optional[OfferFactor]:
    """Already-applied price drops: precedent.

    Two contributions: count of drops (was the seller resistant or
    willing?) and total %-dropped (how much have they conceded so far).
    """
    n_drops = int(listing.get("num_drops") or 0)
    total_drop = abs(float(listing.get("total_drop_pct") or 0))
    if n_drops == 0 and total_drop == 0:
        return None

    # 1.5% discount per drop event, plus a magnitude term.
    discount = -1.5 * n_drops - 0.3 * total_drop
    discount = max(discount, -CAP_DROPS)
    return OfferFactor(
        label        = "Historial de bajadas",
        discount_pct = round(discount, 2),
        why          = (
            f"{n_drops} bajada{'s' if n_drops != 1 else ''} previa{'s' if n_drops != 1 else ''} "
            f"({total_drop:.1f}% acumulado) — la disposición a bajar ya está demostrada."
        ),
    )


def _factor_seller_type(listing: dict) -> Optional[OfferFactor]:
    """Private sellers tend to accept slightly deeper offers than agencies."""
    if listing.get("seller_type") == "Particular":
        return OfferFactor(
            label        = "Vendedor particular",
            discount_pct = -CAP_SELLER_TYPE,
            why          = "Sin agencia que defienda su comisión — margen estructural de ~3%.",
        )
    return None


def _factor_republications(property_history: Any) -> Optional[OfferFactor]:
    """Republished property = fatigue signal.

    Note: the current fingerprint matcher produces some false
    positives (multi-agency parallel listings, obra nueva).  We
    discount this factor accordingly — at most ``CAP_REPUBLICATIONS``
    even for 5+ listings, so a misclustered case can't blow up the
    suggestion.  Once the matcher is refined (separate temporal vs
    parallel republications) we can push the weight up.
    """
    if property_history is None:
        return None
    reps = getattr(property_history, "republication_count", 0)
    if reps <= 0:
        return None
    # Each republication adds ~1.5%, capped.
    discount = max(-1.5 * reps, -CAP_REPUBLICATIONS)
    return OfferFactor(
        label        = "Republicaciones",
        discount_pct = round(discount, 2),
        why          = (
            f"Esta propiedad ha aparecido en {reps + 1} anuncios distintos. "
            "Probable fatiga del vendedor (con la salvedad de que el matcher "
            "actual no distingue 100% republicación temporal vs multi-agencia)."
        ),
    )


def _factor_nlp_signals(amenities: Optional[dict]) -> Optional[OfferFactor]:
    """Explicit NLP signals: 'negociable', 'urgencia', 'necesita reforma'.

    *amenities* here is actually the listing_signals dict produced by
    ``nlp_analyzer.get_signals_for_listings`` (lives next to the
    amenities table but has different keys: urgency, negotiable,
    direct, needs_work, renovated, signal_count).  The detail page
    already loads it; we just consume.
    """
    if not amenities:
        return None

    bits: list[str] = []
    disc = 0.0
    if amenities.get("negotiable"):
        bits.append("negociable")
        disc -= 2.0
    if amenities.get("urgency"):
        bits.append("urgencia")
        disc -= 1.5
    if amenities.get("direct"):
        bits.append("venta directa")
        disc -= 0.5
    if amenities.get("needs_work"):
        bits.append("a reformar")
        disc -= 2.0

    if not bits:
        return None

    disc = max(disc, -CAP_NLP_SIGNALS)
    return OfferFactor(
        label        = "Señales en la descripción",
        discount_pct = round(disc, 2),
        why          = "Detectado: " + ", ".join(bits) + ".",
    )


def _factor_overpriced(asking_price: int, fair_value: int) -> Optional[OfferFactor]:
    """When asking > fair_value the property is overpriced relative to comps.

    Distinct from the buyer-leverage factors: this one *isn't*
    discount from fair_value — it's a flag that fair_value already
    sits below asking_price, so the practical "discount from asking"
    will be larger.  We still add a small discount on top to push
    the offer further below fair when the gap is wide (the seller
    will negotiate around their own asking, not yours).
    """
    if fair_value <= 0 or asking_price <= fair_value:
        return None
    gap_pct = (asking_price - fair_value) * 100.0 / fair_value
    if gap_pct < 5:                    # tolerance for fair-value noise
        return None
    # Stronger pull below fair when the overprice gap is big.
    disc = max(-(gap_pct - 5) * 0.25, -CAP_OVERPRICED)
    return OfferFactor(
        label        = "Sobreprecio vs comparables",
        discount_pct = round(disc, 2),
        why          = (
            f"Precio pedido {gap_pct:.1f}% por encima del fair value calculado. "
            "Cuanto más se aleja, mayor el margen para ofertar por debajo."
        ),
    )


# ──────────────────────────────────────────────────────────────────────
# Range derivation
# ──────────────────────────────────────────────────────────────────────


def _range_band_for_confidence(confidence: str) -> float:
    """How wide the suggested ``[low, high]`` band is around the midpoint.

    High-confidence fair_value (lots of comps) → tighter range
    (±2.5 %). Low-confidence → ±5 %, so the buyer knows the
    suggestion is fuzzier.
    """
    return {"high": 0.025, "medium": 0.035}.get(confidence, 0.05)


# ──────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────


def suggest_offer(
    listing:           dict,
    fair_value:        int,
    *,
    fair_confidence:   str       = "medium",
    fair_value_method: str       = "barrio_comps",
    property_history:  Any       = None,
    nlp_signals:       Optional[dict] = None,
) -> OfferSuggestion:
    """Build a complete offer suggestion for a listing.

    Parameters
    ----------
    listing
        Listing dict containing at least ``price``, ``days_on_market``,
        ``num_drops``, ``total_drop_pct`` and ``seller_type``. Missing
        fields are tolerated and treated as neutral.

    fair_value
        Output of ``estimate_fair_price`` — what the property is worth
        before applying buyer-leverage discounts.

    fair_confidence
        Passed through from ``estimate_fair_price``.  Controls the
        tightness of the ``[low, high]`` band.

    property_history
        Optional ``PropertyHistory`` (from ``property_history.py``).
        When present and ``republication_count > 0``, contributes a
        small additional discount.

    nlp_signals
        Optional dict from ``nlp_analyzer.get_signals_for_listings``
        with boolean flags for negotiable / urgency / direct / needs_work.
    """
    asking_price = int(listing.get("price") or 0)

    factors: list[OfferFactor] = []
    for fn, arg in (
        (_factor_days_on_market,  listing),
        (_factor_price_drops,     listing),
        (_factor_seller_type,     listing),
        (_factor_republications,  property_history),
        (_factor_nlp_signals,     nlp_signals),
        (_factor_overpriced,      None),         # special-case below
    ):
        if fn is _factor_overpriced:
            f = _factor_overpriced(asking_price, fair_value)
        else:
            f = fn(arg)
        if f is not None:
            factors.append(f)

    # Total discount, signed (negative numbers), summed and clamped.
    total_discount = sum(f.discount_pct for f in factors)
    total_discount = max(min(total_discount, MIN_DISCOUNT_PCT), -MAX_DISCOUNT_PCT)

    # Apply the discount on top of ``min(fair, asking)`` — never suggest
    # paying more than the seller is already willing to accept.  Two
    # regimes:
    #   * asking ≥ fair_value (the common case, overpriced or near-fair):
    #     suggested mid = fair × (1 + discount).  The buyer is anchoring
    #     to the market, not to the seller's wishlist.
    #   * asking < fair_value (a chollo / mispriced listing): suggested
    #     mid = asking × (1 + discount).  Even on bargains the leverage
    #     signals (DOM, drops, particular vendor) still warrant *some*
    #     room below asking; we just don't pretend the buyer would
    #     volunteer to pay above the seller's price.
    base_for_discount = min(fair_value, asking_price) if asking_price > 0 else fair_value
    suggested_mid  = int(round(base_for_discount * (1 + total_discount / 100.0)))
    band           = _range_band_for_confidence(fair_confidence)
    # Range is clamped to never exceed the base — neither suggesting
    # above-asking nor above-fair is ever appropriate.
    suggested_low  = int(round(suggested_mid * (1 - band)))
    suggested_high = int(round(min(suggested_mid * (1 + band), base_for_discount)))

    discount_vs_asking = (
        (asking_price - suggested_mid) * 100.0 / asking_price
        if asking_price > 0 else 0.0
    )

    return OfferSuggestion(
        asking_price            = asking_price,
        fair_value              = fair_value,
        fair_value_method       = fair_value_method,
        fair_confidence         = fair_confidence,
        suggested_low           = suggested_low,
        suggested_mid           = suggested_mid,
        suggested_high          = suggested_high,
        total_discount_pct      = round(total_discount, 2),
        discount_vs_asking_pct  = round(discount_vs_asking, 2),
        factors                 = factors,
    )
