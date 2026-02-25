"""
Daily market report email module.
Sends an HTML summary to luis.nunno@gmail.com via Gmail SMTP after each scrape.

Setup (one-time):
  1. Enable 2-Step Verification on perroverdeviejo@gmail.com
  2. Go to: myaccount.google.com → Security → App Passwords
  3. Create an App Password for "Mail"
  4. Add to .env:  GMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx
"""

import os
import smtplib
import traceback
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
SENDER_EMAIL    = "perroverdeviejo@gmail.com"
RECIPIENT_EMAIL = "luis.nunno@gmail.com"
SMTP_HOST       = "smtp.gmail.com"
SMTP_PORT       = 587


# ── Colour palette (light email-safe theme) ───────────────────────────────────
SCORE_COLORS = {
    "Mercado Muy Activo":   "#1b7f3a",
    "Mercado Activo":       "#2e9e52",
    "Mercado Estable":      "#e69c1a",
    "Mercado Ralentizado":  "#d4631a",
    "Mercado en Corrección":"#c0392b",
}


# =============================================================================
# HTML BUILDER
# =============================================================================

def _score_color(score: Dict) -> str:
    label = score.get("label", "")
    for key, color in SCORE_COLORS.items():
        if key in label:
            return color
    return "#555555"


def _kpi_row(label: str, value: str, note: str = "") -> str:
    note_html = f"<br><span style='font-size:11px;color:#888;'>{note}</span>" if note else ""
    return f"""
        <tr>
          <td style='padding:8px 12px;border-bottom:1px solid #eee;color:#555;font-size:13px;'>{label}</td>
          <td style='padding:8px 12px;border-bottom:1px solid #eee;font-weight:600;font-size:14px;text-align:right;'>{value}{note_html}</td>
        </tr>"""


def _chol_row(idx: int, p: Dict) -> str:
    change_pct = p.get("total_drop_pct", 0)
    urgency    = p.get("urgency_score", 0)
    price_now  = p.get("current_price", 0)
    price_orig = p.get("initial_price", 0)
    url        = p.get("url", "#")
    barrio     = p.get("barrio", "")
    distrito   = p.get("distrito", "")
    rooms      = p.get("rooms") or "—"
    sqm        = p.get("size_sqm") or "—"
    drops      = p.get("num_drops", 0)

    urgency_color = "#c0392b" if urgency >= 60 else ("#e69c1a" if urgency >= 30 else "#2e9e52")
    sqm_str = f"{sqm:.0f} m²" if isinstance(sqm, float) else str(sqm)

    return f"""
        <tr style='{"background:#fffbf0;" if idx % 2 == 0 else ""}'>
          <td style='padding:10px 12px;font-size:13px;'>
            <a href='{url}' style='color:#1a73e8;font-weight:600;text-decoration:none;'>#{idx} — {barrio}, {distrito}</a><br>
            <span style='color:#888;font-size:11px;'>{rooms} hab · {sqm_str} · {drops} bajadas</span>
          </td>
          <td style='padding:10px 12px;text-align:right;font-size:13px;'>
            <span style='font-weight:700;'>€{price_now:,}</span><br>
            <span style='color:#888;font-size:11px;text-decoration:line-through;'>€{price_orig:,}</span>
          </td>
          <td style='padding:10px 12px;text-align:right;font-size:13px;color:#c0392b;font-weight:700;'>
            {change_pct:.1f}%
          </td>
          <td style='padding:10px 12px;text-align:center;'>
            <span style='background:{urgency_color};color:white;border-radius:12px;padding:3px 10px;font-size:11px;font-weight:700;'>
              {urgency}
            </span>
          </td>
        </tr>"""


def _yield_row(idx: int, r: Dict) -> str:
    bg = "#f0fff4" if idx % 2 == 0 else ""
    return f"""
        <tr style='{"background:" + bg + ";" if bg else ""}'>
          <td style='padding:8px 12px;font-size:13px;font-weight:600;'>{idx}. {r["barrio"]}</td>
          <td style='padding:8px 12px;font-size:12px;color:#666;'>{r["distrito"]}</td>
          <td style='padding:8px 12px;text-align:right;font-size:13px;color:#1b7f3a;font-weight:700;'>{r["yield_pct"]:.2f}%</td>
          <td style='padding:8px 12px;text-align:right;font-size:12px;'>€{int(r["median_rent"]):,}/mes</td>
          <td style='padding:8px 12px;text-align:right;font-size:12px;'>€{int(r["median_sale_price"]):,}</td>
        </tr>"""


def build_html_report(
    score: Dict,
    indicators: Dict,
    macro: Dict,
    chollos: List[Dict],
    yields: List[Dict],
    alerts: List[Dict],
) -> str:
    today     = datetime.now().strftime("%d/%m/%Y")
    score_val = score.get("score", 0)
    score_lbl = score.get("label", "—")
    score_col = _score_color(score)
    score_emoji = score.get("emoji", "📊")

    # ── KPI section ──────────────────────────────────────────────────────────
    price   = indicators.get("price_trend", {})
    speed   = indicators.get("sales_speed", {})
    inv     = indicators.get("inventory", {})
    afford  = indicators.get("affordability", {})
    pdr     = indicators.get("price_drop_ratio", {})
    ry      = indicators.get("rental_yield", {})
    euribor = macro.get("euribor", {})
    paro    = macro.get("paro", {})

    kpi_rows = ""
    if price.get("current"):
        kpi_rows += _kpi_row("💰 Precio mediano", f"€{price['current']:,.0f}",
                             f"{price.get('change_pct', 0):+.1f}% semanal")
    if price.get("current_sqm"):
        kpi_rows += _kpi_row("📏 €/m² mediano", f"€{price['current_sqm']:,.0f}")
    if speed.get("current") is not None:
        kpi_rows += _kpi_row("⏱️ Velocidad venta", f"{speed['current']:.0f} días en mercado")
    if inv.get("current"):
        kpi_rows += _kpi_row("🏠 Inventario activo", f"{inv['current']:,} propiedades",
                             f"{inv.get('change_pct', 0):+.1f}% semanal")
    if pdr.get("current") is not None:
        kpi_rows += _kpi_row("📉 Estrés vendedor",
                             f"{pdr['current']:.1f}% con bajadas",
                             f"{pdr.get('listings_with_drop', 0):,} propiedades")
    if afford.get("current") and afford.get("reference_income_monthly"):
        pmt_ratio = afford["current"] / afford["reference_income_monthly"] * 100
        kpi_rows += _kpi_row("📊 Esfuerzo hipotecario", f"{pmt_ratio:.0f}% ingresos",
                             f"Cuota: €{afford['current']:,}/mes")
    if ry.get("current") is not None:
        kpi_rows += _kpi_row("🏘️ Rentabilidad bruta media",
                             f"{ry['current']:.1f}%",
                             f"Sobre {ry.get('barrio_count', 0)} barrios")
    if euribor.get("current") is not None:
        kpi_rows += _kpi_row("🏦 Euríbor 12M",
                             f"{euribor['current']:.2f}%",
                             f"{euribor.get('change', 0):+.3f} pp")
    if paro.get("current") is not None:
        kpi_rows += _kpi_row("👥 Paro España", f"{paro['current']:.1f}%")

    # ── Alerts section ───────────────────────────────────────────────────────
    alerts_html = ""
    if alerts:
        alert_items = "".join(
            f"<li style='margin:6px 0;color:#555;font-size:13px;'>"
            f"<strong>{a.get('emoji','⚠️')} {a.get('title','')}</strong>: {a.get('message','')}</li>"
            for a in alerts[:5]
        )
        alerts_html = f"""
        <div style='background:#fff8e1;border-left:4px solid #f9a825;border-radius:6px;padding:16px 20px;margin:24px 0;'>
          <p style='margin:0 0 10px;font-weight:700;font-size:14px;color:#b47d00;'>⚠️ Alertas de Mercado</p>
          <ul style='margin:0;padding-left:18px;'>{alert_items}</ul>
        </div>"""

    # ── Chollos section ──────────────────────────────────────────────────────
    chollos_rows = "".join(_chol_row(i + 1, c) for i, c in enumerate(chollos[:10]))
    chollos_html = f"""
        <h2 style='color:#c0392b;font-size:17px;margin:32px 0 12px;'>
          🔥 Mejores Chollos — Propiedades con Múltiples Bajadas
        </h2>
        <p style='color:#777;font-size:12px;margin:0 0 12px;'>
          Propiedades activas con ≥2 bajadas de precio. Puntuación de urgencia = |bajada%| × nº bajadas.
        </p>
        <table style='width:100%;border-collapse:collapse;font-family:Arial,sans-serif;'>
          <thead>
            <tr style='background:#f5f5f5;'>
              <th style='padding:10px 12px;text-align:left;font-size:12px;color:#888;'>Propiedad</th>
              <th style='padding:10px 12px;text-align:right;font-size:12px;color:#888;'>Precio actual</th>
              <th style='padding:10px 12px;text-align:right;font-size:12px;color:#888;'>Bajada total</th>
              <th style='padding:10px 12px;text-align:center;font-size:12px;color:#888;'>Urgencia</th>
            </tr>
          </thead>
          <tbody>{chollos_rows}</tbody>
        </table>""" if chollos else """
        <p style='color:#aaa;font-style:italic;font-size:13px;margin:24px 0;'>
          Sin chollos destacados hoy (se necesitan ≥2 bajadas de precio por propiedad).
        </p>"""

    # ── Yields section ───────────────────────────────────────────────────────
    yield_rows = "".join(_yield_row(i + 1, r) for i, r in enumerate(yields[:10]))
    yields_html = f"""
        <h2 style='color:#1b7f3a;font-size:17px;margin:32px 0 12px;'>
          🏘️ Top 10 Barrios por Rentabilidad Bruta
        </h2>
        <table style='width:100%;border-collapse:collapse;font-family:Arial,sans-serif;'>
          <thead>
            <tr style='background:#f5f5f5;'>
              <th style='padding:8px 12px;text-align:left;font-size:12px;color:#888;'>Barrio</th>
              <th style='padding:8px 12px;text-align:left;font-size:12px;color:#888;'>Distrito</th>
              <th style='padding:8px 12px;text-align:right;font-size:12px;color:#888;'>Rentabilidad</th>
              <th style='padding:8px 12px;text-align:right;font-size:12px;color:#888;'>Alquiler med.</th>
              <th style='padding:8px 12px;text-align:right;font-size:12px;color:#888;'>Venta med.</th>
            </tr>
          </thead>
          <tbody>{yield_rows}</tbody>
        </table>""" if yields else """
        <p style='color:#aaa;font-style:italic;font-size:13px;'>
          Sin datos de rentabilidad todavía — se poblarán tras el primer scrape completo.
        </p>"""

    # ── Full HTML ────────────────────────────────────────────────────────────
    return f"""<!DOCTYPE html>
<html lang="es">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Informe Mercado Inmobiliario Madrid — {today}</title></head>
<body style='margin:0;padding:0;background:#f4f4f4;font-family:Arial,Helvetica,sans-serif;'>

<table width='100%' cellpadding='0' cellspacing='0' style='background:#f4f4f4;padding:24px 0;'>
<tr><td align='center'>
<table width='620' cellpadding='0' cellspacing='0' style='background:#ffffff;border-radius:10px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);'>

  <!-- HEADER -->
  <tr><td style='background:#1a1a2e;padding:28px 32px;text-align:center;'>
    <p style='margin:0;color:#8ab4f8;font-size:12px;letter-spacing:2px;text-transform:uppercase;'>Inmobiliario Madrid</p>
    <h1 style='margin:8px 0 4px;color:#ffffff;font-size:22px;'>Informe Diario de Mercado</h1>
    <p style='margin:0;color:#aaa;font-size:13px;'>{today}</p>
  </td></tr>

  <!-- SCORE BADGE -->
  <tr><td style='padding:28px 32px 16px;text-align:center;'>
    <div style='display:inline-block;background:{score_col};border-radius:50%;width:90px;height:90px;line-height:90px;text-align:center;'>
      <span style='font-size:36px;color:white;font-weight:900;'>{score_val}</span>
    </div>
    <p style='margin:12px 0 4px;font-size:20px;font-weight:700;color:{score_col};'>{score_emoji} {score_lbl}</p>
    <p style='margin:0;font-size:12px;color:#999;'>Score sobre 100 — mayor = mercado más activo/saludable</p>
  </td></tr>

  <!-- ALERTS -->
  <tr><td style='padding:0 32px;'>{alerts_html}</td></tr>

  <!-- KPIs -->
  <tr><td style='padding:8px 32px 0;'>
    <h2 style='color:#1a1a2e;font-size:17px;margin:16px 0 12px;'>📊 Indicadores Clave</h2>
    <table style='width:100%;border-collapse:collapse;'>
      <tbody>{kpi_rows}</tbody>
    </table>
  </td></tr>

  <!-- CHOLLOS -->
  <tr><td style='padding:0 32px;'>{chollos_html}</td></tr>

  <!-- YIELDS -->
  <tr><td style='padding:0 32px;'>{yields_html}</td></tr>

  <!-- FOOTER -->
  <tr><td style='background:#f9f9f9;padding:20px 32px;text-align:center;border-top:1px solid #eee;margin-top:32px;'>
    <p style='margin:0;color:#aaa;font-size:11px;'>
      Generado automáticamente por el scraper de Idealista Madrid.<br>
      Datos de venta: Idealista · Euríbor: BCE · Paro: INE
    </p>
  </td></tr>

</table>
</td></tr>
</table>
</body></html>"""


# =============================================================================
# GMAIL SENDER
# =============================================================================

def send_report(html: str, subject: str) -> bool:
    """
    Send HTML email via Gmail SMTP using App Password.
    Requires GMAIL_APP_PASSWORD in .env.

    Returns True if sent successfully.
    """
    app_password = os.getenv("GMAIL_APP_PASSWORD", "").replace(" ", "")
    if not app_password:
        print(
            "\n💡 Email no configurado. Para activarlo:\n"
            "   1. Activa verificación en 2 pasos en perroverdeviejo@gmail.com\n"
            "   2. Crea una App Password en: myaccount.google.com → Seguridad → Contraseñas de apps\n"
            "   3. Añade a .env:  GMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx\n"
        )
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SENDER_EMAIL
    msg["To"]      = RECIPIENT_EMAIL
    msg.attach(MIMEText(html, "html", "utf-8"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(SENDER_EMAIL, app_password)
            server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, msg.as_string())
        print(f"✅ Informe enviado a {RECIPIENT_EMAIL}")
        return True
    except smtplib.SMTPAuthenticationError:
        print(
            "❌ Error de autenticación Gmail. Comprueba que:\n"
            "   - La App Password en .env es correcta (sin espacios)\n"
            "   - La cuenta perroverdeviejo@gmail.com tiene 2FA activado\n"
        )
        return False
    except Exception as exc:
        print(f"❌ Error enviando email: {exc}")
        traceback.print_exc()
        return False


# =============================================================================
# MAIN ENTRY POINT  (called from scraper.py)
# =============================================================================

def send_daily_report() -> bool:
    """
    Build and send the daily market report.
    Imports all needed data directly — no arguments required.
    Returns True if email was sent successfully.
    """
    try:
        from market_indicators import (
            get_all_internal_indicators,
            calculate_market_score,
            get_market_alerts,
        )
        from macro_data import get_all_macro_data, get_euribor_data, get_paro_data
        from database import get_properties_with_multiple_drops, get_rental_yields

        # Gather data
        euribor   = get_euribor_data()
        euribor_rate = euribor.get("current") if euribor else None
        indicators = get_all_internal_indicators(euribor_rate=euribor_rate)
        macro      = get_all_macro_data()
        paro       = get_paro_data()

        score = calculate_market_score(
            price_trend    = indicators.get("price_trend", {}),
            sales_speed    = indicators.get("sales_speed", {}),
            supply_demand  = indicators.get("supply_demand", {}),
            inventory      = indicators.get("inventory", {}),
            euribor        = euribor,
            paro           = paro,
            affordability  = indicators.get("affordability"),
            price_drop_ratio = indicators.get("price_drop_ratio"),
        )

        alerts = get_market_alerts(
            price_trend   = indicators.get("price_trend", {}),
            sales_speed   = indicators.get("sales_speed", {}),
            supply_demand = indicators.get("supply_demand", {}),
            inventory     = indicators.get("inventory", {}),
            rotation      = indicators.get("rotation", {}),
            affordability = indicators.get("affordability", {}),
            macro         = macro,
        )

        chollos = get_properties_with_multiple_drops(min_drops=2, min_total_drop_pct=5.0)
        yields  = get_rental_yields(min_listings=3)

        # Build & send
        html    = build_html_report(score, indicators, macro, chollos, yields, alerts)
        today   = datetime.now().strftime("%d/%m/%Y")
        subject = (
            f"📊 Mercado Madrid {today} — Score {score.get('score', '?')}/100 "
            f"{score.get('emoji', '')} {score.get('label', '')}"
        )
        return send_report(html, subject)

    except Exception as exc:
        print(f"❌ Error generando informe diario: {exc}")
        traceback.print_exc()
        return False


# =============================================================================
# CLI — test mode
# =============================================================================

if __name__ == "__main__":
    import sys
    dry_run = "--dry-run" in sys.argv

    if dry_run:
        # Generate HTML but save to file instead of sending
        print("🔍 Modo dry-run: generando HTML sin enviar...")
        try:
            from market_indicators import get_all_internal_indicators, calculate_market_score, get_market_alerts
            from macro_data import get_all_macro_data, get_euribor_data, get_paro_data
            from database import get_properties_with_multiple_drops, get_rental_yields

            euribor      = get_euribor_data()
            euribor_rate = euribor.get("current") if euribor else None
            indicators   = get_all_internal_indicators(euribor_rate=euribor_rate)
            macro        = get_all_macro_data()
            paro         = get_paro_data()
            score        = calculate_market_score(
                price_trend    = indicators.get("price_trend", {}),
                sales_speed    = indicators.get("sales_speed", {}),
                supply_demand  = indicators.get("supply_demand", {}),
                inventory      = indicators.get("inventory", {}),
                euribor        = euribor,
                paro           = paro,
                affordability  = indicators.get("affordability"),
                price_drop_ratio = indicators.get("price_drop_ratio"),
            )
            alerts  = get_market_alerts(
                price_trend   = indicators.get("price_trend", {}),
                sales_speed   = indicators.get("sales_speed", {}),
                supply_demand = indicators.get("supply_demand", {}),
                inventory     = indicators.get("inventory", {}),
                rotation      = indicators.get("rotation", {}),
                affordability = indicators.get("affordability", {}),
                macro         = macro,
            )
            chollos = get_properties_with_multiple_drops(min_drops=2, min_total_drop_pct=5.0)
            yields  = get_rental_yields(min_listings=3)
            html    = build_html_report(score, indicators, macro, chollos, yields, alerts)

            out = "email_preview.html"
            with open(out, "w", encoding="utf-8") as f:
                f.write(html)
            print(f"✅ HTML guardado en {out} — ábrelo en el navegador para previsualizar")
        except Exception as e:
            print(f"❌ Error: {e}")
            traceback.print_exc()
    else:
        print("Enviando informe diario...")
        send_daily_report()
