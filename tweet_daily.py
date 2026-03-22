#!/usr/bin/env python3
"""
tweet_daily.py — Publicación semiautomática diaria para @Madridhometech

Uso:
    python3 tweet_daily.py           # genera borrador, pide confirmación antes de publicar
    python3 tweet_daily.py --dry-run # solo muestra el borrador sin publicar
    python3 tweet_daily.py --force   # publica sin pedir confirmación

Variables de entorno necesarias (en .env o exportadas):
    TWITTER_API_KEY
    TWITTER_API_SECRET
    TWITTER_ACCESS_TOKEN
    TWITTER_ACCESS_TOKEN_SECRET

Requiere:
    pip3 install tweepy python-dotenv
"""

import json
import os
import sys
import random
from datetime import datetime, date
from pathlib import Path

# ── Carga .env si existe ─────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass  # python-dotenv no instalado, se usan las vars de entorno del sistema

# ── Rutas ────────────────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).parent
METRICS_FILE = BASE_DIR / "market-thermometer" / "public" / "metrics.json"
NEWS_FILE    = BASE_DIR / "market-thermometer" / "content" / "news.json"
SITE_URL     = "https://madridhome.tech"


# ── Carga de datos ────────────────────────────────────────────────────────────

def load_metrics() -> dict:
    try:
        with open(METRICS_FILE) as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️  No se pudo cargar metrics.json: {e}")
        return {}

def load_news() -> list:
    try:
        with open(NEWS_FILE) as f:
            items = json.load(f)
        return sorted(items, key=lambda x: x["date"], reverse=True)
    except Exception as e:
        print(f"⚠️  No se pudo cargar news.json: {e}")
        return []


# ── Generadores de tweets ─────────────────────────────────────────────────────

def fmt_price(p) -> str:
    if p is None:
        return "—"
    return f"{int(p):,}".replace(",", ".") + " €"

def fmt_pct(p) -> str:
    if p is None:
        return "—"
    sign = "+" if p > 0 else ""
    return f"{sign}{p:.1f}%"

def tweet_dashboard_summary(metrics: dict) -> str:
    """Lunes: resumen semanal del dashboard."""
    ind = metrics.get("indicators", {})
    pt  = ind.get("price_trend", {})
    inv = ind.get("inventory", {})
    spd = ind.get("sales_speed", {})

    price     = fmt_price(pt.get("current"))
    chg       = fmt_pct(pt.get("change_pct"))
    sqm       = pt.get("current_sqm")
    sqm_str   = f"{int(sqm):,} €/m²".replace(",", ".") if sqm else "—"
    stock     = inv.get("current")
    stock_str = f"{int(stock):,}".replace(",", ".") if stock else "—"
    speed     = spd.get("current")
    speed_str = f"{speed:.0f} días" if speed else "—"
    score     = metrics.get("market_score", {}).get("score")
    label     = metrics.get("market_score", {}).get("label", "")

    options = [
        f"📊 Mercado inmobiliario Madrid — semana {date.today().isocalendar()[1]}\n\n"
        f"🏠 Precio mediano: {price} ({chg} vs semanas anteriores)\n"
        f"📐 €/m²: {sqm_str}\n"
        f"📦 Stock activo: {stock_str} pisos\n"
        f"⚡ Velocidad de venta: {speed_str}\n\n"
        f"Termómetro del mercado: {score}/100 — {label}\n\n"
        f"🔗 {SITE_URL}",

        f"🏡 ¿Cómo está el mercado de la vivienda en #Madrid esta semana?\n\n"
        f"💰 Precio mediano: {price}\n"
        f"📈 Variación semanal: {chg}\n"
        f"🏘️ Pisos en venta: {stock_str}\n\n"
        f"Todos los datos en tiempo real 👇\n{SITE_URL} #InmobiliarioMadrid",
    ]
    return random.choice(options)


def tweet_news(news: list) -> str:
    """Martes y jueves: compartir última noticia relevante."""
    if not news:
        return tweet_dashboard_summary(load_metrics())

    item = news[0]  # más reciente
    title   = item["title"]
    summary = item["summary"]
    url     = item["url"]
    source  = item["source"]

    # Truncar summary si es muy largo (280 chars - overhead)
    max_summary = 120
    if len(summary) > max_summary:
        summary = summary[:max_summary].rstrip() + "…"

    options = [
        f"📰 {title}\n\n{summary}\n\nVía {source} 🔗 {url}\n\n#Vivienda #Madrid #Inmobiliario",
        f"🗞️ {title}\n\n{summary}\n\n→ {url}",
    ]
    return random.choice(options)


def tweet_district_ranking(metrics: dict) -> str:
    """Miércoles: ranking de distritos por precio."""
    ind      = metrics.get("indicators", {})
    districts = ind.get("district_prices", {}).get("districts", [])

    if not districts:
        return tweet_dashboard_summary(metrics)

    # Top 3 más caros y top 3 más baratos
    by_sqm  = sorted(districts, key=lambda d: d.get("median_sqm", 0), reverse=True)
    top3    = by_sqm[:3]
    cheap3  = by_sqm[-3:][::-1]

    top_lines   = "\n".join(f"  {d['district']}: {int(d['median_sqm']):,} €/m²".replace(",", ".") for d in top3)
    cheap_lines = "\n".join(f"  {d['district']}: {int(d['median_sqm']):,} €/m²".replace(",", ".") for d in cheap3)

    return (
        f"🗺️ Ranking de precios por distrito en #Madrid\n\n"
        f"🔴 Más caros:\n{top_lines}\n\n"
        f"🟢 Más económicos:\n{cheap_lines}\n\n"
        f"Tabla completa: {SITE_URL} #Vivienda #InmobiliarioMadrid"
    )


def tweet_rental_yields(metrics: dict) -> str:
    """Viernes: rentabilidad de alquiler."""
    ind    = metrics.get("indicators", {})
    rental = ind.get("rental_yields", {})
    yields = rental.get("yields", [])

    if not yields:
        return tweet_dashboard_summary(metrics)

    top5 = sorted(yields, key=lambda y: y.get("yield_pct", 0), reverse=True)[:5]
    lines = "\n".join(
        f"  {y['barrio']}: {y['yield_pct']:.1f}% ({y.get('median_rent', '—')} €/mes)"
        for y in top5
    )

    avg_yield = rental.get("avg_yield")
    avg_str   = f"{avg_yield:.1f}%" if avg_yield else "—"

    return (
        f"💰 Top barrios por rentabilidad de alquiler en #Madrid\n\n"
        f"{lines}\n\n"
        f"Media ciudad: {avg_str}\n\n"
        f"Más datos: {SITE_URL} #InversionInmobiliaria #Alquiler"
    )


def tweet_weekly_recap(metrics: dict) -> str:
    """Sábado: recap semanal con las alertas activas."""
    alerts = metrics.get("alerts", [])
    score  = metrics.get("market_score", {}).get("score")
    label  = metrics.get("market_score", {}).get("label", "")

    alert_lines = ""
    for a in alerts[:3]:
        emoji = "🔴" if a.get("level") == "critical" else "🟡" if a.get("level") == "warning" else "🔵"
        alert_lines += f"{emoji} {a.get('title', '')}\n"

    return (
        f"📋 Resumen semanal del mercado inmobiliario de #Madrid\n\n"
        f"Termómetro: {score}/100 — {label}\n\n"
        f"Señales activas:\n{alert_lines}\n"
        f"Panel completo: {SITE_URL} #Vivienda #InmobiliarioMadrid"
    )


def tweet_market_fact(metrics: dict) -> str:
    """Domingo: dato curioso o comparativa."""
    ind  = metrics.get("indicators", {})
    affd = ind.get("affordability", {})
    years = affd.get("years_salary")
    effort = affd.get("rental_effort_pct")

    pt    = ind.get("price_trend", {})
    price = pt.get("current")

    options = []

    if years:
        options.append(
            f"💡 ¿Sabías que en Madrid se necesitan {years:.1f} años de salario bruto "
            f"para comprar una vivienda al precio mediano actual?\n\n"
            f"La media histórica en España ronda los 7-8 años.\n\n"
            f"Datos y evolución: {SITE_URL} #Vivienda #Madrid"
        )

    if effort:
        options.append(
            f"🏠 En Madrid, el alquiler mediano supone el {effort:.1f}% del salario neto.\n\n"
            f"La Unión Europea considera 'esfuerzo extremo' superar el 40%.\n\n"
            f"Todos los indicadores: {SITE_URL} #Alquiler #Madrid #Vivienda"
        )

    if price:
        price_fmt = fmt_price(price)
        options.append(
            f"📊 El precio mediano de una vivienda en #Madrid es actualmente {price_fmt}.\n\n"
            f"¿Cuánto cuesta en tu barrio? Consulta el desglose por distrito 👇\n"
            f"{SITE_URL} #InmobiliarioMadrid"
        )

    if not options:
        return tweet_dashboard_summary(metrics)

    return random.choice(options)


# ── Selector de contenido por día de la semana ────────────────────────────────

ROTATION = {
    0: "dashboard",   # Lunes
    1: "news",        # Martes
    2: "districts",   # Miércoles
    3: "news",        # Jueves
    4: "rental",      # Viernes
    5: "recap",       # Sábado
    6: "fact",        # Domingo
}

def generate_tweet(metrics: dict, news: list, tipo: str = None) -> str:
    if tipo is None:
        tipo = ROTATION[date.today().weekday()]

    generators = {
        "dashboard": lambda: tweet_dashboard_summary(metrics),
        "news":      lambda: tweet_news(news),
        "districts": lambda: tweet_district_ranking(metrics),
        "rental":    lambda: tweet_rental_yields(metrics),
        "recap":     lambda: tweet_weekly_recap(metrics),
        "fact":      lambda: tweet_market_fact(metrics),
    }
    return generators.get(tipo, lambda: tweet_dashboard_summary(metrics))()


# ── Publicación en Twitter ────────────────────────────────────────────────────

def post_tweet(text: str) -> bool:
    try:
        import tweepy
    except ImportError:
        print("\n❌ tweepy no instalado. Ejecuta: pip3 install tweepy python-dotenv")
        sys.exit(1)

    api_key    = os.environ.get("TWITTER_API_KEY")
    api_secret = os.environ.get("TWITTER_API_SECRET")
    acc_token  = os.environ.get("TWITTER_ACCESS_TOKEN")
    acc_secret = os.environ.get("TWITTER_ACCESS_TOKEN_SECRET")

    if not all([api_key, api_secret, acc_token, acc_secret]):
        print("\n❌ Faltan variables de entorno. Comprueba tu archivo .env:")
        print("   TWITTER_API_KEY, TWITTER_API_SECRET,")
        print("   TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET")
        sys.exit(1)

    client = tweepy.Client(
        consumer_key=api_key,
        consumer_secret=api_secret,
        access_token=acc_token,
        access_token_secret=acc_secret,
    )

    response = client.create_tweet(text=text)
    tweet_id = response.data["id"]
    print(f"\n✅ Tweet publicado: https://twitter.com/Madridhometech/status/{tweet_id}")
    return True


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    dry_run = "--dry-run" in sys.argv
    force   = "--force" in sys.argv

    # Tipo de contenido opcional: python3 tweet_daily.py --tipo=news
    tipo = None
    for arg in sys.argv:
        if arg.startswith("--tipo="):
            tipo = arg.split("=", 1)[1]

    print("📡 Cargando datos...")
    metrics = load_metrics()
    news    = load_news()

    weekday_names = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
    today_name = weekday_names[date.today().weekday()]
    content_type = tipo or ROTATION[date.today().weekday()]

    print(f"📅 Hoy es {today_name} → contenido: {content_type}\n")

    tweet = generate_tweet(metrics, news, tipo)

    print("─" * 60)
    print(tweet)
    print("─" * 60)
    print(f"\n📏 Caracteres: {len(tweet)}/280")

    if len(tweet) > 280:
        print("⚠️  El tweet supera 280 caracteres. Se truncará automáticamente.")

    if dry_run:
        print("\n🔍 Modo dry-run: no se publica nada.")
        return

    if not force:
        respuesta = input("\n¿Publicar este tweet? [s/N] ").strip().lower()
        if respuesta not in ("s", "si", "sí", "y", "yes"):
            print("❌ Publicación cancelada.")
            return

    post_tweet(tweet[:280])


if __name__ == "__main__":
    main()
