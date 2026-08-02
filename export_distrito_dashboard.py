"""
Genera un dashboard HTML autocontenido con la evolución de precios de un
distrito (venta real notarial + precio de oferta del año en curso + desglose
por barrio + señales de mercado) y una previsión a cierre de año.

Los datos se leen de la BD en el momento de ejecutar, así que cada vez que lo
lances tendrás cifras frescas. El HTML resultante es estático: para actualizar
el artefacto publicado hay que volver a lanzarlo y republicar el fichero.

Uso:
    python export_distrito_dashboard.py                        # Moratalaz -> public/dashboards/
    python export_distrito_dashboard.py --distrito Retiro
    python export_distrito_dashboard.py --distrito Moratalaz -o /ruta/salida.html
    python export_distrito_dashboard.py --year 2026

Requiere DB_BACKEND=postgres (o DATABASE_URL en .env). El script lo fuerza
automáticamente si encuentra DATABASE_URL.
"""

import argparse
import json
import os
import re
import sys
import unicodedata
from datetime import date, datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ---------------------------------------------------------------------------
# Supuestos de la PREVISIÓN (editables). No son un modelo: son una regla
# transparente y conservadora. Cámbialos aquí si quieres otro escenario.
# ---------------------------------------------------------------------------
# Venta real (notarial): la banda va del CAGR de largo plazo (suelo) a la media
# entre ese CAGR y el último interanual (techo). Modera picos recientes.
FORECAST_YEARS_CAGR = 5          # ventana del CAGR de largo plazo
# Oferta (asking) a fin de año: se asume esencialmente plana respecto al último
# dato, con una banda estrecha.
ASKING_FC_LOW_FACTOR = 0.995
ASKING_FC_HIGH_FACTOR = 1.030
# Un mes de oferta se considera fiable si tiene al menos esta fracción de la
# mediana de anuncios/mes (filtra cortes de scraping).
RELIABLE_N_FRACTION = 0.5


def _die(msg: str) -> None:
    print(f"❌ {msg}", file=sys.stderr)
    sys.exit(1)


def slugify(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def fetch_data(distrito: str, year: int) -> dict:
    from database import get_connection

    like = f"%{distrito}%"
    ystart = f"{year}-01-01"
    yend = f"{year + 1}-01-01"

    with get_connection() as conn:
        c = conn.cursor()

        def one(sql, params=()):
            c.execute(sql, params)
            return c.fetchone()

        def all_(sql, params=()):
            c.execute(sql, params)
            return c.fetchall()

        # --- ¿existe el distrito? ---
        n_any = one(
            "SELECT COUNT(*) AS n FROM listings WHERE distrito ILIKE ?", (like,)
        )["n"]
        if not n_any:
            _die(
                f"No hay anuncios para un distrito que contenga '{distrito}'. "
                "Revisa el nombre (p. ej. 'Moratalaz', 'Retiro', 'Chamberí')."
            )

        # --- Serie notarial (venta real) ---
        notarial = [
            [r["periodo"], round(r["precio_m2"])]
            for r in all_(
                "SELECT periodo, precio_m2 FROM notarial_prices "
                "WHERE distrito ILIKE ? ORDER BY periodo",
                (like,),
            )
        ]

        # --- Oferta mensual del año en curso ---
        asking_monthly = []
        for m in range(1, 13):
            ms = f"{year}-{m:02d}-01"
            me = f"{year}-{m + 1:02d}-01" if m < 12 else f"{year + 1}-01-01"
            # solo meses ya empezados
            if date(year, m, 1) > date.today():
                break
            r = one(
                "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP "
                "  (ORDER BY price/NULLIF(size_sqm,0)) AS med, "
                "COUNT(*) AS n "
                "FROM listings "
                "WHERE distrito ILIKE ? AND size_sqm > 0 "
                "  AND first_seen_date < ? AND last_seen_date >= ?",
                (like, me, ms),
            )
            asking_monthly.append(
                {"m": m, "val": round(r["med"]) if r["med"] else None, "n": r["n"]}
            )

        # marcar fiabilidad por volumen
        ns = [x["n"] for x in asking_monthly if x["n"]]
        ns_sorted = sorted(ns)
        median_n = ns_sorted[len(ns_sorted) // 2] if ns_sorted else 0
        thresh = median_n * RELIABLE_N_FRACTION
        for x in asking_monthly:
            x["reliable"] = bool(x["val"]) and x["n"] >= thresh

        # --- Desglose por barrio (activos) ---
        barrios = [
            [r["barrio"], round(r["eur_m2"]), r["n"]]
            for r in all_(
                "SELECT barrio, "
                "  ROUND(AVG(price/NULLIF(size_sqm,0))::numeric, 0) AS eur_m2, "
                "  COUNT(*) AS n "
                "FROM listings "
                "WHERE distrito ILIKE ? AND status = 'active' AND size_sqm > 0 "
                "GROUP BY barrio HAVING COUNT(*) >= 5 "
                "ORDER BY eur_m2 DESC",
                (like,),
            )
        ]

        # --- Señales de mercado ---
        active_total = one(
            "SELECT COUNT(*) AS n FROM listings "
            "WHERE distrito ILIKE ? AND status = 'active'",
            (like,),
        )["n"]

        asking_now = one(
            "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP "
            "  (ORDER BY price/NULLIF(size_sqm,0)) AS med "
            "FROM listings "
            "WHERE distrito ILIKE ? AND status = 'active' AND size_sqm > 0",
            (like,),
        )["med"]
        asking_now = round(asking_now) if asking_now else None

        chg = one(
            "SELECT "
            "  SUM(CASE WHEN ph.change_amount < 0 THEN 1 ELSE 0 END) AS cuts, "
            "  SUM(CASE WHEN ph.change_amount > 0 THEN 1 ELSE 0 END) AS rises, "
            "  ROUND(AVG(CASE WHEN ph.change_amount < 0 "
            "    THEN ph.change_percent END)::numeric, 1) AS cut_pct "
            "FROM price_history ph JOIN listings l ON l.listing_id = ph.listing_id "
            "WHERE l.distrito ILIKE ? "
            "  AND ph.date_recorded::date >= CURRENT_DATE - INTERVAL '90 days'",
            (like,),
        )

        removed60 = one(
            "SELECT COUNT(*) AS n FROM listings "
            "WHERE distrito ILIKE ? AND status <> 'active' "
            "  AND last_seen_date::date >= CURRENT_DATE - INTERVAL '60 days'",
            (like,),
        )["n"]
        new60 = one(
            "SELECT COUNT(*) AS n FROM listings "
            "WHERE distrito ILIKE ? "
            "  AND first_seen_date::date >= CURRENT_DATE - INTERVAL '60 days'",
            (like,),
        )["n"]
        dom = one(
            "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP "
            "  (ORDER BY (CURRENT_DATE - first_seen_date::date)) AS d "
            "FROM listings "
            "WHERE distrito ILIKE ? AND status = 'active'",
            (like,),
        )["d"]

        # --- Velocidad de venta (sold_removed = desapareció del portal) ---
        WIN = 120  # ventana en días para las ventas recientes
        sell = one(
            "SELECT COUNT(*) AS n, "
            "  PERCENTILE_CONT(0.5) WITHIN GROUP "
            "    (ORDER BY (last_seen_date::date - first_seen_date::date)) AS med, "
            "  ROUND(AVG(last_seen_date::date - first_seen_date::date)::numeric,0) AS avg, "
            "  SUM(CASE WHEN (last_seen_date::date - first_seen_date::date) <= 30 "
            "      THEN 1 ELSE 0 END) AS le30, "
            "  SUM(CASE WHEN (last_seen_date::date - first_seen_date::date) BETWEEN 31 AND 90 "
            "      THEN 1 ELSE 0 END) AS m1_3, "
            "  SUM(CASE WHEN (last_seen_date::date - first_seen_date::date) > 90 "
            "      THEN 1 ELSE 0 END) AS gt90 "
            "FROM listings "
            "WHERE distrito ILIKE ? AND status = 'sold_removed' "
            "  AND last_seen_date::date >= CURRENT_DATE - INTERVAL '120 days' "
            "  AND last_seen_date::date > first_seen_date::date",
            (like,),
        )
        sold30 = one(
            "SELECT COUNT(*) AS n FROM listings "
            "WHERE distrito ILIKE ? AND status = 'sold_removed' "
            "  AND last_seen_date::date >= CURRENT_DATE - INTERVAL '30 days'",
            (like,),
        )["n"]
        # Días hasta vender (mediana) por barrio — solo barrios con muestra suficiente
        sell_by_barrio = [
            [r["barrio"], round(r["med"]), r["n"]]
            for r in all_(
                "SELECT barrio, COUNT(*) AS n, "
                "  PERCENTILE_CONT(0.5) WITHIN GROUP "
                "    (ORDER BY (last_seen_date::date - first_seen_date::date)) AS med "
                "FROM listings "
                "WHERE distrito ILIKE ? AND status = 'sold_removed' "
                "  AND last_seen_date::date >= CURRENT_DATE - INTERVAL '120 days' "
                "  AND last_seen_date::date > first_seen_date::date "
                "GROUP BY barrio HAVING COUNT(*) >= 8 "
                "ORDER BY med ASC",
                (like,),
            )
        ]

    absorption = round(sold30 / active_total * 100, 1) if active_total else None
    months_supply = round(active_total / sold30, 1) if sold30 else None

    return {
        "distrito": distrito,
        "year": year,
        "generated_at": datetime.now().strftime("%d %b %Y").lower(),
        "notarial": notarial,
        "asking_monthly": asking_monthly,
        "asking_now": asking_now,
        "barrios": barrios,
        "dynamics": {
            "cuts90": chg["cuts"] or 0,
            "rises90": chg["rises"] or 0,
            "cut_pct": float(chg["cut_pct"]) if chg["cut_pct"] is not None else None,
            "new60": new60,
            "removed60": removed60,
            "dom_median": int(dom) if dom is not None else None,
            "active_total": active_total,
        },
        "velocity": {
            "window_days": WIN,
            "sold_n": sell["n"] or 0,
            "sold_median": round(sell["med"]) if sell["med"] is not None else None,
            "sold_mean": int(sell["avg"]) if sell["avg"] is not None else None,
            "le30": sell["le30"] or 0,
            "m1_3": sell["m1_3"] or 0,
            "gt90": sell["gt90"] or 0,
            "active_median": int(dom) if dom is not None else None,
            "sold_30d": sold30,
            "absorption_pct": absorption,
            "months_supply": months_supply,
            "by_barrio": sell_by_barrio,
        },
    }


def compute_forecasts(data: dict) -> dict:
    """Añade previsiones (banda) a partir de reglas transparentes y editables."""
    notarial = data["notarial"]
    fc_notarial = None
    if len(notarial) >= 2:
        last_year, last_val = notarial[-1]
        prev_val = notarial[-2][1]
        last_yoy = (last_val / prev_val - 1) if prev_val else 0.0
        # CAGR de largo plazo
        base = notarial[-(FORECAST_YEARS_CAGR + 1):]
        n_years = len(base) - 1
        cagr = (base[-1][1] / base[0][1]) ** (1 / n_years) - 1 if n_years else last_yoy
        g_low, g_high = sorted([cagr, (cagr + last_yoy) / 2])
        fc_notarial = {
            "year": last_year + 1,
            "from_year": last_year,
            "from_val": last_val,
            "last_yoy": round(last_yoy * 100, 1),
            "low": round(last_val * (1 + g_low)),
            "high": round(last_val * (1 + g_high)),
            "mid": round(last_val * (1 + (g_low + g_high) / 2)),
            "g_low": round(g_low * 100, 1),
            "g_high": round(g_high * 100, 1),
        }

    fc_asking = None
    if data["asking_now"]:
        a = data["asking_now"]
        fc_asking = {
            "low": round(a * ASKING_FC_LOW_FACTOR),
            "high": round(a * ASKING_FC_HIGH_FACTOR),
            "mid": round(a * (ASKING_FC_LOW_FACTOR + ASKING_FC_HIGH_FACTOR) / 2),
        }

    data["forecast_notarial"] = fc_notarial
    data["forecast_asking"] = fc_asking
    return data


def render_html(data: dict) -> str:
    payload = json.dumps(data, ensure_ascii=False)
    first_year = data["notarial"][0][0] if data["notarial"] else ""
    last_year = data["forecast_notarial"]["year"] if data.get("forecast_notarial") else data["year"]
    title = f"{data['distrito']} · Evolución de precios {first_year}–{last_year}"
    return (
        HTML_TEMPLATE
        .replace("<title>Precios de vivienda por distrito</title>", f"<title>{title}</title>")
        .replace("/*__DATA__*/null", payload)
    )


# ---------------------------------------------------------------------------
# Plantilla HTML (fija). Toda la variabilidad entra por el objeto DATA.
# ---------------------------------------------------------------------------
HTML_TEMPLATE = r"""<title>Precios de vivienda por distrito</title>
<style>
  .viz {
    color-scheme: light;
    --surface-0:#f4f2ec; --surface-1:#fcfbf7; --surface-2:#efece3; --border:#e2ddd0;
    --text-1:#16150f; --text-2:#5f5c50; --text-3:#8f8b7c;
    --series-1:#2a5db0; --series-1-soft:#cddcf4;
    --forecast:#eb6834; --forecast-soft:#f9dbcb;
    --asking:#1baf7a; --asking-soft:#c3ecdd;
    --grid:#e6e2d6; --good:#1f8f5f; --warn:#c8641f;
  }
  @media (prefers-color-scheme: dark){
    :root:where(:not([data-theme="light"])) .viz{
      color-scheme:dark;
      --surface-0:#14140f; --surface-1:#1c1c16; --surface-2:#24231b; --border:#34322a;
      --text-1:#f6f4ea; --text-2:#b7b3a4; --text-3:#85806f;
      --series-1:#5b9bef; --series-1-soft:#22375c;
      --forecast:#f07a44; --forecast-soft:#47281a;
      --asking:#23c78c; --asking-soft:#17402f;
      --grid:#2c2b22; --good:#35b478; --warn:#e0863c;
    }
  }
  :root[data-theme="dark"] .viz{
    color-scheme:dark;
    --surface-0:#14140f; --surface-1:#1c1c16; --surface-2:#24231b; --border:#34322a;
    --text-1:#f6f4ea; --text-2:#b7b3a4; --text-3:#85806f;
    --series-1:#5b9bef; --series-1-soft:#22375c;
    --forecast:#f07a44; --forecast-soft:#47281a;
    --asking:#23c78c; --asking-soft:#17402f;
    --grid:#2c2b22; --good:#35b478; --warn:#e0863c;
  }
  *{box-sizing:border-box;}
  .viz{background:var(--surface-0);color:var(--text-1);
    font-family:ui-sans-serif,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
    font-feature-settings:"tnum" 1;-webkit-font-smoothing:antialiased;
    padding:clamp(20px,4vw,56px);min-height:100vh;line-height:1.5;}
  .wrap{max-width:940px;margin:0 auto;}
  .eyebrow{font-size:12px;letter-spacing:.14em;text-transform:uppercase;color:var(--text-3);font-weight:600;margin:0 0 10px;}
  h1{font-size:clamp(28px,5vw,44px);line-height:1.05;margin:0 0 12px;font-weight:700;letter-spacing:-.02em;text-wrap:balance;}
  .lede{font-size:16px;color:var(--text-2);max-width:62ch;margin:0 0 8px;}
  .updated{font-size:12.5px;color:var(--text-3);margin:4px 0 0;}
  .tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin:32px 0;}
  .tile{background:var(--surface-1);border:1px solid var(--border);border-radius:12px;padding:16px 16px 14px;}
  .tile .k{font-size:11.5px;letter-spacing:.04em;text-transform:uppercase;color:var(--text-3);font-weight:600;}
  .tile .v{font-size:26px;font-weight:700;letter-spacing:-.02em;margin-top:6px;}
  .tile .u{font-size:12.5px;color:var(--text-2);margin-top:3px;}
  .tile .v .unit{font-size:14px;font-weight:600;color:var(--text-2);margin-left:3px;}
  .chip{display:inline-block;font-size:12px;font-weight:700;padding:1px 7px;border-radius:999px;}
  .chip.up{color:var(--good);background:color-mix(in srgb,var(--good) 14%,transparent);}
  .chip.warn{color:var(--warn);background:color-mix(in srgb,var(--warn) 14%,transparent);}
  .panel{background:var(--surface-1);border:1px solid var(--border);border-radius:16px;padding:clamp(16px,3vw,26px);margin:18px 0;}
  .panel h2{font-size:18px;margin:0 0 3px;font-weight:700;letter-spacing:-.01em;}
  .panel .sub{font-size:13px;color:var(--text-2);margin:0 0 8px;max-width:66ch;}
  .mini-h{font-size:13.5px;font-weight:600;color:var(--text-2);margin:18px 0 2px;}
  .legend{display:flex;flex-wrap:wrap;gap:16px;margin:10px 0 2px;font-size:13px;color:var(--text-2);}
  .legend span{display:inline-flex;align-items:center;gap:7px;}
  .swatch{width:22px;height:4px;border-radius:2px;display:inline-block;}
  .swatch.dash{background:none;border-top:3px dashed var(--forecast);height:0;}
  .chart-scroll{overflow-x:auto;}
  svg{display:block;width:100%;height:auto;touch-action:pan-y;}
  svg text{font-family:inherit;}
  .axis-label{fill:var(--text-3);font-size:12px;}
  .val-label{fill:var(--text-2);font-size:11.5px;font-weight:600;}
  .tip{position:fixed;pointer-events:none;z-index:20;opacity:0;background:var(--surface-2);
    border:1px solid var(--border);border-radius:9px;padding:8px 11px;font-size:12.5px;color:var(--text-1);
    box-shadow:0 6px 22px rgba(0,0,0,.16);transition:opacity .1s;white-space:nowrap;}
  .tip b{font-size:14px;} .tip .muted{color:var(--text-3);}
  details.method{margin:22px 0 0;border-top:1px solid var(--border);padding-top:16px;}
  details.method summary{cursor:pointer;font-weight:600;font-size:14px;color:var(--text-2);}
  details.method li{font-size:13px;color:var(--text-2);margin-bottom:6px;}
  details.method ul{margin:10px 0 0;padding-left:18px;}
  .foot{font-size:12px;color:var(--text-3);margin-top:26px;}
  .note{font-size:12.5px;color:var(--text-2);margin:10px 0 0;padding:9px 12px;background:var(--surface-2);border-radius:9px;border-left:3px solid var(--forecast);}
</style>

<div class="viz"><div class="wrap">
  <p class="eyebrow" id="eyebrow"></p>
  <h1 id="title"></h1>
  <p class="lede">Precio de venta real (escrituras notariales) frente al precio de oferta actual, con la proyección para el conjunto del año.</p>
  <p class="updated" id="updated"></p>
  <div class="tiles" id="tiles"></div>

  <section class="panel">
    <h2 id="h-hist"></h2>
    <p class="sub" id="sub-hist"></p>
    <div class="legend">
      <span><i class="swatch" style="background:var(--series-1)"></i>Venta real (histórico)</span>
      <span><i class="swatch dash"></i>Proyección (rango est.)</span>
    </div>
    <div class="chart-scroll"><div id="chart-hist"></div></div>
  </section>

  <section class="panel">
    <h2 id="h-ask"></h2>
    <p class="sub" id="sub-ask"></p>
    <div class="legend">
      <span><i class="swatch" style="background:var(--asking)"></i>Oferta (mediana)</span>
      <span><i class="swatch dash" style="border-top-color:var(--asking)"></i>Proyección dic. (rango est.)</span>
    </div>
    <div class="chart-scroll"><div id="chart-ask"></div></div>
    <p class="note" id="note-gap" style="display:none"></p>
  </section>

  <section class="panel">
    <h2 id="h-barrio"></h2>
    <p class="sub">Mediana del €/m² pedido en anuncios activos por barrio.</p>
    <div class="chart-scroll"><div id="chart-barrio"></div></div>
  </section>

  <section class="panel" id="panel-velocity" style="display:none">
    <h2 id="h-velocity"></h2>
    <p class="sub" id="sub-velocity"></p>
    <div class="tiles" id="velocity-tiles"></div>
    <h3 class="mini-h" id="h-velocity-barrio">Días hasta vender por barrio (más rápido arriba)</h3>
    <div class="chart-scroll"><div id="chart-velocity"></div></div>
    <p class="note" id="note-velocity"></p>
  </section>

  <details class="method">
    <summary>Metodología, señales del mercado y aviso sobre la previsión</summary>
    <ul id="method"></ul>
  </details>
  <p class="foot" id="foot"></p>
</div></div>
<div class="tip" id="tip"></div>

<script>
const DATA = /*__DATA__*/null;
(function(){
  "use strict";
  const NS="http://www.w3.org/2000/svg";
  const ML=["ene","feb","mar","abr","may","jun","jul","ago","sep","oct","nov","dic"];
  const fmt=n=>Math.round(n).toLocaleString("es-ES");
  const el=(t,a)=>{const e=document.createElementNS(NS,t);for(const k in(a||{}))e.setAttribute(k,a[k]);return e;};
  const css=v=>getComputedStyle(document.querySelector(".viz")).getPropertyValue(v).trim();
  const D=DATA;

  // ---- headings ----
  document.getElementById("eyebrow").textContent="Madrid · Distrito de "+D.distrito;
  document.getElementById("title").textContent="Evolución de precios y previsión a cierre de "+D.year;
  document.getElementById("updated").textContent="Datos a "+D.generated_at+" · €/m²";
  const nfc=D.forecast_notarial;
  document.getElementById("h-hist").textContent="Precio de venta real ("+(D.notarial[0]?D.notarial[0][0]:"")+" – "+(nfc?nfc.year:(D.notarial.slice(-1)[0]||[""])[0])+")";
  document.getElementById("sub-hist").innerHTML="Media anual de €/m² escriturado."+(nfc?" Último interanual medido: <strong>"+(nfc.last_yoy>0?"+":"")+nfc.last_yoy+"%</strong>. La proyección de "+nfc.year+" asume una moderación (regla: +"+nfc.g_low+"% a +"+nfc.g_high+"%).":"");
  document.getElementById("h-ask").textContent="Precio de oferta en "+D.year+" (mensual)";
  document.getElementById("sub-ask").innerHTML="Mediana del €/m² pedido en los anuncios activos."+(D.asking_now?" Nivel actual: <strong>"+fmt(D.asking_now)+" €/m²</strong>. La proyección a diciembre se asume prácticamente plana.":"");
  document.getElementById("h-barrio").textContent="Oferta por barrio ("+(ML[new Date().getMonth()])+". "+D.year+")";
  document.getElementById("foot").textContent="Fuente: base de datos propia (anuncios activos + histórico notarial). €/m² · "+D.distrito+", Madrid capital.";

  const tip=document.getElementById("tip");
  function showTip(html,ev){tip.innerHTML=html;tip.style.opacity=1;
    let x=ev.clientX+14,y=ev.clientY+14;const r=tip.getBoundingClientRect();
    if(x+r.width>innerWidth-8)x=ev.clientX-r.width-14;
    if(y+r.height>innerHeight-8)y=ev.clientY-r.height-14;
    tip.style.left=x+"px";tip.style.top=y+"px";}
  function hideTip(){tip.style.opacity=0;}

  // ---- stat tiles ----
  const dy=D.dynamics, tiles=[];
  if(D.notarial.length){const ly=D.notarial.slice(-1)[0];
    tiles.push({k:"Venta real "+ly[0],v:fmt(ly[1]),unit:true,
      u:nfc?('<span class="chip '+(nfc.last_yoy>=0?"up":"warn")+'">'+(nfc.last_yoy>=0?"▲ ":"▼ ")+(nfc.last_yoy>0?"+":"")+nfc.last_yoy+"% interanual</span>"):"€/m²"});}
  if(D.asking_now)tiles.push({k:"Oferta actual",v:fmt(D.asking_now),unit:true,u:"mediana · €/m²"});
  if(dy.cuts90+dy.rises90>0)tiles.push({k:"Ajuste de precios (90d)",
    v:dy.cuts90+' <span class="chip warn">↓</span> / '+dy.rises90+" ↑",
    u:dy.cut_pct!=null?("rebaja media "+dy.cut_pct+"%"):"bajadas / subidas"});
  if(dy.dom_median!=null)tiles.push({k:"Días en mercado",v:dy.dom_median,u:"mediana · activos"});
  if(nfc)tiles.push({k:"Previsión venta "+nfc.year,v:"≈"+fmt(nfc.low/1000*10)/10+"–"+fmt(nfc.high/1000*10)/10+"k",u:"€/m² · +"+nfc.g_low+" a +"+nfc.g_high+"%"});
  const tc=document.getElementById("tiles");
  tiles.forEach(t=>{const d=document.createElement("div");d.className="tile";
    d.innerHTML='<div class="k">'+t.k+'</div><div class="v">'+t.v+(t.unit?'<span class="unit">€/m²</span>':'')+'</div><div class="u">'+t.u+'</div>';
    tc.appendChild(d);});

  // ---- generic line chart ----
  function lineChart(mountId,opts){
    const W=900,H=340,m={t:22,r:22,b:40,l:56},iw=W-m.l-m.r,ih=H-m.t-m.b;
    const svg=el("svg",{viewBox:"0 0 "+W+" "+H,role:"img","aria-label":opts.aria});
    svg.style.minWidth="560px";
    const xmin=opts.xdom[0],xmax=opts.xdom[1];
    const X=v=>m.l+(v-xmin)/(xmax-xmin)*iw;
    const[ymin,ymax]=opts.ydom,Y=v=>m.t+ih-(v-ymin)/(ymax-ymin)*ih;
    opts.yticks.forEach(v=>{svg.appendChild(el("line",{x1:m.l,x2:m.l+iw,y1:Y(v),y2:Y(v),stroke:css("--grid"),"stroke-width":1}));
      const tx=el("text",{x:m.l-10,y:Y(v)+4,"text-anchor":"end",class:"axis-label"});tx.textContent=fmt(v);svg.appendChild(tx);});
    opts.xlabels.forEach(([v,lab])=>{const tx=el("text",{x:X(v),y:m.t+ih+24,"text-anchor":"middle",class:"axis-label"});tx.textContent=lab;svg.appendChild(tx);});
    if(opts.band){const b=opts.band;
      svg.appendChild(el("path",{d:"M "+X(b.x0)+" "+Y(b.hi0)+" L "+X(b.x1)+" "+Y(b.hi1)+" L "+X(b.x1)+" "+Y(b.lo1)+" L "+X(b.x0)+" "+Y(b.lo0)+" Z",fill:opts.bandFill,stroke:"none"}));}
    opts.series.forEach(s=>{
      if(s.area){const pts=s.points.filter(Boolean);if(pts.length){let a="M "+X(pts[0].x)+" "+Y(ymin);
        pts.forEach(p=>a+=" L "+X(p.x)+" "+Y(p.y));a+=" L "+X(pts[pts.length-1].x)+" "+Y(ymin)+" Z";
        svg.appendChild(el("path",{d:a,fill:s.areaFill,stroke:"none"}));}}
      let d="",pen=false;s.points.forEach(p=>{if(p==null){pen=false;return;}d+=(pen?" L ":" M ")+X(p.x)+" "+Y(p.y);pen=true;});
      svg.appendChild(el("path",{d,fill:"none",stroke:s.color,"stroke-width":s.width||2.4,"stroke-linejoin":"round","stroke-linecap":"round","stroke-dasharray":s.dash||"none"}));
    });
    if(opts.connector){const cc=opts.connector;
      svg.appendChild(el("line",{x1:X(cc.x0),y1:Y(cc.y0),x2:X(cc.x1),y2:Y(cc.y1),stroke:cc.color,"stroke-width":2,"stroke-dasharray":"3 5",opacity:.55}));}
    opts.series.forEach(s=>{s.points.forEach(p=>{if(p==null)return;
      svg.appendChild(el("circle",{cx:X(p.x),cy:Y(p.y),r:p.big?5:4,fill:css("--surface-1"),stroke:s.color,"stroke-width":2.2}));
      if(p.label){const t=el("text",{x:X(p.x),y:Y(p.y)-12,"text-anchor":"middle",class:"val-label",fill:s.color});t.textContent=fmt(p.y);svg.appendChild(t);}
      const hit=el("circle",{cx:X(p.x),cy:Y(p.y),r:16,fill:"transparent",style:"cursor:pointer"});
      hit.addEventListener("pointermove",ev=>showTip('<span class="muted">'+p.xlab+'</span><br><b style="color:'+s.color+'">'+fmt(p.y)+"</b> €/m²"+(p.sub?'<br><span class="muted">'+p.sub+"</span>":""),ev));
      hit.addEventListener("pointerleave",hideTip);svg.appendChild(hit);});});
    document.getElementById(mountId).appendChild(svg);
  }

  function niceDom(vals,pad){const mn=Math.min.apply(null,vals),mx=Math.max.apply(null,vals);
    const lo=Math.floor((mn-pad)/500)*500,hi=Math.ceil((mx+pad)/500)*500;return[lo,hi];}
  function ticks(lo,hi,step){const t=[];for(let v=lo;v<=hi+1;v+=step)t.push(v);return t;}

  // ---- Panel 1: notarial + forecast ----
  if(D.notarial.length>=2){
    const N=D.notarial, x0=N[0][0], xf=nfc?nfc.year:N[N.length-1][0];
    const allV=N.map(p=>p[1]).concat(nfc?[nfc.low,nfc.high]:[]);
    const[ylo,yhi]=niceDom(allV,150);
    const histPts=N.map((p,i)=>({x:p[0],y:p[1],xlab:p[0],
      label:(i===0||i===N.length-1),big:(i===N.length-1),
      sub:i>0?((p[1]>=N[i-1][1]?"▲ ":"▼ ")+(((p[1]-N[i-1][1])/N[i-1][1]*100).toFixed(0))+"% vs "+N[i-1][0]):""}));
    const series=[{name:"Venta real",color:css("--series-1"),width:2.6,area:true,areaFill:css("--series-1-soft"),points:histPts}];
    let band=null;
    if(nfc){const fcPts=[{x:nfc.from_year,y:nfc.from_val,xlab:nfc.from_year},
      {x:nfc.year,y:nfc.mid,xlab:nfc.year+" (previsión)",label:true,big:true,sub:"rango "+fmt(nfc.low)+"–"+fmt(nfc.high)+" · +"+nfc.g_low+" a +"+nfc.g_high+"%"}];
      series.push({name:"Proyección",color:css("--forecast"),width:2.6,dash:"6 5",points:fcPts});
      band={x0:nfc.from_year,x1:nfc.year,lo0:nfc.from_val,hi0:nfc.from_val,lo1:nfc.low,hi1:nfc.high};}
    const span=xf-x0;
    const xl=[];for(let y=x0;y<=xf;y+=(span>8?2:1))xl.push([y,"'"+String(y).slice(2)+(y===xf&&nfc?"p":"")]);
    lineChart("chart-hist",{aria:"Precio de venta real "+D.distrito,
      xdom:[x0,xf],xlabels:xl,ydom:[ylo,yhi],yticks:ticks(ylo,yhi,500),
      bandFill:css("--forecast-soft"),band,series});
  }

  // ---- Panel 2: asking monthly + forecast ----
  const AM=D.asking_monthly.filter(x=>x.val!=null);
  if(AM.length){
    const afc=D.forecast_asking;
    const lastMonth=AM[AM.length-1];
    const allV=AM.map(x=>x.val).concat(afc?[afc.low,afc.high]:[]);
    const[ylo,yhi]=niceDom(allV,80);
    // fiabilidad: solo dibujamos meses fiables como puntos de la serie; los no fiables se saltan (gap)
    const pts=[];const firstM=AM[0].m, xEnd=afc?12:lastMonth.m;
    for(let mo=firstM;mo<=lastMonth.m;mo++){
      const rec=D.asking_monthly.find(x=>x.m===mo);
      if(rec&&rec.reliable)pts.push({x:mo,y:rec.val,xlab:ML[mo-1]+" "+D.year,
        big:(mo===lastMonth.m),label:(mo===firstM||mo===lastMonth.m),sub:"n="+rec.n});
      else pts.push(null);
    }
    const series=[{name:"Oferta",color:css("--asking"),width:2.8,points:pts}];
    let band=null,connector=null;
    if(afc){series.push({name:"Proyección",color:css("--asking"),width:2.6,dash:"6 5",
      points:[{x:lastMonth.m,y:lastMonth.val,xlab:ML[lastMonth.m-1]},
        {x:12,y:afc.mid,xlab:"dic (previsión)",big:true,label:true,sub:"rango "+fmt(afc.low)+"–"+fmt(afc.high)+" · plano"}]});
      band={x0:lastMonth.m,x1:12,lo0:lastMonth.val,hi0:lastMonth.val,lo1:afc.low,hi1:afc.high};}
    // conector punteado a través de huecos no fiables entre dos meses fiables
    const rel=D.asking_monthly.filter(x=>x.reliable);
    for(let i=1;i<rel.length;i++){if(rel[i].m-rel[i-1].m>1){
      connector={x0:rel[i-1].m,y0:rel[i-1].val,x1:rel[i].m,y1:rel[i].val,color:css("--asking")};break;}}
    const xl=[];for(let mo=firstM;mo<=xEnd;mo++){if(mo%1===0)xl.push([mo,ML[mo-1]+(mo===12&&afc?" p":"")]);}
    lineChart("chart-ask",{aria:"Precio de oferta mensual "+D.distrito,
      xdom:[firstM,xEnd],xlabels:xl,ydom:[ylo,yhi],yticks:ticks(ylo,yhi,200),
      bandFill:css("--asking-soft"),band,connector,series});
    // aviso de huecos
    const gaps=D.asking_monthly.filter(x=>!x.reliable&&x.m<=lastMonth.m);
    if(gaps.length){const g=document.getElementById("note-gap");g.style.display="";
      g.textContent=(gaps.length===1?"El mes de ":"Los meses de ")+gaps.map(x=>ML[x.m-1]).join(", ")+
      " se omite"+(gaps.length===1?"":"n")+": muestra insuficiente (corte en la recogida de datos), no comparable"+(gaps.length===1?"":"s")+".";}
  }

  // ---- Panel 3: barrios ----
  if(D.barrios.length){
    const data=D.barrios;
    const W=900,H=Math.max(180,44*data.length+44),m={t:16,r:64,b:28,l:120},iw=W-m.l-m.r,ih=H-m.t-m.b;
    const svg=el("svg",{viewBox:"0 0 "+W+" "+H,role:"img","aria-label":"Precio de oferta por barrio"});
    svg.style.minWidth="520px";
    const xmax=Math.ceil(Math.max.apply(null,data.map(d=>d[1]))/500)*500;
    const X=v=>m.l+v/xmax*iw,gap=ih/data.length,bh=gap*0.62;
    ticks(0,xmax,1000).slice(1).forEach(v=>{svg.appendChild(el("line",{x1:X(v),x2:X(v),y1:m.t,y2:m.t+ih,stroke:css("--grid"),"stroke-width":1}));
      const t=el("text",{x:X(v),y:m.t+ih+20,"text-anchor":"middle",class:"axis-label"});t.textContent=fmt(v);svg.appendChild(t);});
    data.forEach((d,i)=>{const y=m.t+i*gap+(gap-bh)/2;
      svg.appendChild(el("rect",{x:m.l,y,width:Math.max(2,X(d[1])-m.l),height:bh,rx:4,fill:css("--series-1"),opacity:(0.55+(1-i/data.length)*0.4).toFixed(2)}));
      const lab=el("text",{x:m.l-12,y:y+bh/2+4,"text-anchor":"end",fill:css("--text-1"),"font-size":13,"font-weight":600});lab.textContent=d[0];svg.appendChild(lab);
      const val=el("text",{x:X(d[1])+8,y:y+bh/2+4,fill:css("--text-2"),"font-size":12.5,"font-weight":600});val.textContent=fmt(d[1]);svg.appendChild(val);
      const hit=el("rect",{x:m.l,y:m.t+i*gap,width:iw,height:gap,fill:"transparent",style:"cursor:pointer"});
      hit.addEventListener("pointermove",ev=>showTip("<b>"+d[0]+"</b><br><b style='color:"+css("--series-1")+"'>"+fmt(d[1])+"</b> €/m²<br><span class='muted'>"+d[2]+" anuncios</span>",ev));
      hit.addEventListener("pointerleave",hideTip);svg.appendChild(hit);});
    document.getElementById("chart-barrio").appendChild(svg);
  }

  // ---- Panel: velocidad de venta ----
  const V=D.velocity;
  if(V && V.sold_n>0){
    document.getElementById("panel-velocity").style.display="";
    document.getElementById("h-velocity").textContent="¿Cuánto tarda en venderse un piso?";
    document.getElementById("sub-velocity").innerHTML=
      "Días desde que un piso se publica hasta que desaparece del portal (≈ vendido), sobre los <strong>"+
      V.sold_n+"</strong> que se fueron en los últimos "+V.window_days+" días.";
    // tiles
    const pct30=V.sold_n?Math.round(V.le30/V.sold_n*100):0;
    const vt=[
      {k:"Mediana hasta vender", v:V.sold_median!=null?V.sold_median:"—", u:"días · media "+(V.sold_mean!=null?V.sold_mean:"—")+"d"},
      {k:"Se venden en ≤30 días", v:pct30+"%", u:V.le30+" de "+V.sold_n},
      {k:"Meses de stock", v:V.months_supply!=null?V.months_supply:"—", u:"al ritmo actual ("+(V.absorption_pct!=null?V.absorption_pct:"—")+"%/mes)"},
      {k:"Llevan en venta (activos)", v:V.active_median!=null?V.active_median:"—", u:"días · mediana"},
    ];
    const vc=document.getElementById("velocity-tiles");
    vt.forEach(t=>{const d=document.createElement("div");d.className="tile";
      d.innerHTML='<div class="k">'+t.k+'</div><div class="v">'+t.v+'</div><div class="u">'+t.u+'</div>';
      vc.appendChild(d);});
    // barras por barrio (más rápido arriba)
    const data=V.by_barrio||[];
    if(data.length){
      const W=900,H=Math.max(150,42*data.length+44),m={t:16,r:58,b:28,l:120},iw=W-m.l-m.r,ih=H-m.t-m.b;
      const svg=el("svg",{viewBox:"0 0 "+W+" "+H,role:"img","aria-label":"Días hasta vender por barrio"});
      svg.style.minWidth="520px";
      const xmax=Math.ceil(Math.max.apply(null,data.map(d=>d[1]))/10)*10||10;
      const X=v=>m.l+v/xmax*iw,gap=ih/data.length,bh=gap*0.62;
      for(let t=10;t<=xmax;t+=(xmax>60?20:10)){svg.appendChild(el("line",{x1:X(t),x2:X(t),y1:m.t,y2:m.t+ih,stroke:css("--grid"),"stroke-width":1}));
        const tx=el("text",{x:X(t),y:m.t+ih+20,"text-anchor":"middle",class:"axis-label"});tx.textContent=t;svg.appendChild(tx);}
      data.forEach((d,i)=>{const y=m.t+i*gap+(gap-bh)/2;
        svg.appendChild(el("rect",{x:m.l,y,width:Math.max(2,X(d[1])-m.l),height:bh,rx:4,fill:css("--series-1"),opacity:(0.55+(1-i/data.length)*0.4).toFixed(2)}));
        const lab=el("text",{x:m.l-12,y:y+bh/2+4,"text-anchor":"end",fill:css("--text-1"),"font-size":13,"font-weight":600});lab.textContent=d[0];svg.appendChild(lab);
        const val=el("text",{x:X(d[1])+8,y:y+bh/2+4,fill:css("--text-2"),"font-size":12.5,"font-weight":600});val.textContent=d[1]+"d";svg.appendChild(val);
        const hit=el("rect",{x:m.l,y:m.t+i*gap,width:iw,height:gap,fill:"transparent",style:"cursor:pointer"});
        hit.addEventListener("pointermove",ev=>showTip("<b>"+d[0]+"</b><br><b style='color:"+css("--series-1")+"'>"+d[1]+"</b> días (mediana)<br><span class='muted'>"+d[2]+" ventas</span>",ev));
        hit.addEventListener("pointerleave",hideTip);svg.appendChild(hit);});
      document.getElementById("chart-velocity").appendChild(svg);
    } else {
      document.getElementById("h-velocity-barrio").style.display="none";
    }
    document.getElementById("note-velocity").innerHTML=
      "⚠️ \"Vendido\" aquí = desapareció del portal (a veces es una retirada, no una venta). Y la mediana va sesgada a la baja: "+
      "los pisos que no se venden siguen en el pool de activos (los "+(V.active_median!=null?V.active_median:"—")+" días de arriba). "+
      "Léelo como termómetro de ritmo, no como dato exacto.";
  }

  // ---- methodology ----
  const meth=[
    "<strong>Venta real:</strong> media anual de €/m² escriturado por distrito (fuente notarial). Serie histórica medida; el último año es proyección propia.",
    "<strong>Oferta:</strong> mediana del €/m² pedido, sobre los anuncios activos capturados cada mes por el scraper propio.",
  ];
  const parts=[];
  if(dy.cuts90+dy.rises90>0)parts.push(dy.cuts90+" bajadas de precio frente a "+dy.rises90+" subidas en 90 días"+(dy.cut_pct!=null?" (rebaja media "+dy.cut_pct+"%)":""));
  if(dy.new60||dy.removed60)parts.push(dy.new60+" anuncios nuevos vs. "+dy.removed60+" vendidos/retirados en 60 días");
  if(dy.dom_median!=null)parts.push(dy.dom_median+" días de mediana en mercado");
  if(parts.length)meth.push("<strong>Señales actuales:</strong> "+parts.join("; ")+".");
  if(nfc)meth.push("<strong>Base de la previsión:</strong> venta "+nfc.year+" estimada en "+fmt(nfc.low)+"–"+fmt(nfc.high)+" €/m² (+"+nfc.g_low+" a +"+nfc.g_high+"% interanual, regla: CAGR de largo plazo → media con el último interanual). Es una estimación editable, no un dato.");
  if(V && V.sold_n>0)meth.push("<strong>Velocidad de venta:</strong> días entre publicación y desaparición del portal (status <code>sold_removed</code>), sobre ventas de los últimos "+V.window_days+" días. Mediana sesgada a la baja por supervivencia (los lentos siguen activos); \"vendido\" incluye retiradas. Meses de stock = activos ÷ vendidos del último mes.");
  meth.push("La brecha entre oferta y venta real es normal: el notarial es media anual retrasada y el pedido es aspiracional.");
  document.getElementById("method").innerHTML=meth.map(x=>"<li>"+x+"</li>").join("");
})();
</script>
"""


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--distrito", default="Moratalaz",
                    help="Nombre del distrito (por defecto: Moratalaz)")
    ap.add_argument("--year", type=int, default=date.today().year,
                    help="Año en curso para la serie de oferta (por defecto: actual)")
    ap.add_argument("-o", "--output", default=None,
                    help="Ruta del HTML de salida (por defecto: public/dashboards/<slug>_precios.html)")
    args = ap.parse_args()

    # Forzar backend Postgres si hay DATABASE_URL disponible.
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
    except Exception:
        pass
    if os.environ.get("DATABASE_URL") and os.environ.get("DB_BACKEND", "").lower() != "postgres":
        os.environ["DB_BACKEND"] = "postgres"

    print(f"📊 Consultando datos de {args.distrito} ({args.year})...")
    data = fetch_data(args.distrito, args.year)
    data = compute_forecasts(data)

    out = args.output or os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "public", "dashboards", f"{slugify(args.distrito)}_precios.html",
    )
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        f.write(render_html(data))

    nfc = data.get("forecast_notarial")
    print(f"✅ Dashboard generado: {out}")
    print(f"   Barrios: {len(data['barrios'])} · "
          f"Oferta actual: {data['asking_now']} €/m² · "
          f"Serie notarial: {len(data['notarial'])} años")
    if nfc:
        print(f"   Previsión venta {nfc['year']}: {nfc['low']}–{nfc['high']} €/m² "
              f"(+{nfc['g_low']} a +{nfc['g_high']}%)")
    print("\n👉 Para publicarlo como artefacto: ábrelo y pídeme que lo republique, "
          "o pásame la ruta.")


if __name__ == "__main__":
    main()
