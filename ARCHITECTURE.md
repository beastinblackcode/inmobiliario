# Arquitectura del Sistema: Madrid Real Estate Tracker

> **Última actualización:** junio 2026

## 📋 Índice

1. [Visión General](#visión-general)
2. [Mapa de Archivos del Proyecto](#mapa-de-archivos-del-proyecto)
3. [Componentes del Sistema](#componentes-del-sistema)
4. [Arquitectura de Datos](#arquitectura-de-datos)
5. [Panel de Vigilancia del Mercado](#panel-de-vigilancia-del-mercado)
6. [Flujo de Operación](#flujo-de-operación)
7. [Despliegue](#despliegue)
8. [Seguridad](#seguridad)
9. [Costes y Escalabilidad](#costes-y-escalabilidad)

---

## Visión General

Sistema de monitorización del mercado inmobiliario de Madrid que:

- Rastrea diariamente ~184 barrios de Madrid vía scraping de Idealista
- Detecta nuevas propiedades, cambios de precio y ventas
- Visualiza tendencias y métricas del mercado en un dashboard Streamlit
- Calcula un score de salud del mercado combinando indicadores internos y macro
- Proporciona acceso web seguro con autenticación multi-usuario

### Diagrama General

```
┌─────────────────────────────────────────────────────────────┐
│                    USUARIO (Navegador)                       │
└───────────────┬─────────────────────────────┬───────────────┘
                │ HTTPS                         │ HTTPS
                ▼                               ▼
┌──────────────────────────────┐   ┌──────────────────────────┐
│  STREAMLIT CLOUD (interno)   │   │  VERCEL (público)        │
│  app.py → st.navigation      │   │  market-thermometer/     │
│   pages/ + tabs/             │   │  Next.js 14 · ISR        │
│   database.py · analytics.py │   │  madridhome.tech         │
│   market_indicators.py       │   │  ← metrics.json (CI)     │
│   macro_data.py · predictive │   └──────────────────────────┘
└──────────────┬───────────────┘
               │ DATABASE_URL (psycopg pool)
               ▼
       ┌─────────────────────┐
       │   NEON POSTGRES     │  ◄── scraper y CI escriben aquí
       │   (serverless)      │      directamente (sin Google Drive)
       └─────────────────────┘
               ▲
               │ upsert listings / price_history / scraping_log
┌──────────────┴──────────────────────────────────────────────┐
│        GITHUB ACTIONS (scraping + pipeline, diario)          │
│   scraper.py → compute_snapshots.py → email → export metrics │
│   retry_scraper.py · mi_zona_alerts.py                       │
└──────────────┬───────────────────────────────────────────────┘
               │
               ▼
   ┌─────────────────────┐   ┌─────────────────┐
   │  BRIGHT DATA (1º)   │   │  BCE / INE      │
   │  Oxylabs (fallback) │   │  (APIs macro)   │
   └─────────────────────┘   └─────────────────┘
               │
               ▼
   ┌─────────────────────┐
   │   IDEALISTA.COM     │
   └─────────────────────┘
```

---

## Mapa de Archivos del Proyecto

### Núcleo del Dashboard

| Archivo | Líneas (aprox.) | Función |
|---------|--------|---------|
| `app.py` | ~170 | Orquestador fino: bootstrap de backend, auth, `st.navigation`, sidebar |
| `data_utils.py` | ~40 | `load_data()` con `@st.cache_data` compartido entre páginas |
| `database.py` | ~3,640 | God-module de acceso a datos (CRUD, stats, historial). Candidato a partir — ver `ROADMAP.md` |
| `analytics.py` | ~560 | Análisis avanzado: chollos, velocidad, evolución de propiedades |
| `db/connection_pg.py` · `db/dialect.py` | — | Pool Postgres + shim de dialecto SQLite/Postgres |

### Páginas del Dashboard (`pages/`)

Streamlit multipage. Cada archivo es una página independiente accesible desde el sidebar.

| Archivo | Función |
|---------|---------|
| `pages/admin.py` | Actividad scraping, costes, estadísticas, purga manual de listings fantasma |
| `pages/bajadas.py` | Ranking de bajadas de precio por barrio + overview |
| `pages/busqueda.py` | Búsquedas personalizadas con guardado y seguimiento |
| `pages/detalle.py` | Ficha de propiedad: histórico de precios, KPIs, comparables |
| `pages/oportunidades.py` | Top oportunidades con score calidad-precio + NLP |
| `pages/seguimientos.py` | Watchlist y alertas del usuario |
| `pages/vigilancia.py` | Semáforo del mercado, indicadores internos + macro |

### Componentes reutilizables (`tabs/`)

Lógica encapsulada llamada desde las páginas (legacy nombre `tabs/`, no son tabs físicas).

| Archivo | Función |
|---------|---------|
| `tabs/admin_tab.py` | Render del panel admin (incluye purga de stale listings) |
| `tabs/alerts_tab.py` | Render de alertas del usuario |
| `tabs/detail_tab.py` | Render del detalle con `_build_chart_series()` defensivo |
| `tabs/opportunities_tab.py` | Render de oportunidades + scoring |
| `tabs/price_drops_tab.py` | Render de bajadas de precio |
| `tabs/search_tab.py` | Render de búsquedas guardadas |
| `tabs/watchlist_tab.py` | Render de propiedades seguidas |

### Vigilancia de Mercado

| Archivo | Líneas | Función |
|---------|--------|---------|
| `market_surveillance.py` | ~890 | Render de vigilancia: semáforo, KPIs, alertas, diagnóstico |
| `market_indicators.py` | ~2,440 | Cálculo de todos los indicadores internos y score de mercado (incl. absorption, months-of-supply, yield, notarial gap, lanzamientos, morosidad) |
| `macro_data.py` | ~570 | Datos macroeconómicos: Euríbor (BCE), desempleo (INE) |

### Modelo Predictivo

| Archivo | Función |
|---------|---------|
| `predictive_model.py` | Random Forest para valuación de propiedades |
| `model_metadata.json` | Metadatos del modelo entrenado (generado en runtime) |

### Scraper y pipeline diario

| Archivo | Función |
|---------|---------|
| `scraper.py` | Scraping principal de los barrios de Madrid; Bright Data primario + Oxylabs fallback, modo `lite`, coste real vía billing API |
| `retry_scraper.py` | Reintento de barrios fallidos |
| `compute_snapshots.py` | Pre-cálculo de KPIs diarios → tabla `market_snapshots` |
| `nlp_analyzer.py` | Extracción de señales NLP de descripciones (urgencia, directo, negociable) |
| `email_report.py` | Resumen diario por email |
| `tweet_daily.py` | Tweet automático diario con headline del mercado |
| `export_public_metrics.py` | Genera `metrics.json` para el frontend público |
| `compute_property_fingerprints.py` | Agrupa listings en propiedades físicas + clasifica clusters |
| `migration_sqlite_to_postgres.py` | One-shot backfill SQLite→Postgres (Phase C cutover) |
| `mi_zona_alerts.py` | Job diario que envía email con matches de Mi Zona |

### Utilidades de Visualización

| Archivo | Función |
|---------|---------|
| `map_view.py` | Creación del mapa Folium con marcadores por precio |
| `coordinates.py` | Diccionario de coordenadas lat/lon por barrio |

### Scripts de Utilidad / Mantenimiento

| Archivo | Uso |
|---------|-----|
| `fix_false_sold.py` | Corrige propiedades marcadas como vendidas por error |
| `migration_add_price_history.py` | Migración de esquema para añadir tabla `price_history` |
| `analyze_404_errors.py` | Análisis de errores 404 del scraper |
| `check_missing_barrios.py` | Detecta barrios no scrapeados |
| `geocode_barrios.py` | Geocodificación de barrios |
| `validate_barrio_urls.py` | Valida URLs de barrios |
| `inspect_html.py` | Inspección de HTML de Idealista |
| `find_oldest.py` | Busca propiedades más antiguas en BD |
| `verify_pg_queries.py` | **Barrido de queries contra Postgres**: ejercita todas las funciones de lectura y delata SQLite-isms residuales (excepciones + errores tragados). Sale 1 si algo falla. |
| `tests/` | Suite pytest (~305 tests): unit, integration, regression |

### Archivos de Configuración

| Archivo | Contenido |
|---------|-----------|
| `.env` | `DATABASE_URL` (Neon) + credenciales de proveedores (local/CI, no en git) |
| `.env.example` | Plantilla de variables de entorno |
| `.streamlit/secrets.toml` | Secrets de Streamlit Cloud: `[postgres].url` + `[auth.users_hashed]` (no en git) |
| `.streamlit/config.toml` | Config del servidor Streamlit |
| `requirements.txt` | Dependencias Python |
| `.gitignore` | Archivos excluidos del repositorio |
| `barrios_urls.csv` | URLs de todos los barrios a scrapeear |

### Archivos de Documentación

| Archivo | Contenido |
|---------|-----------|
| `README.md` | Punto de entrada y guía rápida |
| `ARCHITECTURE.md` | Este documento — cómo funciona el sistema hoy |
| `DATA_MODEL.md` | Esquema detallado de la BD |
| `DEPLOYMENT.md` | Despliegue en Streamlit Cloud |
| `AUTH.md` | Configuración de auth multi-usuario |
| `ROADMAP.md` | Próximos pasos: features pendientes + plan arquitectónico (Fase 1/2/3) |

### ⚠️ Archivos a Limpiar

| Archivo | Problema |
|---------|----------|
| `madrid_housing.db` | BD vacía (0 bytes) — eliminar |
| `real_estate_backup_*.db` | Backups pesados en repo — mover fuera del repo |
| `404_errors.log`, `scraper_output.log` | Logs de runtime — añadir a `.gitignore` |
| `barrios_scrapeados_hoy.txt` | Estado de ejecución — añadir a `.gitignore` |
| `barrio_page_history.json` | Estado de ejecución — añadir a `.gitignore` |
| `current_scraper_urls.txt`, `urls_from_web.txt` | Archivos temporales — añadir a `.gitignore` |

---

## Componentes del Sistema

### 1. Scraper (GitHub Actions)

**Archivo:** `scraper.py` (~2,460 líneas)

**Responsabilidades:**
- Scraping de los barrios de Madrid
- Extracción de datos de propiedades
- Detección de cambios (nuevas, actualizadas, vendidas)
- Upsert directo a Neon Postgres
- Registro de costes (real, vía billing API) y duración por ejecución y por proveedor

**Tecnologías:**
- Python 3.x + BeautifulSoup4 + Requests
- Bright Data Web Unlocker (primario) · Oxylabs (fallback dormido)
- Neon Postgres vía `DATABASE_URL`

**Datos extraídos por propiedad:**
```python
{
    'listing_id': str,           # ID único de Idealista
    'title': str,                # Título del anuncio
    'url': str,                  # URL completa
    'price': int,                # Precio en €
    'distrito': str,             # Distrito de Madrid
    'barrio': str,               # Barrio específico
    'rooms': int,                # Número de habitaciones
    'size_sqm': float,           # Superficie en m²
    'floor': str,                # Planta
    'orientation': str,          # Interior/Exterior
    'has_lift': bool,            # Ascensor
    'is_exterior': bool,         # Exterior
    'seller_type': str,          # Particular/Agencia
    'is_new_development': bool,  # Obra nueva
    'description': str,          # Descripción parcial
}
```

---

### 2. Base de Datos (Neon Postgres)

**Producción:** Neon Postgres serverless, accedido vía `DATABASE_URL` con un
pool de conexiones psycopg (`db/connection_pg.py`). El scraper, el CI y el
dashboard escriben/leen **la misma BD** — desapareció el sync vía Google Drive.

**Compatibilidad SQLite (dev):** `db/connection.py` actúa de shim y cae a
SQLite local si no hay `DATABASE_URL`/`DB_BACKEND=postgres`. `db/dialect.py`
abstrae las diferencias de dialecto (`julianday_diff`, `iso_week`,
`week_start`, `date_offset_days`, `as_datetime`, `current_date`, …).

> ⚠️ **Deuda de migración:** quedan SQLite-isms residuales que rompen en
> silencio sobre Postgres. Ejecuta `verify_pg_queries.py` tras tocar SQL —
> barre todas las funciones de lectura contra Neon. Ver `ROADMAP.md` §1.

**Esquema (forma SQLite original; en Postgres los tipos son nativos:
`SERIAL`, `TIMESTAMP`, `BOOLEAN`, etc.):**

```sql
-- Tabla principal de propiedades
CREATE TABLE listings (
    listing_id TEXT PRIMARY KEY,
    title TEXT,
    url TEXT,
    price INTEGER,
    distrito TEXT,
    barrio TEXT,
    rooms INTEGER,
    size_sqm REAL,
    floor TEXT,
    orientation TEXT,
    has_lift BOOLEAN,
    is_exterior BOOLEAN,
    seller_type TEXT,
    is_new_development BOOLEAN,
    description TEXT,
    first_seen_date TEXT,
    last_seen_date TEXT,
    status TEXT DEFAULT 'active'   -- active | sold_removed
);

-- Historial de cambios de precio
CREATE TABLE price_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id TEXT,
    date TEXT,
    old_price INTEGER,
    new_price INTEGER,
    price_change INTEGER,          -- new_price - old_price
    FOREIGN KEY (listing_id) REFERENCES listings(listing_id)
);

-- Log de ejecuciones del scraper
CREATE TABLE scraping_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    start_time TEXT,
    end_time TEXT,
    duration_minutes REAL,
    properties_processed INTEGER,
    new_listings INTEGER,
    updated_listings INTEGER,
    sold_listings INTEGER,
    cost_estimate_usd REAL,
    status TEXT
);

-- Índices
CREATE INDEX idx_status       ON listings(status);
CREATE INDEX idx_distrito     ON listings(distrito);
CREATE INDEX idx_last_seen    ON listings(last_seen_date);
CREATE INDEX idx_first_seen   ON listings(first_seen_date);
CREATE INDEX idx_price        ON listings(price);
CREATE INDEX idx_ph_listing   ON price_history(listing_id);
CREATE INDEX idx_ph_date      ON price_history(date);
```

---

### 3. Dashboard Web (Streamlit Cloud)

**Orquestador:** `app.py` (287 líneas — thin orchestrator)

El `app.py` solo gestiona: autenticación, sidebar con filtros, carga de datos vía `data_utils.load_data()`, y routing a cada tab. Todo el rendering está delegado al paquete `tabs/`.

#### Estructura de navegación

`app.py` usa `st.navigation` (Streamlit ≥1.36): solo la página seleccionada
se ejecuta en cada run. Dos grupos en el sidebar:

```
app.py  →  st.navigation
├── 🏠 Caza
│   ├── 🎯 Mi Zona            → pages/mi_zona.py      (default)
│   ├── 🏆 Oportunidades      → pages/oportunidades.py
│   ├── 📉 Bajadas de Precio  → pages/bajadas.py
│   ├── 🔍 Búsqueda           → pages/busqueda.py
│   ├── 🔔 Mis Seguimientos   → pages/seguimientos.py
│   └── 🔎 Detalle de Anuncio → pages/detalle.py
└── ⚙️ Operaciones
    ├── ⚙️ Administración      → pages/admin.py
    └── 🛡️ Vigilancia          → pages/vigilancia.py
```

Cada `pages/*.py` es una vista fina que delega el render al paquete `tabs/`
(`mi_zona_tab`, `opportunities_tab`, `price_drops_tab`, `search_tab`,
`watchlist_tab`, `detail_tab`, `admin_tab`, `alerts_tab`).

#### 🎯 Mi Zona (`pages/mi_zona.py` → `mi_zona_tab`)

- Criterios guardados por usuario (`.streamlit/userpref_*`): barrios, precio, m², etc.
- Nuevas propiedades en tus barrios que pasan el umbral de margen de oferta
- Mismo motor que el cron `mi_zona_alerts.py` (email diario)

#### 🏆 Oportunidades (`pages/oportunidades.py` → `opportunities_tab`)

- Top propiedades por score calidad-precio (combina precio/m² vs barrio + señales NLP)
- Detección de vendedor desesperado / gangas vs distrito

#### 📉 Bajadas de Precio (`pages/bajadas.py` → `price_drops_tab`)

- Overview de reducciones, ranking por barrio, bajadas recientes, histograma de magnitud
- Heatmap semanal €/m² por distrito

#### 🔍 Búsqueda (`pages/busqueda.py` → `search_tab`)

- Búsquedas personalizadas con guardado y seguimiento de evolución de precios

#### 🔔 Mis Seguimientos (`pages/seguimientos.py` → `watchlist_tab` + `alerts_tab`)

- Watchlist de propiedades + alertas del usuario

#### 🔎 Detalle de Anuncio (`pages/detalle.py` → `detail_tab`)

- Histórico de precios (gráfico + KPIs), sugerencia de oferta, **vista rica de comparables**
- Modelo predictivo (RF) con intervalo por percentiles de árboles

#### ⚙️ Administración (`pages/admin.py` → `admin_tab`)

- Actividad de scraping, control de costes (coste real Bright Data), subsección **Proveedores**
- Propiedades nuevas por distrito/fecha, buscador por URL/ID con historial, purga de stale

#### 🛡️ Vigilancia (`pages/vigilancia.py`)

- Semáforo de mercado 0-100, indicadores internos + macro, alertas y diagnóstico

---

### 4. Panel de Vigilancia del Mercado

**Archivo:** `market_surveillance.py` (894 líneas)

Página independiente que combina indicadores internos (calculados sobre la BD) con datos macroeconómicos externos.

#### Score de Salud del Mercado

Índice 0-100 calculado como media ponderada de 7 componentes:

| Componente | Peso | Qué mide | Fuente |
|---|---|---|---|
| Tendencia de precios | 25% | Variación % semanal del precio mediano | BD interna |
| Velocidad de ventas | 20% | Días medianos hasta venta/retirada | BD interna |
| Ratio oferta/demanda | 15% | Nuevas publicaciones / vendidas (semanal) | BD interna |
| Asequibilidad | 15% | Cuota hipotecaria / ingreso de referencia | BD + Euríbor |
| Euríbor + tendencia | 10% | Nivel actual ± ajuste por tendencia (±5 pts) | BCE |
| Estrés vendedor | 10% | % activos con ≥1 bajada en 30 días | BD interna |
| Desempleo | 5% | Tasa de paro EPA | INE |

**Interpretación:**
- 🟢 75-100 → **ALCISTA**: demanda sólida, vendedores con poder
- 🟡 40-74 → **EN TRANSICIÓN**: señales mixtas
- 🔴 0-39 → **BAJISTA**: demanda débil, estrés vendedor generalizado

#### Indicadores Internos (`market_indicators.py`)

| Función | Qué calcula |
|---------|-------------|
| `get_weekly_price_evolution()` | Serie semanal de precio mediano con breakpoint detection |
| `get_weekly_sales_speed()` | Días medianos en mercado de propiedades vendidas |
| `get_supply_demand_ratio()` | Ratio nuevas/vendidas semanal (cap en 10x) |
| `get_inventory_evolution()` | Evolución del stock activo |
| `get_rotation_rate()` | % de rotación rolling 4 semanas |
| `get_price_dispersion()` | Diferencia media/mediana como proxy de outliers |
| `get_affordability_index()` | Cuota hipotecaria (80% LTV, 25 años, Euríbor+spread) |
| `get_price_drop_ratio()` | % activos con bajada en 30 días + profundidad media |
| `get_price_by_zone()` | Precio mediano por distrito/barrio |
| `get_sales_speed_by_zone()` | Velocidad de venta por distrito/barrio |
| `get_market_alerts()` | Lista de alertas por nivel (critical/warning/info) |
| `calculate_market_score()` | Score compuesto 0-100 con 7 componentes |

#### Datos Macroeconómicos (`macro_data.py`)

| Indicador | Fuente | Frecuencia |
|-----------|--------|------------|
| Euríbor 12M | BCE API | Mensual |
| Tasa de paro EPA | INE API | Trimestral |

---

### 5. Modelo Predictivo (`predictive_model.py`)

- **Algoritmo:** Random Forest Regressor (scikit-learn Pipeline)
- **Features:** distrito, barrio, m², habitaciones, planta, ascensor, exterior
- **Validación:** Cross-validation k-fold
- **Output:** precio central + percentiles P10/P90 de los árboles individuales
- **Reentrenamiento automático:** cuando los datos son más recientes que el modelo
- **Métricas persistidas:** `model_metadata.json` (R², MAE, RMSE, MAPE, fecha, importancias)

---

## Arquitectura de Datos

### Flujo de Datos

```
1. SCRAPING (GitHub Actions, diario)
   Idealista.com → Bright Data (1º) / Oxylabs (fallback) → parsing
   → upsert directo a Neon Postgres
   ↓
2. PRE-CÓMPUTO (mismo run de CI)
   compute_snapshots.py → tabla market_snapshots
   export_public_metrics.py → metrics.json (consumido por el front público)
   ↓
3. DASHBOARD (Streamlit Cloud, bajo demanda)
   Neon Postgres → psycopg pool → Streamlit cache → visualización
```

### Detección de Cambios en el Scraper

En cada ejecución el scraper:
1. Obtiene todos los `listing_id` activos de la BD
2. Scrapea todos los barrios configurados en `barrios_urls.csv`
3. Compara: nuevos → `INSERT`, precio cambiado → `UPDATE` + registro en `price_history`, no vistos hoy → `mark_as_sold`
4. Registra la ejecución en `scraping_log` con coste estimado y duración

### Semana ISO en SQL

Las consultas semanales usan los helpers `iso_week()` / `week_start()` de
`db/dialect.py` (que emiten `strftime('%Y-%W', …)` en SQLite y `TO_CHAR` /
`date_trunc('week', …)` en Postgres) para evitar el bug de agrupación
cross-year (semana 01 de 2025 vs 2026).

---

## Flujo de Operación

El ciclo es **automático vía GitHub Actions** — no hay pasos manuales de
scraping ni de subida de BD.

```
daily_scraper.yml (cron diario)
  1. scraper.py            → upsert a Neon (Bright Data 1º, Oxylabs fallback)
  2. compute_snapshots.py  → market_snapshots
  3. email_report.py       → resumen diario
  4. export_public_metrics → metrics.json
  5. health-check          → falla el run si no se procesó suficiente data

export-metrics.yml (lun 07:00 UTC)  → regenera metrics.json + barrios_profiles
mi_zona_alerts.yml  (diario 07:00)   → email con matches de Mi Zona
```

El operador solo revisa el panel ⚙️ Administración (días bajos, coste,
proveedores) y el score de 🛡️ Vigilancia.

---

## Despliegue

### Scraping / CI (GitHub Actions)

Variables como secrets del repo (no en `.env` versionado):

```
DATABASE_URL          # Neon Postgres
BRIGHTDATA_*          # credenciales del proveedor primario
OXYLABS_*             # fallback (dormido)
SCRAPE_MODE=lite      # newest-first + early-stop entre semana
```

### Streamlit Cloud (Dashboard)

- **Repositorio:** `github.com/beastinblackcode/inmobiliario`
- **Branch:** `main` · **Main file:** `app.py`
- **Auto-deploy** en cada push a `main`

**Secrets necesarios (`.streamlit/secrets.toml`):**
```toml
[postgres]
url = "postgresql://…@…neon.tech/…?sslmode=require"

[auth.users_hashed]   # bcrypt — generados con gen_password_hash.py
admin = "$2b$12$…"
luis  = "$2b$12$…"
```

`app.py` detecta el bloque `[postgres]` y fija `DB_BACKEND=postgres`
automáticamente.

---

## Seguridad

| Aspecto | Implementación |
|---------|---------------|
| Autenticación | bcrypt (`auth.py`): hashes en `st.secrets["auth"]["users_hashed"]`, verificación en tiempo constante |
| Rate limiting | 5 intentos fallidos → bloqueo de 5 min por sesión |
| Sesión | Expiry + audit log de accesos |
| HTTPS | Automático en Streamlit Cloud / Vercel |
| Credenciales (Bright Data, Neon) | Solo en secrets de CI / Streamlit (nunca en git) |
| Indexación bots | `public/robots.txt` con `Disallow: /` |

---

## Costes y Escalabilidad

### Costes Actuales

| Servicio | Coste |
|----------|-------|
| Bright Data | coste real reportado desde la billing API (`get_brightdata_cost`); la estimación interna del scraper subestima ~6×, no usar como referencia |
| Streamlit Cloud | Gratis (Community tier) |
| Neon Postgres | Free tier (con cold starts; el pool está tuneado para ello) |
| Vercel (front público) | Gratis (Hobby) |

### Escalabilidad

| Dimensión | Estado actual | Límite práctico |
|-----------|--------------|-----------------|
| Listings activos | ~17-20 k | Sin límite relevante |
| RAM Streamlit | <200 MB | ~1 GB disponible |
| Concurrencia | Pool tuneado para Neon free-tier | Subir plan Neon si crece |

> Para detalle de roadmap, deuda técnica y próximos pasos, ver
> [`ROADMAP.md`](ROADMAP.md) — este documento describe el sistema **tal como
> es hoy**, no lo que falta.
