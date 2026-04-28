# Arquitectura del Sistema: Madrid Real Estate Tracker

> **Última actualización:** abril 2026

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
└────────────────────────┬────────────────────────────────────┘
                         │ HTTPS
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              STREAMLIT CLOUD (Dashboard Web)                 │
│                                                             │
│  app.py ──► tabs/dashboard_tab.py                           │
│         ├──► tabs/map_tab.py                                │
│         ├──► tabs/prediction_tab.py                         │
│         ├──► tabs/search_tab.py                             │
│         ├──► tabs/admin_tab.py                              │
│         └──► market_surveillance.py                         │
│                      │                                      │
│              database.py / data_utils.py                    │
│                      │                                      │
│           analytics.py · market_indicators.py               │
│           macro_data.py · predictive_model.py               │
└──────────────────────┬──────────────────────────────────────┘
                       │ Descarga DB al iniciar
                       ▼
             ┌─────────────────────┐
             │   GOOGLE DRIVE      │
             │  real_estate.db     │
             │  (~16 MB SQLite)    │
             └─────────────────────┘
                       ▲
                       │ Upload tras scraping
┌──────────────────────┴──────────────────────────────────────┐
│              MÁQUINA LOCAL (Scraping)                        │
│                                                             │
│   scraper.py ──► database.py ──► real_estate.db             │
│   retry_scraper.py                                          │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
             ┌─────────────────────┐      ┌─────────────────┐
             │   BRIGHT DATA       │      │  BCE / INE      │
             │  Web Unlocker Proxy │      │  (APIs macro)   │
             └─────────────────────┘      └─────────────────┘
                       │
                       ▼
             ┌─────────────────────┐
             │   IDEALISTA.COM     │
             └─────────────────────┘
```

---

## Mapa de Archivos del Proyecto

### Núcleo del Dashboard

| Archivo | Líneas | Función |
|---------|--------|---------|
| `app.py` | 287 | Orquestador: auth, sidebar, carga de datos, routing de tabs |
| `data_utils.py` | 43 | `load_data()` con `@st.cache_data` compartido entre tabs |
| `database.py` | 1,209 | Toda la capa de acceso a datos (CRUD, stats, historial) |
| `analytics.py` | 559 | Análisis avanzado: chollos, velocidad, evolución de propiedades |

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
| `market_surveillance.py` | 894 | Página completa de vigilancia: semáforo, KPIs, alertas, diagnóstico |
| `market_indicators.py` | 1,530 | Cálculo de todos los indicadores internos y score de mercado |
| `macro_data.py` | 569 | Datos macroeconómicos: Euríbor (BCE), desempleo (INE) |

### Modelo Predictivo

| Archivo | Función |
|---------|---------|
| `predictive_model.py` | Random Forest para valuación de propiedades |
| `model_metadata.json` | Metadatos del modelo entrenado (generado en runtime) |

### Scraper y pipeline diario

| Archivo | Función |
|---------|---------|
| `scraper.py` | Scraping principal de 139 barrios vía Bright Data Web Unlocker |
| `retry_scraper.py` | Reintento de barrios fallidos |
| `compute_snapshots.py` | Pre-cálculo de KPIs diarios → tabla `market_snapshots` |
| `nlp_analyzer.py` | Extracción de señales NLP de descripciones (urgencia, directo, negociable) |
| `email_report.py` | Resumen diario por email |
| `tweet_daily.py` | Tweet automático diario con headline del mercado |
| `export_public_metrics.py` | Genera `metrics.json` para el frontend público |
| `ci_drive_upload.py` / `upload_to_drive.py` | Sube `real_estate.db` a Google Drive |
| `migration_backfill_initial_history.py` | Backfill idempotente de `price_history` (corre en CI) |

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
| `test_description.py` | Tests del scraper |
| `test_sold_logic.py` | Tests de la lógica de vendidos |

### Archivos de Configuración

| Archivo | Contenido |
|---------|-----------|
| `.env` | Credenciales Bright Data (local, no en git) |
| `.env.example` | Plantilla de variables de entorno |
| `.streamlit/secrets.toml` | Secrets de Streamlit Cloud (no en git) |
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

### 1. Scraper (Ejecución Local)

**Archivo:** `scraper.py` (1,009 líneas)

**Responsabilidades:**
- Scraping de 184 barrios de Madrid
- Extracción de datos de propiedades
- Detección de cambios (nuevas, actualizadas, vendidas)
- Actualización de base de datos local
- Registro de costes y duración por ejecución

**Tecnologías:**
- Python 3.x + BeautifulSoup4 + Requests
- Bright Data Web Unlocker (proxy anti-bot)
- SQLite (base de datos local)

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

### 2. Base de Datos (SQLite)

**Archivo activo:** `real_estate.db` (~16 MB)

**Esquema completo:**

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

```
app.py
├── Página: 🏠 Dashboard Principal
│   ├── 📊 Dashboard     → tabs/dashboard_tab.py
│   ├── 🗺️ Mapa          → tabs/map_tab.py
│   ├── 🔮 Predicción    → tabs/prediction_tab.py
│   ├── 🔍 Mis Búsquedas → tabs/search_tab.py
│   └── ⚙️ Administración → tabs/admin_tab.py
└── Página: 🛡️ Vigilancia del Mercado → market_surveillance.py
```

#### Tab: 📊 Dashboard (`tabs/dashboard_tab.py`)

- KPIs principales: precio mediano, €/m², activos, vendidos 30d
- Evolución de bajadas de precio diarias
- Top barrios por precio/m²
- Distribución por tipo de vendedor (pie chart)
- Precio por distrito (grouped bar)
- Evolución del precio/m² semanal con filtros
- Analytics avanzado: velocidad de venta, oportunidades, chollos
- Tiempo en mercado por distrito
- Zonas con bajadas de precio
- Historial: bajadas, evolución de propiedades, vendedores desesperados

#### Tab: 🗺️ Mapa (`tabs/map_tab.py`)

- Mapa Folium interactivo con marcadores coloreados por precio
- Heat layer de intensidad de precios
- Selector de límite de propiedades (100/500/1000/Todos)
- Solo muestra propiedades activas con coordenadas disponibles

#### Tab: 🔮 Predicción (`tabs/prediction_tab.py`)

- Modelo Random Forest entrenado sobre propiedades activas
- Inputs: distrito, barrio, m², habitaciones, planta, ascensor, exterior
- Output: precio estimado + intervalo P10-P90 por percentiles de árboles
- Métricas de rendimiento: R², MAE, RMSE, MAPE
- Importancia de variables
- Comparación con precio medio de la zona

#### Tab: 🔍 Mis Búsquedas (`tabs/search_tab.py`)

- Búsqueda fija: activos 250k-450k€, ≥40m², con ascensor, sin bajos
- Distritos: Centro, Chamberí, Retiro, Salamanca, Chamartín, etc.
- Seguimiento de evolución de precios de los resultados
- Tabla de bajadas de precio recientes

#### Tab: ⚙️ Administración (`tabs/admin_tab.py`)

- Actividad de scraping (30 días): daily bar chart con alertas de días bajos
- Control de costes: coste por ejecución y duración (últimas 30 ejecuciones)
- Propiedades nuevas por distrito y fecha (tabla pivote / lista detallada)
- Buscador de propiedad por URL o ID con historial completo de precios

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
1. SCRAPING (Local, diario)
   Idealista.com → Bright Data proxy → BeautifulSoup → real_estate.db (local)
   ↓
2. UPLOAD (Manual tras scraping)
   real_estate.db → Google Drive (File ID: 1ajdgLaneXwb6OWl_S727gwyYZUfrdF7p)
   ↓
3. DASHBOARD (Streamlit Cloud, bajo demanda)
   Google Drive → download → Streamlit cache → visualización
```

### Detección de Cambios en el Scraper

En cada ejecución el scraper:
1. Obtiene todos los `listing_id` activos de la BD
2. Scrapea todos los barrios configurados en `barrios_urls.csv`
3. Compara: nuevos → `INSERT`, precio cambiado → `UPDATE` + registro en `price_history`, no vistos hoy → `mark_as_sold`
4. Registra la ejecución en `scraping_log` con coste estimado y duración

### Semana ISO en SQL

Todas las consultas semanales usan `strftime('%Y-%W', date)` para evitar el bug de agrupación cross-year (semana 01 de 2025 vs 2026).

---

## Flujo de Operación

### Ciclo Diario

```
[ ] 1. Ejecutar scraper (2-4h)
       cd ~/inmobiliario && source venv/bin/activate && python scraper.py

[ ] 2. Verificar logs y días bajos en pestaña ⚙️ Administración

[ ] 3. Subir real_estate.db a Google Drive (2 min)
       - Opción manual: Drive → Gestionar versiones → Subir nueva versión
       - Opción CLI: gdrive update 1ajdgLaneXwb6OWl_S727gwyYZUfrdF7p real_estate.db

[ ] 4. (Opcional) Reboot app en Streamlit Cloud si los datos no se actualizan

[ ] 5. Verificar métricas en dashboard y score de vigilancia
```

### Frecuencia Recomendada

| Actividad | Frecuencia | Duración |
|-----------|------------|----------|
| Scraping completo | Diario (noche) | 2-4h |
| Upload a Drive | Después de cada scraping | 2-5 min |
| Revisión del score | Semanal | — |

---

## Despliegue

### Entorno Local (Scraping)

```bash
cd ~/inmobiliario
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Variables en .env
BRIGHTDATA_USER=tu_usuario
BRIGHTDATA_PASS=tu_contraseña
BRIGHTDATA_HOST=brd.superproxy.io:33335
```

### Streamlit Cloud (Dashboard)

- **Repositorio:** `github.com/beastinblackcode/inmobiliario`
- **Branch:** `main` · **Main file:** `app.py`
- **Auto-deploy** en cada push a `main`

**Secrets necesarios (`.streamlit/secrets.toml`):**
```toml
[database]
google_drive_file_id = "1ajdgLaneXwb6OWl_S727gwyYZUfrdF7p"

[auth.users]
admin = "ContraseñaAdmin"
luis  = "ContraseñaLuis"
```

---

## Seguridad

| Aspecto | Implementación |
|---------|---------------|
| Autenticación | Username + password en Streamlit Secrets |
| Multi-usuario | Credenciales individuales por usuario |
| HTTPS | Automático en Streamlit Cloud |
| Credenciales Bright Data | Solo en `.env` local (en `.gitignore`) |
| DB solo lectura | La DB de Google Drive es pública pero read-only |
| Indexación bots | `public/robots.txt` con `Disallow: /` |

---

## Costes y Escalabilidad

### Costes Actuales

| Servicio | Coste |
|----------|-------|
| Bright Data | ~$0.02-0.04 por ejecución (post-optimización) |
| Streamlit Cloud | Gratis (Community tier) |
| Google Drive | Gratis (15 GB incluidos; DB actual ~16 MB) |

> La optimización del scraper redujo el coste por ejecución de ~$20 a menos de $0.05 mediante uso de API JSON en lugar de parsing HTML completo donde es posible.

### Escalabilidad

| Dimensión | Estado actual | Límite práctico |
|-----------|--------------|-----------------|
| Listings activos | ~20,000 | Sin límite relevante |
| Tamaño DB | ~16 MB | SQLite aguanta hasta ~140 TB |
| RAM Streamlit | <200 MB | ~1 GB disponible |
| Proyección 5 años | ~250 MB | Sin problema |

---

## Mejoras Futuras Pendientes

| Mejora | Prioridad | Complejidad |
|--------|-----------|-------------|
| Indicador de precio de alquiler en score | Alta | Media (requiere nueva fuente) |
| Indicador de rentabilidad bruta (yield) | Alta | Baja (precio alquiler / precio compra) |
| Visados de obra nueva (INE trimestral) | Media | Media |
| Automatización del upload a Drive (gdrive CLI) | Media | Baja |
| Notificaciones Telegram / email por alertas | Media | Media |
| Migración a PostgreSQL (eliminación upload manual) | Baja | Alta |
| GitHub Actions para scraping automático | Baja | Media |
