# Roadmap — Madrid Real Estate Tracker

> **Última actualización:** junio 2026
> **Alcance:** este documento consolida el roadmap funcional, técnico y arquitectónico del proyecto. Reemplaza a `NUEVA_ARQUITECTURA.MD`, `PROPUESTA_ARQUITECTURA.md`, `Analisis_Funcional.docx`, `Propuestas_Funcionalidades_Metricas.docx` y `specs-nuevas-features-front.docx`.

---

## 1. Estado actual (junio 2026)

### Lo que ya funciona en producción

| Área | Implementado |
|---|---|
| Scraping | **Bright Data primario + Oxylabs fallback dormido** (registro por proveedor en `provider_config`/`scraping_log_provider`), recorrido por barrios, retry+backoff, circuit breaker en `mark_stale_as_sold`, reactivación de listings falsamente marcados, budget cap, **coste real desde la billing API de Bright Data**, modo `SCRAPE_MODE=lite` (newest-first + early-stop), scraping de alquiler tras flag `RENTAL_SCRAPE_ENABLED` |
| Pipeline diario | GitHub Action `daily_scraper.yml` hablando **directo a Neon Postgres**: scraper → `compute_snapshots` → email → export metrics → health-check. Sin descarga/subida de DB. |
| Calidad de datos | Auditoría cerrada al 100% (purga stale, mediana+IQR en trend, filtro warmup en velocidad de venta, gap-guard en tendencias semanales para el hueco feb-may 2026) |
| Frontend interno | Streamlit multipage (`st.navigation`, 8 páginas en `pages/`): 🏠 Caza (`mi_zona`, `oportunidades`, `bajadas`, `busqueda`, `seguimientos`, `detalle`) · ⚙️ Operaciones (`admin`, `vigilancia`). Detalle con vista rica de comparables. |
| Frontend público | Next.js 14 (`market-thermometer/`) en madridhome.tech con `metrics.json` regenerado por CI, ISR y i18n (es/en) |
| Análisis | Score calidad-precio (NLP de descripciones: urgencia, estado, certificación energética, año), score de negociabilidad, detección vendedor desesperado, gangas vs distrito, **desglose de recortes activos por distrito** (`get_district_repricing_breakdown`: % del stock activo bajando precio + recorte medio € en ventana móvil) |
| ML | Random Forest con OneHotEncoder + intervalo por percentiles de árboles, reentrenamiento automático cuando los datos son más recientes que el modelo |
| Vigilancia macro | Indicadores internos (incluidos **Absorption Rate, Months of Supply, Rental Yield, notarial gap, lanzamientos CGPJ, morosidad, rent burden**) + macro (BCE Euríbor, INE paro) → market score 0-100 |
| Auth | Multi-usuario con **bcrypt + rate-limit (5 intentos→5min) + expiry de sesión + audit log** (`auth.py`) |
| Persistencia | **Neon Postgres** con pool de conexiones (`db/connection_pg.py`); shim dialecto en `db/dialect.py` para compatibilidad SQLite en dev |
| Tests | **~305 tests** en `tests/` (unit + integration + regression), `conftest.py` con fixtures de BD |

### Limitaciones estructurales conocidas

- **🔴 Deuda de migración Postgres (riesgo #1)**: la migración SQLite→Postgres (mayo 2026) dejó SQL incompatible disperso (SQLite-isms) que rompe **en silencio** — funciones que envuelven su query en `except Exception`, imprimen el error y devuelven vacío. Las últimas semanas han sido fixes uno a uno (`ROUND(double,int)`, `strptime`, `date()`, alias en `HAVING`, columnas fuera de `GROUP BY`, conns no devueltas al pool). **`verify_pg_queries.py`** barre todas las funciones de lectura contra Neon para cerrar esto de raíz.
- **Errores silenciosos** (✅ mitigado, junio 2026): los `except Exception` genéricos enmascaraban fallos con `print()` en lugar de `logging`. Convertidos a `logger.exception` (traceback a ERROR) en `database.py`, `market_indicators.py`, `macro_data.py` (commit `7e1076a`) vía `logging_config.get_logger`. Además el CLI del email diario (`email_report.py`) ahora sale con código ≠0 al fallar, así el step del workflow se pone en rojo en vez de terminar verde sobre un email fallido. Queda residual: prints en scripts CLI sueltos (`fetch_opendata`, `retry_scraper`) — intencionales, no errores tragados.
- **`database.py` god-module**: ~3.640 líneas mezclan infraestructura, CRUD, lógica de negocio y utilidades de UI.
- **Modelo predictivo**: sin validación cruzada visible al usuario, sin métricas de rendimiento expuestas en cada predicción, sin reentrenamiento programado (solo on-demand al detectar datos frescos).
- **Frecuencia de scraping**: cadencia diaria con modo `lite` entre semana; `days_to_sell` mantiene un suelo estructural por la combinación cadencia + threshold stale.
- **Streamlit full-rerun**: mitigado con multipage (`st.navigation`) + `@st.fragment`, pero cada interacción dentro de una página re-ejecuta esa página.

---

## 2. Roadmap funcional (qué construir)

Priorización: **🔥 Alta · ⭐ Media · 💤 Baja** · esfuerzo en horas/días.

### 2.1 Métricas adicionales

| Métrica | Prio | Esfuerzo | Comentario |
|---|---|---|---|
| ✅ ~~**Absorption Rate**~~ | — | — | **Hecho** — `market_indicators.get_absorption_rate`. |
| ✅ ~~**Months of Supply**~~ | — | — | **Hecho** — `market_indicators.get_months_of_supply`. |
| ✅ ~~**Score de Negociabilidad**~~ | — | — | **Hecho** — `analytics.calculate_negotiability_score` (0-100): días_mercado 35 + bajadas 30 + gap vs mediana distrito 20 + seller_type 15. Cableado en Oportunidades (badges/expander/filtros) y en `offer_engine`. Verificado contra Neon: distribución 4-94 sobre 16.4k activos. |
| **Yield bruto por alquiler** | ⭐ | Alto | Parcialmente hecho (`get_rental_yield` + `rental_prices`). Falta scraper de alquileres robusto (hoy tras flag `RENTAL_SCRAPE_ENABLED`, desactivado). |
| ✅ ~~**Price Pressure Index**~~ | — | — | **Hecho** — `market_indicators.get_price_pressure_index` (`(% subidas − % bajadas) × velocidad`, factor de absorción 0.5–2.0). Cableado en `get_all_internal_indicators` (reusa la absorción ya calculada) y en la página de Vigilancia (KPI #15). Verificado contra Neon: −25.6 (subidas 0.9% vs bajadas 13.7%, absorción alta → presión a la baja). Autocubierto por `verify_pg_queries.py` (62 ok). |
| ✅ ~~**Coeficiente de Gini de precios**~~ | — | — | **Hecho** — `market_indicators.get_price_gini`: `G = Σ(2i−n−1)·x_i / (n·Σx_i)` sobre precios de activos (mismo origen que la dispersión). Más robusto que el gap media/mediana ante outliers de lujo; al alza = polarización/gentrificación. Cableado en `get_all_internal_indicators` y en Vigilancia (KPI #16). Tests en `tests/integration/test_price_gini.py` (valor exacto 0.170 sobre la semilla). Autocubierto por `verify_pg_queries.py`. |
| ✅ ~~**Volatilidad móvil** (std en ventanas 7d/30d)~~ | — | — | **Hecho** — `market_indicators.get_price_volatility`: coef. de variación (std÷media×100) del €/m² mediano diario en ventanas 7d/30d, leyendo la serie `market_snapshots` (city, `median_price_sqm`) vía `get_snapshot_series` (sin query pesada extra). 7d ≫ 30d → turbulencia (señal adelantada de cambio de tendencia); 7d ≪ 30d → se calma. Cableado en `get_all_internal_indicators` y en Vigilancia (KPI #17). Tests en `tests/integration/test_price_volatility.py` (CV exacto sobre serie sembrada). Autocubierto por `verify_pg_queries.py`. |
| **Ratio precio pedido vs vendido** | 💤 | 6h | Aproximación al descuento de negociación. Limitación: no conocemos precio final real. |

### 2.2 Funcionalidades de usuario

| Feature | Prio | Esfuerzo | Comentario |
|---|---|---|---|
| **Alertas por email para usuarios del front público** | 🔥 | 1-2 semanas | El motor interno ya existe (`alerts_tab.py`). Falta exponerlo en madridhome.tech con email + criterios básicos. Convierte el dashboard público en un producto con retención. |
| ✅ ~~**Comparador de propiedades** (2-4 lado a lado)~~ | — | — | **Hecho** — página ⚖️ Comparador (`pages/comparador.py` + `tabs/compare_tab.py`): multiselect (2-4, filtrable por sidebar o por seguimientos), tabla lado a lado (precio, €/m², vs mediana barrio, m², hab., planta, días, vendedor, enlace), radar normalizado orientado a comprador-residente (más barato/grande/habitaciones/margen = mejor) y mini-mapa por centroide de barrio. Lógica pura testeada (`tests/unit/test_compare.py`). |
| ✅ ~~**Perfil de barrio inteligente**~~ | — | — | **Hecho** — página 🏘️ Perfil de Barrio (`pages/barrio.py` + `tabs/barrio_profile_tab.py`) sobre el builder ya existente `barrio_profiles.build_all_barrio_profiles` (cacheado): veredicto determinista (tensionado/infravalorado/equilibrio + modificador caliente/bajista), KPIs (€/m², precio, días, vs distrito/Madrid, % con bajadas), distribución (precio/habitaciones/amenities/año), top oportunidades del barrio y barrios vecinos comparables. Verificado contra Neon (127 barrios). Lógica de veredicto/vecinos ya testeada en `tests/unit/test_barrio_profiles.py`. |
| **Detección de anomalías** | ⭐ | 4 días | Isolation Forest o Z-scores por barrio para flagear chollos / errores / sobreprecios. |
| **Predicción probabilidad venta 30d** | ⭐ | 1-2 semanas | Modelo de clasificación binaria entrenado con `sold_removed`. Mostrar % en cada ficha. |
| **Informes PDF/DOCX automatizados** | ⭐ | 1 semana | Semanal, mensual, por distrito. Cron + reportlab/python-docx. |
| **Heatmap temporal animado** | ⭐ | 4 días | Slider que avanza semana a semana sobre el mapa de calor. |
| ✅ ~~**Exportación CSV/Excel** desde el dashboard~~ | — | — | **Hecho** — botones de descarga (CSV `utf-8-sig` + Excel vía openpyxl) en la pestaña 🔍 Búsquedas, exportan los listings filtrados con columnas renombradas. |
| ✅ ~~**Mapa de recortes activos por distrito**~~ | — | — | **Hecho** — `market_indicators.get_district_repricing_breakdown` + tabla "🗺️ Recortes Activos por Distrito" en Oportunidades: % del stock activo bajando precio, recorte medio €, ratio bajan:suben y €/m² mediano en ventana móvil (solo activos, guard de muestra `min_active`). 3ª lente de oportunidad junto al score de negociabilidad (margen estructural) y el ahorro absoluto €. Verificado contra Neon (Arganzuela 23% lidera en frecuencia; Retiro −€73k en euros). |
| **i18n del Streamlit interno** | 💤 | 3-4 días | Centralizar textos. Hoy mezcla es/en. |
| **Comparativa de distritos** | 💤 | 3 días | Radar chart superpuesto. |

### 2.3 Mejoras de Machine Learning

| Mejora | Prio | Esfuerzo |
|---|---|---|
| Features NLP de descripciones — ✅ estado, certificación energética y año **hechos** (`nlp_analyzer.py`); faltan terraza/garaje/trastero | ⭐ | 2 días |
| ✅ ~~**Features adicionales al RF**: precio/m² mediano del barrio, distancia a Sol, densidad de oferta, velocidad del barrio~~ | — | — | **Hecho** — `predictive_model.PricePredictor` añade 4 features de contexto de barrio (`barrio_median_sqm`, `barrio_dist_sol` vía `coordinates.distance_to_sol`/haversine, `barrio_supply`, `barrio_velocity`), aprendidas en `train()` y persistidas para enriquecer `predict()` desde atributos crudos. A/B limpio sobre 42.767 activos+vendidos: R² 0.905→**0.915**, MAPE 16.8→**16.2%**, MAE −€5.270; `barrio_median_sqm` ya es la 2ª feature (imp. 0.18). Tests en `tests/unit/test_predictive_features.py`. Nota: el agregado de barrio se computa sobre todo el dataset (leve sesgo optimista en CV); un pipeline 100% leak-proof lo metería dentro de cada fold. |
| **Modelo AVM con comparables**: 5 propiedades más similares + ajustes por característica + intervalo real | ⭐ | 1-2 semanas |
| **Quantile Regression Forest** (intervalos de confianza reales en vez de ±10% heurístico) | ⭐ | 4 días |
| **Métricas de rendimiento visibles** (R², MAE, MAPE) + reentrenamiento periódico | ⭐ | 3 días |
| **Series temporales (Prophet/ARIMA)** para predecir tendencia de precio/m² por distrito | 💤 | 1 semana |

### 2.4 Plataforma

| Item | Prio | Esfuerzo | Comentario |
|---|---|---|---|
| ✅ ~~**Tests automatizados con pytest**~~ | — | — | **Hecho** — ~305 tests en `tests/` (unit + integration + regression) con `conftest.py`. Falta ampliar cobertura de las queries Postgres (ver `verify_pg_queries.py`, candidato a meterse en CI). |
| ✅ ~~**Logging estructurado** (módulo `logging`, niveles, sustituir prints)~~ | — | — | **Hecho** — `logging_config.get_logger` (setup idempotente, `LOG_LEVEL` env). Los `except Exception: print(error)` de las funciones de librería pasan a `logger.exception` (traceback a ERROR) en `database.py`/`market_indicators.py`/`macro_data.py` (commit `7e1076a`). El email diario sale ≠0 al fallar → step en rojo en vez de verde silencioso. Pendiente opcional: enrutar logs a un sink agregado (hoy van a stdout/CI). |
| ✅ ~~**`verify_pg_queries.py` en CI**~~ | — | — | **Hecho** — job `pg-query-sweep` en `tests.yml`: postgres:16 efímero + `alembic upgrade head` + barrido (`--seed`). 61 ok / 0 fallos, verificado en CI real. Un SQLite-ism nuevo ahora rompe el build. |
| **API REST pública** (FastAPI) | 💤 | 2-3 semanas | Ver Fase 2 del roadmap arquitectónico (sección 3). |
| **Scraping multi-portal** (Fotocasa, Habitaclia, pisos.com) | 💤 | 4-6 semanas | Requiere deduplicación cross-portal, campo `source`, scraper abstracto por portal. |

---

## 3. Roadmap arquitectónico

Tres fases incrementales. Cada fase es autónoma y aporta valor por sí misma. **Fase 1 ya parcialmente hecha** — quedan los items marcados ⏳.

### Fase 1 — Optimización dentro de Streamlit

**Estado:** ✅ esencialmente completa. Implementada en commit `dc5a3cf` (12 marzo 2026, "perf: optimización completa de rendimiento Streamlit (Tareas 1-5)").
**Impacto medido:** latencia de 2-5s → 0.5-1.5s por interacción.

| Item | Estado |
|---|---|
| Conexión singleton por thread (`db/connection.py`) | ✅ Hecho |
| Índices compuestos (`status+distrito+price`, `status+barrio+price`, `price_history(listing_id, date_recorded)`, `status+last_seen_date DESC`) | ✅ Hecho — en `init_database()`, aplicados en cada run del scraper |
| `get_listings_page()` con paginación + proyección SQL (`price_per_sqm`, `days_on_market`) | ✅ Hecho (database.py:822) |
| Multipage app (`st.navigation`, 11 páginas en `pages/`) | ✅ Hecho — eliminado el hack JS de polling de tabs |
| `compute_snapshots.py` + tabla `market_snapshots` + step CI | ✅ Hecho |
| `@st.fragment` en `search_tab`, `alerts_tab`, `detail_tab`, sección €/m² del dashboard | ✅ Hecho |
| Threshold `mark_stale_as_sold` 7d → 14d para alinear con downstream (compute_snapshots, market_indicators) | ✅ Hecho (abril 2026) |

### Fase 2 — Capa API (FastAPI)

**Cuándo:** cuando aparezca un segundo consumidor de los datos (app móvil, otro equipo, integración externa). Hasta entonces es over-engineering.

**Estructura objetivo:**

```
api/        → FastAPI app, routers, schemas Pydantic
services/   → lógica de negocio pura (analytics, prediction, market, nlp)
repositories/ → SQL queries tipadas, conexión singleton
models/     → dataclasses de dominio
frontend/   → Streamlit como cliente HTTP de la API
migrations/ → SQL versionado
```

**Endpoints propuestos:** `GET /api/v1/listings`, `GET /api/v1/listings/{id}/price-history`, `GET /api/v1/analytics/kpis`, `GET /api/v1/market/indicators`, `POST /api/v1/predictions`, CRUD para `alerts` y `watchlist`, `POST /api/v1/auth/login → JWT`.

**Beneficios:**
- Swagger automático en `/docs`
- Validación con Pydantic (elimina `data.get('price')` sin tipo)
- Testeable con `httpx` sin levantar Streamlit
- Frontend intercambiable (Streamlit, React, Power BI, app móvil)

### Fase 3 — PostgreSQL ✅ HECHA (mayo 2026)

Migración completada a **Neon Postgres** serverless (no Supabase como se
preveía). Scraper, CI y dashboard apuntan a la misma BD vía `DATABASE_URL`;
desapareció el sync por Google Drive. Pool de conexiones en
`db/connection_pg.py`, dialecto abstraído en `db/dialect.py`.

**Beneficios aún por explotar** (la migración los habilita pero no se usan):
- Materialized views para KPIs (refresh tras scraping, lecturas instantáneas)
- Full-text search en español con `tsvector` + GIN sobre descripciones
- `PERCENTILE_CONT` nativo (mediana en SQL en lugar de en Python)

**Coste pagado** (deuda de migración): SQLite-isms residuales que rompían en
silencio. Mitigado con `verify_pg_queries.py` (§1, §2.4). El tuneo del pool
para los cold starts del free-tier de Neon se hizo en `782dfb5`.

---

## 4. Comparativa de rendimiento esperado

| Métrica | Actual | Fase 1 completa | Fase 2 | Fase 3 |
|---|---|---|---|---|
| Carga inicial | 3-8s | 1-2s | <1s | <500ms |
| Cambio de filtro | 2-5s | 0.5-1.5s | <500ms | <200ms |
| Cambio de pestaña | 1-3s | <500ms | <500ms | <500ms |
| Usuarios concurrentes | 3-5 | 5-10 | 20-50 | 100+ |

---

## 5. Decisiones arquitectónicas

### ¿Por qué no React ahora?
El cuello de botella principal no es el framework de UI sino la falta de paginación, pre-cómputo de indicadores y conexión ineficiente. Corrigiendo eso (Fase 1), Streamlit con multipage + fragments es viable. Si en el futuro hace falta UI más sofisticada, la Fase 2 deja la puerta abierta vía API.

### ¿Por qué FastAPI y no Django?
No necesitamos un ORM opinionado ni un admin panel. FastAPI da: async nativo, validación con Pydantic, Swagger automático y curva de aprendizaje mínima desde el código actual.

### ¿Cuándo PostgreSQL?
Cuando alguno de estos disparadores: DB > 100 MB, > 5 usuarios concurrentes, full-text search en descripciones, o sincronización vía Google Drive empiece a fallar. Antes es over-engineering.

### ¿Y Redis?
No en los próximos 12 meses. La tabla `market_snapshots` cubre el 90% de los casos de caché. Redis solo tiene sentido con > 20 usuarios concurrentes o datos en tiempo real.

---

## 6. Próximos movimientos (mi recomendación)

Por orden de bang-for-buck:

1. ✅ ~~Índices compuestos~~ — hecho en `dc5a3cf` (marzo 2026)
2. ✅ ~~Subir threshold de `mark_stale_as_sold` a 14d~~ — hecho en abril 2026
3. ✅ ~~Absorption Rate + Months of Supply~~ — implementados (`market_indicators.get_absorption_rate` / `get_months_of_supply`)
4. ✅ ~~Barrido sistemático de SQLite-isms~~ — `verify_pg_queries.py` (junio 2026). Ejercita 60 funciones de lectura contra Neon. Detectó y se corrigieron 3 (`get_barrio_ranking`, `get_price_by_zone`, `get_listing_by_url`).
5. ✅ ~~Arreglar el N+1 de `get_properties_with_multiple_drops`~~ — reescrito como una sola query con window functions (commit `d90602a`, junio 2026). 216 props en ~2.8s, equivalencia verificada vs lógica antigua, barrido 61 ok / 0 fallos.
6. ✅ ~~**Logging estructurado + matar los `except Exception` mudos**~~ — `logging_config.get_logger` + conversión a `logger.exception` en los módulos de librería (commit `7e1076a`, junio 2026) y email diario con exit ≠0 al fallar (step en rojo en vez de verde silencioso). Cierra la observabilidad de los fallos que antes salían como "No hay datos disponibles".
7. ✅ ~~Score de Negociabilidad~~ — ya estaba implementado (`analytics.calculate_negotiability_score`) y verificado funcionando contra Neon (junio 2026). El roadmap lo listaba como pendiente por estar desactualizado.
8. **Alertas por email en el front público** (1-2 semanas). Convierte madridhome.tech en producto con retención. El motor interno ya existe.
9. ✅ ~~`verify_pg_queries.py` en CI~~ — job `pg-query-sweep` en `tests.yml` (postgres:16 efímero + alembic + barrido con `--seed`), verificado en CI real (junio 2026).
