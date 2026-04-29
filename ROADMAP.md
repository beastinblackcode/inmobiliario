# Roadmap — Madrid Real Estate Tracker

> **Última actualización:** abril 2026
> **Alcance:** este documento consolida el roadmap funcional, técnico y arquitectónico del proyecto. Reemplaza a `NUEVA_ARQUITECTURA.MD`, `PROPUESTA_ARQUITECTURA.md`, `Analisis_Funcional.docx`, `Propuestas_Funcionalidades_Metricas.docx` y `specs-nuevas-features-front.docx`.

---

## 1. Estado actual (abril 2026)

### Lo que ya funciona en producción

| Área | Implementado |
|---|---|
| Scraping | Bright Data Web Unlocker, recorrido por barrios, retry+backoff, circuit breaker en `mark_stale_as_sold` (1.000/lote), reactivación de listings falsamente marcados, budget cap |
| Pipeline diario | GitHub Action `daily_scraper.yml`: download DB → backfill price_history → scraper → snapshots → email → export metrics → upload DB |
| Calidad de datos | Auditoría cerrada al 100% (purga stale 7d/21d, mediana+IQR en trend, filtro warmup en velocidad de venta, backfill de `price_history` inicial) |
| Frontend interno | Streamlit multipage (`pages/` + `tabs/`): `dashboard`, `mapa`, `tendencias`, `oportunidades`, `bajadas`, `búsqueda`, `seguimientos`, `detalle`, `vigilancia`, `admin` |
| Frontend público | Next.js 14 (`market-thermometer/`) en madridhome.tech con `metrics.json` regenerado por CI, ISR y i18n (es/en) |
| Análisis | Score calidad-precio (NLP de descripciones), detección vendedor desesperado, gangas vs distrito |
| ML | Random Forest con OneHotEncoder + intervalo heurístico ±10% |
| Vigilancia macro | 6 indicadores internos + 6 macro (BCE Euríbor, INE IPC/IPV/compraventas/paro/hipotecas) → market score 0-100 |
| Auth | Multi-usuario vía `st.secrets` (sin hashing) |
| Persistencia | SQLite en Google Drive, descarga al inicio del workflow, upload al final |

### Limitaciones estructurales conocidas

- **Frecuencia de scraping**: corre cada 3 días (lun/jue) → `days_to_sell` tiene un suelo estructural de ~7-8 días por la combinación scraping cada 3d + threshold stale=7d.
- **Modelo predictivo**: intervalo de confianza heurístico fijo (±10%), sin validación cruzada visible al usuario, sin reentrenamiento periódico.
- **Errores silenciosos**: `except Exception` genéricos en muchas funciones que enmascaran fallos. `print()` en lugar de `logging`.
- **`database.py` god-module**: ~3.000 líneas mezclan infraestructura, CRUD, lógica de negocio y utilidades de UI.
- **Streamlit full-rerun**: cada cambio de filtro re-renderiza las 8 pestañas. Latencia 2-5s.

---

## 2. Roadmap funcional (qué construir)

Priorización: **🔥 Alta · ⭐ Media · 💤 Baja** · esfuerzo en horas/días.

### 2.1 Métricas adicionales

| Métrica | Prio | Esfuerzo | Comentario |
|---|---|---|---|
| **Absorption Rate** (vendidos 30d / inventario activo × 100) | 🔥 | 4h | Datos ya existen. Semáforo verde >20%, amarillo 15-20%, rojo <15%. Por distrito y barrio. |
| **Months of Supply** (inventario / ventas mensuales medias) | 🔥 | 2h | Métrica estándar internacional. Complementa el ratio O/D. |
| **Score de Negociabilidad** | 🔥 | 6h | f(días_mercado, n_bajadas, gap_vs_mediana_distrito, seller_type). 0-100. Combinable con quality_score. |
| **Yield bruto por alquiler** | 🔥 | Alto | Parcialmente hecho (`rental_yields` en metrics.json). Falta scraper de alquileres dedicado y tabla `rental_benchmarks`. |
| **Price Pressure Index** | ⭐ | 4h | (% subidas - % bajadas) × velocidad. Leading indicator. |
| **Coeficiente de Gini de precios** | ⭐ | 3h | Más preciso que la dispersión actual. Detecta gentrificación. |
| **Volatilidad móvil** (std en ventanas 7d/30d) | ⭐ | 2h | Anticipa cambios de tendencia. |
| **Ratio precio pedido vs vendido** | 💤 | 6h | Aproximación al descuento de negociación. Limitación: no conocemos precio final real. |

### 2.2 Funcionalidades de usuario

| Feature | Prio | Esfuerzo | Comentario |
|---|---|---|---|
| **Alertas por email para usuarios del front público** | 🔥 | 1-2 semanas | El motor interno ya existe (`alerts_tab.py`). Falta exponerlo en madridhome.tech con email + criterios básicos. Convierte el dashboard público en un producto con retención. |
| **Comparador de propiedades** (2-4 lado a lado) | 🔥 | 3-4 días | Tabla + radar chart + mapa. Streamlit. |
| **Calculadora ROI** (yield, cashflow, TIR, breakeven) | 🔥 | 1 semana | Inputs: precio, ITP, reforma, alquiler, gastos, financiación. Outputs: yield bruto/neto, cashflow, TIR a 5/10/15 años. |
| **Perfil de barrio inteligente** | ⭐ | 1 semana | Página dedicada con métricas, evolución temporal, comparativa, top oportunidades del barrio. |
| **Detección de anomalías** | ⭐ | 4 días | Isolation Forest o Z-scores por barrio para flagear chollos / errores / sobreprecios. |
| **Predicción probabilidad venta 30d** | ⭐ | 1-2 semanas | Modelo de clasificación binaria entrenado con `sold_removed`. Mostrar % en cada ficha. |
| **Informes PDF/DOCX automatizados** | ⭐ | 1 semana | Semanal, mensual, por distrito. Cron + reportlab/python-docx. |
| **Heatmap temporal animado** | ⭐ | 4 días | Slider que avanza semana a semana sobre el mapa de calor. |
| **Exportación CSV/Excel** desde el dashboard | ⭐ | 4h | Botón de descarga de los listings filtrados. |
| **i18n del Streamlit interno** | 💤 | 3-4 días | Centralizar textos. Hoy mezcla es/en. |
| **Comparativa de distritos** | 💤 | 3 días | Radar chart superpuesto. |

### 2.3 Mejoras de Machine Learning

| Mejora | Prio | Esfuerzo |
|---|---|---|
| **Features NLP de descripciones** (terraza, garaje, trastero, año construcción, estado, certificación energética) | 🔥 | 4 días |
| **Features adicionales al RF**: precio/m² mediano del barrio, distancia a Sol, densidad de oferta, velocidad del barrio | 🔥 | 3 días |
| **Modelo AVM con comparables**: 5 propiedades más similares + ajustes por característica + intervalo real | ⭐ | 1-2 semanas |
| **Quantile Regression Forest** (intervalos de confianza reales en vez de ±10% heurístico) | ⭐ | 4 días |
| **Métricas de rendimiento visibles** (R², MAE, MAPE) + reentrenamiento periódico | ⭐ | 3 días |
| **Series temporales (Prophet/ARIMA)** para predecir tendencia de precio/m² por distrito | 💤 | 1 semana |

### 2.4 Plataforma

| Item | Prio | Esfuerzo | Comentario |
|---|---|---|---|
| **Tests automatizados con pytest** | 🔥 | 1 semana | Hoy hay solo `test_sold_logic.py` y `test_description.py`. Falta unit + integración + regresión BD. |
| **Logging estructurado** (módulo `logging`, niveles, sustituir prints) | 🔥 | 2-3 días | Hoy hay `except Exception` que enmascaran errores. |
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

### Fase 3 — PostgreSQL

**Cuándo se cumpla alguna de estas condiciones:**
- DB > 100 MB (hoy: 19 MB)
- Más de 5 usuarios concurrentes
- Necesidad de full-text search en descripciones
- Sincronización Google Drive se vuelve un problema (fallos, lentitud)

**Hosting recomendado:** Supabase free tier (500 MB) para empezar; Azure Database for PostgreSQL (~15€/mes) si se necesita integración con Azure AD.

**Beneficios:**
- Materialized views para KPIs (refresh tras scraping, lecturas instantáneas)
- Full-text search en español con `tsvector` + GIN index sobre descripciones
- `PERCENTILE_CONT` nativo (mediana en SQL en lugar de Python)
- Refresh concurrente sin bloquear lecturas
- Pipeline simplificado: scraper y dashboard apuntan directamente a la misma DB; desaparece el sync vía Google Drive

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
2. ✅ ~~Subir threshold de `mark_stale_as_sold` a 14d~~ — hecho en abril 2026, alinea con los `compute_snapshots` que ya asumían 14d
3. **Métricas de Absorption Rate + Months of Supply** (medio día, datos ya en BD). Métrica estándar internacional, vacío evidente en el dashboard de vigilancia.
4. **Score de Negociabilidad** (medio día). Combina `days_on_market` + `n_bajadas` + gap vs mediana del distrito + seller_type. Se monta junto al `quality_score` existente sin tocar el modelo.
5. **Features NLP de descripciones** (terraza, garaje, trastero, año, estado). El texto está en `description` (BD), solo falta regex + diccionarios. Mejora directa al modelo predictivo y a las fichas.
6. **Alertas por email en el front público** (1-2 semanas). Convierte madridhome.tech en producto con retención. El motor interno ya existe en `alerts_tab.py`.
