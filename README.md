# Madrid Real Estate Tracker

Pipeline de scraping, análisis y visualización del mercado inmobiliario de Madrid (idealista.com).

- **Scraper**: barrios de Madrid vía **Bright Data Web Unlocker** (primario) con **Oxylabs** como fallback dormido. Ejecutado en GitHub Actions con cadencia diaria y modo `lite` (newest-first + early-stop) entre semana.
- **BD**: **Neon Postgres** con pool de conexiones (`db/connection_pg.py`). Scraper y dashboard apuntan directos a la misma BD — sin sync vía Google Drive (retirado tras la migración a Postgres, mayo 2026).
- **Dashboard interno** (Streamlit): análisis avanzado, vigilancia macro, oportunidades, alertas, modelo predictivo. Auth multi-usuario con bcrypt.
- **Dashboard público** (Next.js → [madridhome.tech](https://madridhome.tech)): métricas agregadas con ISR, alimentado desde `metrics.json` regenerado por CI.

## Arranque rápido

```bash
# 1. Clonar y crear venv
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 2. Variables de entorno
cp .env.example .env   # editar con credenciales Bright Data

# 3. Ejecutar el scraper (~30-60 min)
python scraper.py

# 4. Lanzar el dashboard local
streamlit run app.py
```

El dashboard arranca en `http://localhost:8501`. Apunta a Neon Postgres vía `DATABASE_URL` (env) o `st.secrets["postgres"]["url"]` (Streamlit Cloud); con `DB_BACKEND=postgres` para forzarlo. Sin esa config, el shim cae a SQLite local (`db/connection.py`) para desarrollo aislado.

### Verificación de queries contra Postgres

Tras tocar SQL conviene ejecutar el barrido que ejercita todas las funciones de lectura contra Neon y delata SQLite-isms residuales (ver `ROADMAP.md` → deuda de migración):

```bash
python verify_pg_queries.py            # sale 1 si alguna query falla
python verify_pg_queries.py --verbose  # muestra el error capturado
```

## Documentación

| Archivo | Para qué sirve |
|---|---|
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | Cómo funciona el sistema hoy: módulos, flujos, dependencias |
| [`DATA_MODEL.md`](DATA_MODEL.md) | Esquema de la BD (`listings`, `price_history`, `scraping_log`, `market_snapshots`) |
| [`DEPLOYMENT.md`](DEPLOYMENT.md) | Despliegue en Streamlit Cloud + configuración del workflow CI |
| [`AUTH.md`](AUTH.md) | Configuración de autenticación multi-usuario via `st.secrets` |
| [`ROADMAP.md`](ROADMAP.md) | Próximos pasos: features pendientes + plan arquitectónico (Fase 1/2/3) |
| `market-thermometer/audit-calidad-datos.md` | Histórico de la auditoría de calidad de datos (cerrada al 100%) |

## Pipeline diario (GitHub Actions)

`.github/workflows/daily_scraper.yml` corre los lunes y jueves a las 06:00 UTC y ejecuta:

1. Scraping de los ~184 barrios contra Idealista vía Bright Data
2. Pre-cálculo de KPIs diarios (`compute_snapshots.py`)
3. Email diario con el resumen
4. Health-check final que falla el run si no se procesó suficiente data

`.github/workflows/export-metrics.yml` corre los lunes a las 07:00 UTC y regenera el `metrics.json` + `barrios_profiles.json` que alimentan el frontend público.

`.github/workflows/mi_zona_alerts.yml` corre a diario a las 07:00 UTC y envía email con las propiedades nuevas en tus barrios que pasan el umbral de margen de oferta.

Todos los workflows hablan directamente con la BD **Neon Postgres** vía `DATABASE_URL` (sin sincronización vía Google Drive — eso quedó atrás con la migración a Postgres en mayo 2026).

## Ética

Este proyecto es para análisis personal y educativo. Respeta los Términos de Servicio de idealista, usa rate limiting razonable, y no redistribuyas datos scrapeados públicamente sin agregación.

## Licencia

Código abierto para uso educativo y personal.
