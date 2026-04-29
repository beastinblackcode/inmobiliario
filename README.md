# Madrid Real Estate Tracker

Pipeline de scraping, análisis y visualización del mercado inmobiliario de Madrid (idealista.com).

- **Scraper**: 139 barrios via Bright Data Web Unlocker, ejecutado en GitHub Actions cada 3 días.
- **BD**: SQLite (~19 MB) sincronizada con Google Drive entre runs.
- **Dashboard interno** (Streamlit): análisis avanzado, vigilancia macro, oportunidades, alertas, modelo predictivo.
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

El dashboard arranca en `http://localhost:8501`. Si `real_estate.db` no existe, el sistema descarga la última versión desde Google Drive (requiere `GOOGLE_DRIVE_FILE_ID` en `.env` o `.streamlit/secrets.toml`).

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

1. Descarga `real_estate.db` desde Google Drive
2. Backfill idempotente de `price_history` (`migration_backfill_initial_history.py`)
3. Scraping de los 139 barrios
4. Pre-cálculo de KPIs diarios (`compute_snapshots.py`)
5. Email diario con el resumen
6. Export del `metrics.json` y push al repo `market-thermometer`
7. Upload de la DB actualizada a Google Drive

`.github/workflows/export-metrics.yml` permite regenerar solo el `metrics.json` sin scrapear (manual dispatch).

## Ética

Este proyecto es para análisis personal y educativo. Respeta los Términos de Servicio de idealista, usa rate limiting razonable, y no redistribuyas datos scrapeados públicamente sin agregación.

## Licencia

Código abierto para uso educativo y personal.
