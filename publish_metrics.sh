#!/bin/bash
# publish_metrics.sh
# Genera el JSON de métricas públicas y lo sube al repo del termómetro.
# Llamar al final del scrape diario, o manualmente cuando quieras actualizar.
#
# Uso:
#   ./publish_metrics.sh
#   ./publish_metrics.sh --dry-run   # solo genera el JSON, no hace push

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
THERMOMETER_DIR="$SCRIPT_DIR/market-thermometer"
METRICS_FILE="$THERMOMETER_DIR/public/metrics.json"

DRY_RUN=false
[[ "$1" == "--dry-run" ]] && DRY_RUN=true

echo "📊 Generando métricas públicas..."
cd "$SCRIPT_DIR"
python export_public_metrics.py -o "$METRICS_FILE"

if [ $? -ne 0 ]; then
  echo "❌ Error generando métricas. Abortando."
  exit 1
fi

echo "✅ metrics.json actualizado ($(du -h "$METRICS_FILE" | cut -f1))"

if $DRY_RUN; then
  echo "🔍 Modo dry-run: no se hace push."
  exit 0
fi

echo "🚀 Subiendo a GitHub..."
cd "$THERMOMETER_DIR"
git add public/metrics.json
git commit -m "Update metrics $(date '+%Y-%m-%d %H:%M')"
git push origin main

echo "✅ Publicado. Vercel redesplegará en ~30 segundos."
