#!/usr/bin/env bash

set -euo pipefail

COMPOSE_CMD="docker compose"

# Lista dos serviços Airflow conforme seu docker-compose.yaml :contentReference[oaicite:0]{index=0}&#8203;:contentReference[oaicite:1]{index=1}
AIRFLOW_SVCS=(airflow-webserver airflow-scheduler airflow-worker airflow-triggerer)

echo "🚧 Buildando serviços Airflow (Para aplicar mudanças na imagem ou requisitos)..."
# rebuild apenas os serviços Airflow
${COMPOSE_CMD} build "${AIRFLOW_SVCS[@]}"

echo
echo "🔄 Reiniciando somente os serviços Airflow..."
# reinicia em background, sem afetar dependências externas
${COMPOSE_CMD} up -d --no-deps "${AIRFLOW_SVCS[@]}"

echo
echo "✅ Serviços Airflow rebuildados e reiniciados:"
${COMPOSE_CMD} ps "${AIRFLOW_SVCS[@]}"
