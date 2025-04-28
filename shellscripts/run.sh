#!/usr/bin/env bash
set -euo pipefail

# =======================================================
# Shell bootstrap script for Breweries Data Platform
# - Inicializa infraestrutura (Postgres, Redis, MinIO)
# - Provisiona buckets via Terraform
# - Inicializa e sobe Airflow e Metabase
# =======================================================

# Colors for output
GREEN="\033[0;32m"
YELLOW="\033[1;33m"
CYAN="\033[0;36m"
RED="\033[0;31m"
RESET="\033[0m"

info()   { echo -e "${CYAN}[INFO]${RESET}  $*"; }
success(){ echo -e "${GREEN}[OK]${RESET}    $*"; }
warn()   { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
error()  { echo -e "${RED}[ERROR]${RESET} $*"; exit 1; }

# Load environment variables
info "Carregando variáveis de ambiente de .env"
if [[ ! -f .env ]]; then
  error "Arquivo .env não encontrado"
fi
set -a; source .env; set +a

# Validate required env vars
: "${MINIO_ROOT_USER:?Variable MINIO_ROOT_USER não definida}"
: "${MINIO_ROOT_PASSWORD:?Variable MINIO_ROOT_PASSWORD não definida}"

# Create required directories
info "Criando diretórios: dags, logs, plugins, config, terraform"
mkdir -p dags logs plugins config terraform
rm -rf terraform/.terraform*

# Start core services
info "Subindo PostgreSQL, Redis e MinIO"
docker-compose up -d postgres redis minio || error "Falha ao subir infraestrutura"

# Wait for MinIO healthy
info "Aguardando MinIO estar disponível..."
until docker-compose exec minio mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" &> /dev/null; do
  sleep 5 && warn "Tentando conectar ao MinIO..."
done
success "MinIO pronto"

# Terraform: init, plan, apply
info "Provisionando buckets com Terraform"
docker-compose run --rm terraform init -input=false
docker-compose run --rm terraform plan -input=false
docker-compose run --rm terraform apply -auto-approve -input=false
success "Buckets provisionados"

# Initialize Airflow DB and users
info "Inicializando banco e usuários do Airflow"
docker-compose up airflow-init || error "Falha no airflow-init"

# Start Airflow services
info "Subindo serviços do Airflow"
docker-compose up -d airflow-webserver airflow-scheduler airflow-worker airflow-triggerer


# Configure Airflow Variables
info "Configurando variáveis no Airflow"
docker-compose exec airflow-webserver bash -c "python - << 'EOF'
from airflow.models import Variable
Variable.set('OPEN_BREWERY_API_URL', 'https://api.openbrewerydb.org')
Variable.set('MINIO_ENDPOINT', 'host.docker.internal:9000')
Variable.set('MINIO_ACCESS_KEY', '$MINIO_ROOT_USER')
Variable.set('MINIO_SECRET_KEY', '$MINIO_ROOT_PASSWORD')
EOF"
success "Variáveis configuradas"

# Show final status and URLs
info "Verificando status dos containers"
docker-compose ps

cat << EOF

${GREEN}Ambiente pronto!${RESET}
Acesso:
  Airflow UI:    http://localhost:8080
  MinIO Console: http://localhost:9001
  Flower:        http://localhost:5555
EOF

