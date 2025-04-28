#!/bin/bash
# set -e
set -a
source .env
set +a

# Criação de diretórios necessários
echo "Criando diretórios..."
mkdir -p ./dags ./logs ./plugins ./config ./terraform
rm -rf ./terraform/.terraform*  

# Inicia serviços de infraestrutura
echo "Iniciando PostgreSQL, Redis e cluster MinIO..."
docker compose up -d postgres redis minio
# Verifica saúde do MinIO antes de prosseguir
echo "Aguardando o nó MinIO ficar saudável..."
sleep 30

# # Executa o Terraform para criar buckets
echo "Executando Terraform para criar buckets..."
docker compose run --rm terraform init
docker compose run --rm terraform plan
docker compose run --rm terraform apply -auto-approve

# Inicializa o Airflow
echo "Inicializando Airflow..."
docker compose up airflow-init

# Inicia serviços do Airflow
echo "Iniciando serviços Airflow..."
docker compose up -d airflow-webserver airflow-scheduler airflow-worker airflow-triggerer

# Inicia Metabase
echo "Iniciando Metabase..."
# Como o .env já foi carregado no início do script, as variáveis MB_DB_* serão injetadas
docker compose up -d metabase


# Mostra status final
echo "Stack iniciada com sucesso! Verificando status..."
docker compose ps

# Carrega variáveis do .env
set -a
source .env
set +a

# Valida se as variáveis necessárias existem
if [[ -z "${MINIO_ROOT_USER}" || -z "${MINIO_ROOT_PASSWORD}" ]]; then
  echo "❌ Variáveis MINIO_ROOT_USER ou MINIO_ROOT_PASSWORD não foram encontradas no .env"
  exit 1
fi

echo "✅ Variáveis carregadas:"
echo "  MINIO_ROOT_USER=$MINIO_ROOT_USER"
echo "  MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD"

# Executa o comando para definir as variáveis no Airflow
docker compose exec airflow-webserver bash -c "python - << 'EOF'
from airflow.models import Variable
Variable.set('OPEN_BREWERY_API_URL', 'https://api.openbrewerydb.org')
Variable.set('MINIO_ENDPOINT', 'host.docker.internal:9000')
Variable.set('MINIO_ACCESS_KEY', '${MINIO_ROOT_USER}')
Variable.set('MINIO_SECRET_KEY', '${MINIO_ROOT_PASSWORD}')
EOF"

echo "✅ Variáveis MINIO_ENDPOINT, MINIO_ACCESS_KEY e MINIO_SECRET_KEY criadas no Airflow!"


echo -e "\nAcesso:"
echo "Airflow UI:      http://localhost:8080"
echo "MinIO Console:   http://localhost:9001"
echo "Flower:         http://localhost:5555"
echo "Metabase:       http://localhost:3000"



