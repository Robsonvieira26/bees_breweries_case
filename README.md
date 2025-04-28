# Breweries Case

Projeto de ingestão e orquestração de dados de breweries usando Airflow, MinIO e Terraform.
## Pré-requisitos

- Docker & Docker Compose instalados  
- Git  
## Como executar

1. Clone o repositório e entre na pasta do projeto:
   ```bash
   git clone https://github.com/Robsonvieira26/bees_breweries_case.git
   cd bees_breweries_case
   ```
Configure suas credenciais editando o arquivo .env com os valores adequados:

  ```bash
    AIRFLOW_UID=2002
    _AIRFLOW_WWW_USER_USERNAME=user
    _AIRFLOW_WWW_USER_PASSWORD=password
    POSTGRES_USER=airflow
    POSTGRES_PASSWORD=airflow
    POSTGRES_DB=airflow
    MINIO_ROOT_USER=minioadmin
    MINIO_ROOT_PASSWORD=minioadmin
    SMTP_HOST=smtp.gmail.com
    SMTP_USER=mail@gmail.com
    SMTP_PASSWORD=password_email
    SMTP_PORT=587
    MAIL_FROM=airflow_mail
  ```

Torne o script executável e execute-o:

  ```bash
        chmod +x shellscripts/run.sh
        ./shellscripts/run.sh
```
Configure suas do MinIO criando o arquivo ./terraform/terraform.tfvars :
  ```terraform
    minio_user = "minioadmin"
    minio_password = "minioadmin"

    buckets = [
      "raw-data",
      "processed-data",
      "backup"
    ]
  ```

Após alguns instantes, os serviços estarão disponíveis em:

Airflow UI: http://localhost:8080

MinIO Console: http://localhost:9001

Flower (monitor Celery): http://localhost:5555

# Estrutura de arquivos
- **docker-compose.yaml**: Descreve todos os serviços Docker: PostgreSQL, Redis, MinIO, Terraform e componentes do Airflow.
- **.env**: Variáveis de ambiente usadas pelos containers.
- **shellscripts/**
	- **run.sh**: script de bootstrap de toda a stack
	- **rebuild_airflow.sh**: reconstrói o ambiente Airflow
	- **remove.sh**: remove containers e volumes
- **dags/**: Onde ficam as definições de DAGs do Airflow.
- **plugins/**: Hooks e Operators customizados do Airflow.
- **terraform/** Configurações HCL para provisionamento de buckets no MinIO.
- **Arquitetura.md**: Visão dos serviços Docker e suas responsabilidades.
- **Airflow.md**: Detalhamento das camadas do Airflow, fluxo de dados e reações a mudanças nos datasets.
- **Decisões.md**: 

## Referências de Documentação Complementar

| Arquivo         | Descrição breve                                                                 |
|-----------------|---------------------------------------------------------------------------------|
| [Arquitetura](Arquitetura.md) | Detalha a topologia Docker dos serviços (PostgreSQL, Redis, MinIO, Airflow), fluxos de rede e dependências de containers. |
| [Airflow](Airflow.md)   | Explica a orquestração de DAGs, Data-Aware Scheduling (Datasets), sensores e estratégias de retry e monitoramento. |
| [Decisão Arquitetural](Decisão_Arquitetural.md) | Justifica a escolha de MinIO, versão do Airflow, arquitetura de buckets e estratégia de backup em cenário on‑premise open source. | Explica a orquestração de DAGs, Data-Aware Scheduling (Datasets), sensores e estratégias de retry e monitoramento. |
