# Decisão Arquitetural

## Sumário Executivo

Este documento registra a escolha-chave para o meu projeto de orquestração de dados usando Apache Airflow e MinIO em cenário on‑premise e open source. Explica-se o motivo da adoção do MinIO, a seleção da versão do Airflow, a segmentação de buckets para camadas de dados e a rotina de backup que garante confiabilidade e recuperação de desastres.

---

## 1. Escolha do MinIO como bucket

### 1.1 Projeto Open Source
- Escolhi o MinIO por ser uma solução **open source** compatível com a API S3 sob licença AGPLv3, permitindo extender e contribuir no código sem custos de licenciamento.
- A comunidade ativa e o desenvolvimento aberto proporcionam correções e novos recursos de forma ágil, em sintonia com a filosofia de projeto colaborativo.

### 1.2 Cenário On‑Premise
- MinIO roda em hardware commodity, consumindo menos de 128 MiB de RAM e 400 MHz de CPU, ideal para datacenter on‑premise com recursos limitados.
- A compatibilidade total com S3 facilita migrar workloads de nuvem pública para infraestrutura interna sem reescrever aplicações.
---
## 2. Escolha da versão do Apache Airflow

### 2.1 Políticas de Suporte e LTS
- Optei pela versão **2.10.x** do Airflow, que está em suporte ativo com correções de segurança e bugs, enquanto versões anteriores estão em EOL.
- A 2.10.x prepara o caminho para migração futura ao Airflow 3.0, aproveitando o ciclo LTS planejado pela comunidade.

### 2.2 Recursos Relevantes
- Data‑Aware Scheduling (Datasets) e sensoriamento de arquivos, disponíveis desde a 2.7, permitem orquestração reativa a eventos de dados.
- Compatibilidade com Python 3.9–3.12 e integração com Kubernetes Operator facilitam deploy em cluster on‑premise ou híbrido.

---

## 3. Arquitetura de buckets para camadas de dados

Organizo os dados segundo o padrão Medallion (Bronze, Silver, Gold) em buckets distintos para otimizar acesso, segurança e ciclo de vida.

| **Camada** | **Descrição**                                | **Bucket**                 |
|------------|----------------------------------------------|----------------------------|
| Bronze     | Dado bruto imutável, ingestão inicial        | `raw-data`                 |
| Silver     | Dado limpo, validado e padronizado           | `processed-data/silver`    |
| Gold       | Dado agregado pronto para consumo final      | `processed-data/gold`      |

- A segregação física permite políticas de ciclo de vida e criptografia específicas por camada.
- Buckets separados evitam contenção de throughput e possibilitam SLAs de retenção e versionamento distintos.

### 3.1 Uso de Terraform para provisionamento de buckets

- Utilizo **Terraform** como IaC (Infrastructure as Code) para definir de forma declarativa todos os buckets MinIO antes da execução dos DAGs.
- O código Terraform especifica **recursos** `minio_s3_bucket`, ACLs, políticas de versionamento e lifecycle, garantindo **consistência** entre ambientes (dev, staging, prod).
- Com módulos reutilizáveis, crio buckets para cada camada (raw-data, processed-data/silver, processed-data/gold, backups) com parâmetros parametrizáveis (replication, encryption).
- A integração CI/CD executa `terraform plan` e `terraform apply` automaticamente em merge na branch main, assegurando que qualquer alteração de bucket seja revisada em código.
- Esse approach elimina configurações manuais, reduz erros humanos e facilita auditoria de mudanças na infraestrutura.
---

## 4. Rotina de backup

### 4.1 Estratégia de backup
- Realizo **backups incrementais diários** e **semanais**, combinados com versionamento de objetos, para garantir baixo RPO e restauração ponto‑no‑tempo.

### 4.3 Retenção e limpeza
- Defino política de lifecycle para expirar objetos em Bronze após 7 dias; 

---

**Conclusão**: Esta decisão arquitetural garante um pipeline de dados robusto, escalável e alinhado a cenários open source e on‑premise, proporcionando flexibilidade operacional, segurança e baixo custo de manutenção.

