# TechChallenge3 • Pipeline de Dados (AWS + Power BI)

> **Objetivo**  
> Construir uma pipeline de dados que parte de uma camada **SOR** (Source of Records) no Amazon **S3**, aplicar transformações com **AWS Glue**, carregar dados harmonizados na **SOT** (Source of Truth) também no S3, carregar os dados tratados da SOT em um **RDS** (banco relacional) através do **AWS Glue** e disponibilizaz para análise no **Power BI**.

---

## Arquitetura

![Arquitetura](docs/pipeline_dados.png)

Fluxo (da esquerda para a direita):

1. **SOR (S3)** – dados brutos (CSV).  
2. **Glue – ETL** – limpeza, padronização, *de/para*, *dedup*, enriquecimentos.  
3. **SOT (S3)** – dados prontos para consumo analítico/ingestão.  
4. **Glue – Ingestão** – *upsert* no banco relacional (RDS) via JDBC.  
5. **RDS** – modelo relacional para BI (tabelas fato/dim).  
6. **Power BI** – relatórios/dashboards conectados ao RDS.

---

## Stack

- **AWS S3** (data lake – zonas `sor/` e `sot/`)  
- **AWS Glue** (Jobs de ETL em PySpark + Crawlers + Data Catalog)  
- **AWS RDS** (PostgreSQL)  
- **Power BI**  
- **Python 3.10+**

### Estrutura do Repositório

- `docs/pipeline_dados.png` — diagrama da solução  
- `docs/Depara.xlsx` — mapeamentos (*de/para*) usados no ETL  
- `src/glue_jobs/etl_sor_to_sot.py` — Job Glue 1  
- `src/glue_jobs/etl_sot_to_rds.py` — Job Glue 2  
- `src/sql/rds_ddl.sql` — DDL para criar o esquema no RDS  
- `powerbi/modelo.pbit` — *template* do relatório (opcional)  

---