# TechChallenge – Fase 3 • Pipeline de Dados (AWS + Power BI)

> **Objetivo**  
> Construir um pipeline de dados do tipo *batch* que parte de um **SOR** (System of Record) no Amazon **S3**, aplica transformações com **AWS Glue**, publica um **SOT** (System of Truth) também no S3, carrega dados tratados em um **RDS** (banco relacional) e disponibiliza para análise no **Power BI**.

---

## Arquitetura

![Arquitetura](docs/arquitetura.png)

Fluxo (da esquerda para a direita):

1. **SOR (S3)** – dados brutos (CSV/JSON/Parquet).  
2. **Glue – ETL 1** – limpeza, padronização, *de/para*, *dedup*, enriquecimentos.  
3. **SOT (S3)** – dados prontos para consumo analítico/ingestão.  
4. **Glue – ETL 2** – *upsert* no banco relacional (RDS) via JDBC.  
5. **RDS** – modelo relacional para BI (tabelas fato/dim).  
6. **Power BI** – relatórios/dashboards conectados ao RDS.

---

## Stack

- **AWS S3** (data lake – zonas `sor/` e `sot/`)  
- **AWS Glue** (Jobs de ETL em PySpark + Crawlers + Data Catalog)  
- **AWS RDS** (PostgreSQL ou MySQL)  
- **Power BI Desktop/Service**  
- **Python 3.10+**

### Estrutura do Repositório

- `docs/arquitetura.png` — diagrama da solução  
- `docs/Depara.xlsx` — mapeamentos (*de/para*) usados no ETL  
- `src/glue_jobs/etl_sor_to_sot.py` — Job Glue 1  
- `src/glue_jobs/etl_sot_to_rds.py` — Job Glue 2  
- `src/sql/rds_ddl.sql` — DDL para criar o esquema no RDS  
- `powerbi/modelo.pbit` — *template* do relatório (opcional)  

---

## Organização no S3

