# EngLakeHouse 🏗️📊

A modular, containerized local Data Lakehouse architecture built with **Apache Iceberg**, **Apache Spark**, **Trino**, **MinIO**, and **Apache Superset**, orchestrated via Docker Compose.

Designed for local development and analytical engineering, this pipeline implements the **Medallion Architecture** (Bronze, Silver, Gold layers) alongside advanced analytics layers to process source data, write open table formats to object storage, run lightning-fast SQL queries, and power BI dashboards.

---

## 🏛️ Architecture & Tech Stack

* **Storage Layer:** **MinIO** (S3-compatible object storage)
* **Table Format & Catalog:** **Apache Iceberg** with an Iceberg REST Catalog fixture
* **Processing & ETL Engine:** **Apache Spark 3.5** with PySpark jobs (Bronze $\rightarrow$ Silver $\rightarrow$ Gold $\rightarrow$ Analytics)
* **Query Engine:** **Trino** for distributed ANSI SQL analytics over Iceberg tables
* **BI & Visualization:** **Apache Superset** for interactive dashboarding and reporting
* **Orchestration:** Custom shell scripts and Docker Compose

---

## 📁 Project Structure

```text
EngLakeHouse/
├── docker-compose.yml          # Infrastructure orchestration
├── .gitignore                  # Excludes local data, logs, and secrets
├── run-bronze.sh               # Ingestion pipeline scripts
├── run-silver.sh               # Cleansing & transformation pipeline
├── run-gold.sh                 # Dimensional modeling (Star Schema)
├── run-analytics.sh            # Aggregated analytics jobs
├── minio/                      # MinIO Docker configurations
├── spark/                      # Spark configs, JARs, and PySpark jobs
│   ├── conf/                   # Spark defaults
│   ├── jars/                   # Iceberg and Postgres connector JARs
│   └── jobs/                   # PySpark pipelines (Bronze, Silver, Gold, Analytics)
├── trino/                      # Trino coordinator & catalog configurations
└── superset/                   # Superset custom Dockerfile & configuration
