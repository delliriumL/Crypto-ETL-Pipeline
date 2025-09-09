# Crypto ETL Pipeline with Airflow, Postgres & Superset

This project implements an **ETL pipeline for cryptocurrency data** (Bitcoin, Ethereum, Dogecoin) using **Apache Airflow**, **PostgreSQL**, and **Apache Superset** for visualization.

---

## Project Overview
The pipeline performs the following steps:

1. **Extract & Load (STG layer)**  
   - Load raw CSV files (`bitcoin.csv`, `ethereum.csv`, `dogecoin.csv`) into the `stg` schema.  
   - Limited to the first **10,000 rows** per dataset for performance reasons.  

2. **Transform (DDS layer)**  
   - Merge all coins into the table `dds.crypto_prices`.  

3. **Data Marts (DM layer)**  
   - Create analytical tables (`dm_hyp1`, `dm_hyp2`, `dm_hyp3`).  

4. **Views for Visualization**  
   - Build views (`view_hypX_Y`) to support dashboards.  

5. **Visualization (Superset)**  
   - Superset connects to `data-postgres` and visualizes dashboards.  

---

## Architecture

```text
                ┌─────────────┐
                │   CSV Data  │
                └─────┬───────┘
                      │
            ┌─────────▼─────────┐
            │ Airflow (ETL DAG) │
            └─────────┬─────────┘
                      │
              ┌───────▼─────────┐
              │   Postgres DB   │
              │ ─────────────── │
              │   stg schema    │  <- raw CSV load
              │   dds schema    │  <- transformed
              │   dm schema     │  <- marts
              │   views         │  <- BI layer
              └───────┬─────────┘
                      │
            ┌─────────▼─────────┐
            │   Superset (BI)   │
            └───────────────────┘

```

---

## Tech Stack

- **Apache Airflow** – Orchestrating ETL jobs  
- **PostgreSQL** – Data warehouse (STG, DDS, DM schemas)  
- **Pandas** – CSV processing  
- **SQLAlchemy** – Database interaction  
- **Apache Superset** – BI & visualization  
- **Docker Compose** – Containerization  

---

## Project Structure

```bash
.
├── dags/                        # Airflow DAGs & SQL jobs
│   ├── data/                    # CSV input data (bitcoin.csv, ethereum.csv, dogecoin.csv)
│   ├── sql/                     # SQL scripts for DM & Views
│   │   ├── cleaning.sql
│   │   ├── drop_old.sql
│   │   ├── dm_hyp1.sql, dm_hyp2.sql, dm_hyp3.sql
│   │   ├── view_hyp1_1.sql ... view_hyp3_2.sql
│   └── crypto_etl_pipeline.py   # Main Airflow DAG definition
│
├── script/
│   └── crypto_etl.py            # ETL Python functions (schemas, loaders, transforms)
│
├── superset/                    # Superset configuration & metadata
│   └── superset.db              # SQLite DB for Superset (dev only)
│
├── entrypoint.sh                 # Startup script for containers
│
├── docker-compose-LocalExecutor.yml   # Airflow with Local Executor
├── docker-compose-CeleryExecutor.yml  # Airflow with Celery Executor
├── docker-compose.yml                  # Default orchestration
│
├── Dockerfile                   # Airflow Dockerfile
├── Dockerfile.superset          # Superset Dockerfile

```

---

## Running the Project

### 1. Clone the repository
```bash
git clone https://github.com/<your-repo>/crypto-etl.git
cd crypto-etl
```

### 2. Start services
```bash
docker-compose up -d
```

This will start:
- `postgres` – Airflow metadata DB  
- `data-postgres` – Data warehouse  
- `webserver` – Airflow UI (`http://localhost:8080`)  
- `superset` – Superset UI (`http://localhost:8088`)  

### 3. Airflow
- URL: [http://localhost:8080](http://localhost:8080)  
- Login/Password: `airflow / airflow`  
- Activate DAG: **`crypto_etl_pipeline`**  

### 4. Superset
- URL: [http://localhost:8088](http://localhost:8088)  
- Login/Password: `admin / admin`  
- Connect to `dataDB` and create dashboards using `view_hypX_Y`.  

---

## DAG Workflow

- **drop_old_data** – clear old staging data  
- **create_schemas** – create schemas `stg`, `dds`, `dm`  
- **import_bitcoin / import_ethereum / import_dogecoin** – load CSV files  
- **transform_to_dds** – merge into `dds.crypto_prices`  
- **create_dm_hyp1/2/3** – create analytical tables  
- **create_view_hypX_Y** – build views for BI  

---
