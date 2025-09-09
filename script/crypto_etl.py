import pandas as pd
from sqlalchemy import create_engine, text
import os
import psutil

PG_CONN = "postgresql://user:pass@data-postgres:5432/dataDB"
DATA_DIR = "dags/data"
engine = create_engine(PG_CONN)

def log_memory(prefix=""):
    mem = psutil.virtual_memory()
    print(f"{prefix} RAM Used: {mem.used // (1024**2)} MB / {mem.total // (1024**2)} MB")

def create_all_schemas():
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS stg"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS dds"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS dm"))
        print("✅ Схемы stg, dds, dm созданы")

def load_bitcoin():
    _load_csv_limited("stg", "stg_bitcoin", "bitcoin.csv")

def load_ethereum():
    _load_csv_limited("stg", "stg_ethereum", "ethereum.csv")

def load_dogecoin():
    _load_csv_limited("stg", "stg_dogecoin", "dogecoin.csv")

def _load_csv_limited(schema, table, filename, max_rows=10000):
    file_path = os.path.join(DATA_DIR, filename)
    print(f"\n=== Loading {schema}.{table} from {file_path}")
    log_memory("Before read_csv")

    chunk_iter = pd.read_csv(file_path, chunksize=2000)
    chunk_list = []
    total_rows = 0

    for chunk in chunk_iter:
        if total_rows + len(chunk) >= max_rows:
            chunk = chunk.iloc[:max_rows - total_rows]
            chunk_list.append(chunk)
            break
        else:
            chunk_list.append(chunk)
            total_rows += len(chunk)

    df_limited = pd.concat(chunk_list, ignore_index=True)
    log_memory("Before to_sql")

    df_limited.to_sql(table, engine, schema=schema,
                      if_exists="replace", index=False, method="multi")
    log_memory("After to_sql")

    print(f"✅ Loaded {schema}.{table} — {len(df_limited)} rows")

def transform_to_dds():
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dds.crypto_prices"))
        conn.execute(text("""
            CREATE TABLE dds.crypto_prices AS
            SELECT 'bitcoin' AS symbol, * FROM stg.stg_bitcoin
            UNION ALL
            SELECT 'ethereum' AS symbol, * FROM stg.stg_ethereum
            UNION ALL
            SELECT 'dogecoin' AS symbol, * FROM stg.stg_dogecoin
        """))
        print("✅ Created dds.crypto_prices")
