from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
from script.crypto_etl import (
    create_all_schemas,
    load_bitcoin,
    load_ethereum,
    load_dogecoin,
    transform_to_dds,
)

with DAG("crypto_etl_pipeline",
         schedule_interval="@daily",
         start_date=datetime(2024, 1, 1),
         catchup=False,
         template_searchpath="./sql"  # путь к SQL-скриптам
         ) as dag:

    drop_old_data = PostgresOperator(
        task_id='drop_old_data',
        postgres_conn_id='data_postgres',
        sql='sql/drop_old.sql'
    )

    create_schemas = PythonOperator(
        task_id="create_schemas",
        python_callable=create_all_schemas,
    )

    import_btc = PythonOperator(
        task_id="import_bitcoin",
        python_callable=load_bitcoin,
    )

    import_eth = PythonOperator(
        task_id="import_ethereum",
        python_callable=load_ethereum,
    )

    import_doge = PythonOperator(
        task_id="import_dogecoin",
        python_callable=load_dogecoin,
    )

    transform_dds = PythonOperator(
        task_id="transform_to_dds",
        python_callable=transform_to_dds,
    )

    create_dm_hyp1 = PostgresOperator(
        task_id="create_dm_hyp1",
        sql="sql/dm_hyp1.sql",
        postgres_conn_id='data_postgres',
    )

    create_dm_hyp2 = PostgresOperator(
        task_id="create_dm_hyp2",
        sql="sql/dm_hyp2.sql",
        postgres_conn_id='data_postgres',
    )

    create_dm_hyp3 = PostgresOperator(
        task_id="create_dm_hyp3",
        sql="sql/dm_hyp3.sql",
        postgres_conn_id='data_postgres',
    )

    create_view_hyp1_1 = PostgresOperator(
        task_id="create_view_hyp1_1",
        sql="sql/view_hyp1_1.sql",
        postgres_conn_id='data_postgres',
    )

    create_view_hyp1_2 = PostgresOperator(
        task_id="create_view_hyp1_2",
        sql="sql/view_hyp1_2.sql",
        postgres_conn_id='data_postgres',
    )

    create_view_hyp2_1 = PostgresOperator(
        task_id="create_view_hyp2_1",
        sql="sql/view_hyp2_1.sql",
        postgres_conn_id='data_postgres',
    )

    create_view_hyp2_2 = PostgresOperator(
        task_id="create_view_hyp2_2",
        sql="sql/view_hyp2_2.sql",
        postgres_conn_id='data_postgres',
    )

    create_view_hyp3_1 = PostgresOperator(
        task_id="create_view_hyp3_1",
        sql="sql/view_hyp3_1.sql",
        postgres_conn_id='data_postgres',
    )

    create_view_hyp3_2 = PostgresOperator(
        task_id="create_view_hyp3_2",
        sql="sql/view_hyp3_2.sql",
        postgres_conn_id='data_postgres',
    )


    # Dependencies
    drop_old_data >> create_schemas
    create_schemas >> [import_btc, import_eth, import_doge]
    [import_btc, import_eth, import_doge] >> transform_dds
    transform_dds >> [create_dm_hyp1, create_dm_hyp2, create_dm_hyp3]
    create_dm_hyp1 >> [create_view_hyp1_1, create_view_hyp1_2]
    create_dm_hyp2 >> [create_view_hyp2_1, create_view_hyp2_2]
    create_dm_hyp3 >> [create_view_hyp3_1, create_view_hyp3_2]

