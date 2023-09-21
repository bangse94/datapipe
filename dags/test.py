import os
import io

from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

ENV_ID = os.environ.get("SYSTEM_ENV_ID")

with DAG(
    dag_id = "test-dag",
    start_date=datetime(2023, 1, 1),
    schedule='@once',
    catchup=False,
) as dag:
    get_update_date = PostgresOperator(
        task_id='get_update_date',
        sql = "SELECT * FROM engine_task WHERE update_date BETWEEN %(begin_date)s AND %(end_date)s",
        parameters = {"begin_date": "2023-09-19", "end_date": "2023-09-20"},
        hook_params = {"options": "-c statement_timeout=3000ms"},
    )
