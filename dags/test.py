import os
import io

from datetime import datetime
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.connection import Connection
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

con = Connection(
    conn_id = 'cvat_postgres',
    conn_type = 'postgres',
    login = 'root',
    password = ''
)

default_args = {
    "start_date ": days_ago(n=1)
}

dag = DAG(
    dag_id = "test-dag",
    default_args = default_args,
    schedule_interval = "@once"
)

def task_test_query():
    hook = PostgresHook(postgres_conn_id='cvat_postgres')
    
    rows = hook.get_records("SELECT * FROM public.auth_user")
    
    for row in rows:
        print(row)

task_1 = PythonOperator(
    task_id = "run_query_with_python",
    python_callable = task_test_query,
    dag = dag
)

task_1