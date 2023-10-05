import os
import io

import pandas as pd

from datetime import datetime
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.connection import Connection

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

con = Connection(
    conn_id = 'cvat_postgres',
    conn_type = 'postgres',
    login = 'root',
    password = ''
)

dag = DAG(
    dag_id = "cvat-spark-dag",
    start_date = days_ago(2),
    schedule_interval = "@once"
)

def get_annotation_query(**context):
    hook = PostgresHook(postgres_conn_id='cvat_postgres')
    
    #job_ids = hook.get_records("SELECT x.id FROM public.engine_job x WHERE x.stage = 'acceptance' and x.updated_date < current_date and x.updated_date >= current_date-1")
    job_ids = hook.get_records("SELECT x.id FROM public.engine_job x WHERE x.stage = 'acceptance' and x.updated_date < current_date+1")
    res = []
    for job_id in job_ids:
        # file_name, class_name, bbox
        shape_label_rows = hook.get_records(
            "SELECT x.path, y.name, z.points from public.engine_image x, public.engine_label y, public.engine_labeledshape z, \
                public.engine_job a, public.engine_segment b, public.engine_task c \
                    WHERE z.job_id=%s AND y.id = z.label_id AND a.id = %s AND a.segment_id = b.id \
                        AND b.task_id = c.id AND x.data_id = c.data_id AND z.frame = x.frame", parameters=(job_id, job_id))
        track_label_rows = hook.get_records(
            "SELECT x.path, y.name, z.points from public.engine_image x, public.engine_label y, public.engine_trackedshape z, public.engine_labeledtrack w, \
                public.engine_job a, public.engine_segment b, public.engine_task c \
                    WHERE w.job_id = %s AND z.track_id = w.id AND w.label_id = y.id AND a.id = %s \
                        AND a.segment_id = b.id AND b.task_id = c.id AND x.data_id = c.data_id AND z.frame = x.frame", parameters=(job_id, job_id))
    
    df = pd.DataFrame(data=shape_label_rows+track_label_rows, columns=['file_name', 'class', 'points'])
    df.drop_duplicates()
    df.to_csv(f"/home/sjpark/test{datetime.now().strftime('%Y%m%d')}.csv",header=False, mode='a')       
            
def start_func():
    print('DAG start')

def end_func():
    print('DAG finished')
    
start_task = PythonOperator(
    task_id = "start_task",
    python_callable = start_func,
    dag = dag
)

get_annotation = PythonOperator(
    task_id = "get_labels",
    python_callable = get_annotation_query,
    dag = dag
)

end_task = PythonOperator(
    task_id = "end_task",
    python_callable = end_func,
    dag = dag
)

start_task >> get_annotation >> end_task