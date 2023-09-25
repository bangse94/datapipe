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

dag = DAG(
    dag_id = "test-dag",
    start_date = days_ago(1),
    schedule_interval = "@once"
)

def task_test_query():
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
        for i in range(len(shape_label_rows)):
            print(shape_label_rows[i])
        for j in range(len(track_label_rows)):
            print(track_label_rows[j])
        
    # engine_labledshape.frame == engine_image.frame, engine_labeledtrack.frame == engine_image.frame
    # engine_job.segment_id -> engine_segment.task_id -> engine_task.data_id = engine_image.data_id -> engine_image.name
    
task_1 = PythonOperator(
    task_id = "run_query_with_python",
    python_callable = task_test_query,
    dag = dag
)

task_1