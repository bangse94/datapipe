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
    
    for job_id in job_ids:
        shape_label_rows = hook.get_records("SELECT x.label_id FROM public.engine_labeledshape x WHERE x.job_id = %s", parameters=(job_id))
        #track_label_rows = hook.get_records("SELECT id, label_id points FROM public.engine_trackedshape WHERE job_id = %s", parameters=(job_id))
        track_label_rows = hook.get_records(
            "SELECT a.label_id FROM public.engine_labeledtrack a JOIN public.engine_trackedshape b ON a.id = b.track_id WHERE a.job_id = %s", parameters=(job_id)
        )
        print("shape",shape_label_rows)
        print("track",track_label_rows)
        
        for shape_label_row in shape_label_rows:
            annotation = hook.get_records("SELECT x.name, y.points FROM public.engine_label x, public.engine_labeledshape y WHERE x.id = y.label_id AND y.label_id = %s", parameters=([shape_label_row[0]]))
            frame_num = hook.get_records("SELECT x.frame FROM public.engine_labeledshape x WHERE x.label_id = %s", parameters=([shape_label_row[0]]))
            segment_id = hook.get_records("SELECT x.segment_id FROM public.engine_job x WHERE x.id = %s", parameters=([job_id]))
            task_id = hook.get_records("SELECT x.task_id FROM public.engine_segment x WHERE x.id = %s", parameters=([segment_id]))
            data_id = hook.get_records("SELECT x.data_id FROM public.engine_task x WHERE x.id = %s", parameters=([task_id]))
            file_name = hook.get_records("SELECT x.path FROM public.engine_image x WHERE x.data_id = %s and x.frame = %s", parameters=([data_id], [frame_num]))
            print(file_name, annotation)
        for track_label_row in track_label_rows:
            annotation = hook.get_records("SELECT x.name, z.points FROM public.engine_label x, public.engine_labeledtrack y, public.engine_trackedshape z WHERE x.id = y.label_id and z.track_id = y.id and y.label_id = %s", parameters=([track_label_row[0]]))
            frame_num = hook.get_records("SELECT x.frame FROM public.engine_labeledtrack.frame x WHERE x.label_id = %s", parameters([track_label_row[0]]))
            segment_id = hook.get_records("SELECT x.segment_id FROM public.engine_job x WHERE x.id = %s", parameters=([job_id]))
            task_id = hook.get_records("SELECT x.task_id FROM public.engine_segment x WHERE x.id = %s", parameters=([segment_id]))
            data_id = hook.get_records("SELECT x.data_id FROM public.engine_task x WHERE x.id = %s", parameters=([task_id]))
            file_name = hook.get_records("SELECT x.path FROM public.engine_image x WHERE x.data_id = %s and x.frame = %s", parameters=([data_id], [frame_num]))
            print(file_name, annotation)
        
    # engine_labledshape.frame == engine_image.frame, engine_labeledtrack.frame == engine_image.frame
    # engine_job.segment_id -> engine_segment.task_id -> engine_task.data_id = engine_image.data_id -> engine_image.name
    
task_1 = PythonOperator(
    task_id = "run_query_with_python",
    python_callable = task_test_query,
    dag = dag
)

task_1