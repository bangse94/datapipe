import os
from os.path import expanduser, join, abspath
import io

import pandas as pd

from datetime import datetime
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.connection import Connection

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.hive_hooks import HiveServer2Hook
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_csv

con = Connection(
    conn_id = 'cvat_postgres',
    conn_type = 'postgres',
    login = 'root',
    password = ''
)

dag = DAG(
    dag_id = "cvat-spark-dag",
    start_date = days_ago(2),
    schedule_interval = "@daily"
)

def get_annotation_query(**context):
    hook = PostgresHook(postgres_conn_id='cvat_postgres')
    
    #job_ids = hook.get_records("SELECT x.id FROM public.engine_job x WHERE x.stage = 'acceptance' and x.updated_date < current_date and x.updated_date >= current_date-1")
    job_ids = hook.get_records("""
                            SELECT x.id FROM public.engine_job x, public.engine_segment y, public.engine_task z, public.engine_project w
                            where z.project_id = 32 and w.id = 32
                            and y.task_id = z.id
                            and x.segment_id = y.id
                            and x.stage = 'acceptance'
                            and x.id = 5647
                            """)
    res = []
    for job_id in job_ids:
        # file_name, class_name, bbox
        shape_label_rows = hook.get_records(
            "SELECT x.path, y.name, z.points from public.engine_image x, public.engine_label y, public.engine_labeledshape z, \
                public.engine_job a, public.engine_segment b, public.engine_task c \
                    WHERE z.job_id=%s AND y.id = z.label_id AND a.id = %s AND a.segment_id = b.id \
                        AND b.task_id = c.id AND x.data_id = c.data_id AND z.frame = x.frame", parameters=(job_id, job_id))
        
        tracked_shapes = hook.get_records(
            """
                SELECT x.path, y.name, z.track_id, z.points, z.outside, z.frame FROM public.engine_image x, public.engine_label y, public.engine_trackedshape z, public.engine_labeledtrack w,
                public.engine_job a, public.engine_segment b, public.engine_task c
                WHERE w.job_id = %s AND z.track_id = w.id AND w.label_id = y.id AND a.id = %s
                AND a.segment_id = b.id AND b.task_id = c.id AND x.data_id = c.data_id AND z.frame = x.frame ORDER BY z.frame
            """
            , parameters=(job_id, job_id)
        )
        
        f_path = hook.get_records(
            """
                SELECT x.path, x.frame FROM public.engine_image x, public.engine_job a, public.engine_task b, public.engine_segment c
                WHERE a.id = %s AND a.id = c.id AND x.data_id = b.data_id AND c.task_id = b.id ORDER BY x.frame
            """
            , parameters=(job_id)
        )
        
        prev_track_shape = {} # {id : [frame, points]}
        frame_path = {} # {frame : path}
        
        for idx in range(len(f_path)):
            frame, path = f_path[idx][1], f_path[idx][0]
            frame_path[int(frame)] = path
        
        track_label_rows = []
        for idx_shape in range(len(tracked_shapes)):
            path, name, track_id, points, outside, frame = tracked_shapes[idx_shape]
            if outside == False:
                track_label_rows.append([path, name, points])
                if track_id in prev_track_shape:
                    track_label_rows += [[frame_path[int(frame) - (i + 1)], name, prev_track_shape[track_id][1]] for i in range(abs(int(frame) - int(prev_track_shape[track_id][0]) - 1))]
                prev_track_shape[track_id] = [frame, points]
            elif outside == True:
                if track_id in prev_track_shape:
                    if abs(int(frame) - int(prev_track_shape[track_id][0])) > 1:
                        track_label_rows += [[frame_path[int(frame) - (i + 1)], name, points] for i in range(abs(int(frame) - int(prev_track_shape[track_id][0]) - 1))]
                    prev_track_shape[track_id] = [frame, points]
            
        
        #track_label_rows = hook.get_records(
        #    "SELECT x.path, y.name, z.points from public.engine_image x, public.engine_label y, public.engine_trackedshape z, public.engine_labeledtrack w, \
        #        public.engine_job a, public.engine_segment b, public.engine_task c \
        #            WHERE w.job_id = %s AND z.track_id = w.id AND w.label_id = y.id AND a.id = %s \
        #                AND a.segment_id = b.id AND b.task_id = c.id AND x.data_id = c.data_id AND z.frame = x.frame", parameters=(job_id, job_id))
        res = res+shape_label_rows
        res = res+track_label_rows
        
    df = pd.DataFrame(data=res, columns=['file_name', 'class', 'points'])
    if os.path.exists(f"/home/sjpark/validated_{datetime.now().strftime('%Y%m%d')}.csv"):
        df.to_csv(f"/home/sjpark/validated_{datetime.now().strftime('%Y%m%d')}.csv",header=True, mode = 'w')
    else:
        df.to_csv(f"/home/sjpark/validated_{datetime.now().strftime('%Y%m%d')}.csv",header=True, mode="a")
    
def create_hdfs_table():
    hm = HiveServer2Hook(hiveserver2_conn_id = "hiveserver2_warehouse")
    hql = """
        CREATE EXTERNAL TABLE IF NOT EXIST labels(
            file_name STRING,
            class STRING,
            points STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
    """
            
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

#remove_csv_hdfs = BashOperator(
#    task_id = "unvalid_remove_hdfs",
#    bash_command= f"/home/sjpark/hadoop-3.2.4/bin/hdfs dfs -rm /home/sjpark/warehouse/validated_{datetime.now().strftime('%Y%m%d')}.csv",
#   dag = dag
#)

save_csv_hdfs = BashOperator(
    task_id = "save_hdfs",
    bash_command = f"/home/sjpark/hadoop-3.2.4/bin/hdfs dfs -put /home/sjpark/validated_{datetime.now().strftime('%Y%m%d')}.csv /home/sjpark/warehouse",
    dag = dag
)

create_hive_table = PythonOperator(
    task_id = "create_hive_table",
    python_callable = create_hdfs_table,
    dag = dag
)

end_task = PythonOperator(
    task_id = "end_task",
    python_callable = end_func,
    dag = dag
)

start_task >> get_annotation >> save_csv_hdfs >> create_hive_table >> end_task
#start_task >> get_annotation >> remove_csv_hdfs >> save_csv_hdfs >> create_hive_table >> end_task