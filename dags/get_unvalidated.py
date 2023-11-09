import os
from os.path import expanduser, join, abspath
import io

import pandas as pd

from datetime import datetime
from collections import deque, defaultdict

from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.connection import Connection
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.hive_hooks import HiveServer2Hook
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_csv

db_conn = Connection(
    conn_id     = 'cvat_postgres',
    conn_type   = 'postgres',
    login       = 'root',
    password    = ''
)

dag = DAG(
    dag_id              = "get-unvalidated",
    start_date          = days_ago(2),
    schedule_interval   = "@daily"
)

def get_annotations():
    hook = PostgresHook(postgres_conn_id='cvat_postgres')
    job_ids = hook.get_records(
        """
            SELECT x.id FROM public.engine_job x, public.engine_segment y, public.engine_task z, public.engine_project w
            WHERE  (z.project_id = 32 or z.project_id = 34) and (w.id = 32 or w.id = 34)
            AND z.project_id = w.id
            AND y.task_id = z.id
            AND x.segment_id = y.id
        """
    )#
    
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
        
        start_stop_frame = hook.get_records(
            '''
                SELECT x.start_frame, x.stop_frame FROM public.engine_segment x, public.engine_job y
                WHERE x.id = y.segment_id AND x.id = %s
            '''
            ,parameters=(job_id)
        )
        
        deleted_frame = hook.get_records(
            '''
                SELECT x.deleted_frames FROM public.engine_data x, public.engine_job y, public.engine_task z, public.engine_segment w
                WHERE y.id = %s AND z.data_id = x.id AND w.task_id = z.id AND y.segment_id = w.id 
            '''
            ,parameters=(job_id)
        )
        
        deleted_frame = deleted_frame[0][0].split(',')
        
        start_frame, stop_frame = start_stop_frame[0]
        
        track_deq = deque(tracked_shapes)
        track_label_rows = []
        prev_label_rows = []
        curr_label_rows = []
        last_del_frame = -1
        
        frame_labels = defaultdict(lambda : [])
        
        while track_deq:
            shape = track_deq.popleft()
            path, name, _, points, _, frame = shape
            frame_labels[frame].append(shape)
        frame_cnt = 0
        for frame_idx in range(start_frame, stop_frame - start_frame + 1):
            temp_label_rows = []
            if str(frame_idx) in deleted_frame:
                last_del_frame = frame_idx
                continue
            if frame_idx == 0:
                curr_label_rows = frame_labels[frame_idx]
                for curr_idx, curr_label_row in enumerate(curr_label_rows):
                    curr_path, curr_name, curr_track_id, curr_points, curr_outside, curr_frame = curr_label_row
                    if curr_outside == False:
                        temp_label_rows.append((curr_path, curr_name, curr_track_id, curr_points, curr_outside, curr_frame))
                    
                curr_label_rows = temp_label_rows.copy()
                frame_labels[frame_idx] = curr_label_rows.copy()
                frame_cnt = frame_idx
                continue
            if frame_idx == last_del_frame + 1:
                curr_label_rows = frame_labels[frame_idx]
                prev_label_rows = frame_labels[frame_cnt]
            else:    
                curr_label_rows = frame_labels[frame_idx]
                prev_label_rows = frame_labels[frame_idx-1]
            
            prev_append_idxs = [0] * len(prev_label_rows)
            for prev_idx, prev_label_row in enumerate(prev_label_rows):
                _, prev_name, prev_track_id, prev_points, prev_outside, _ = prev_label_row
                for curr_idx, curr_label_row in enumerate(curr_label_rows):
                    curr_path, curr_name, curr_track_id, curr_points, curr_outside, curr_frame = curr_label_row
                    if prev_track_id == curr_track_id and curr_outside == False:
                        temp_label_rows.append((curr_path, curr_name, curr_track_id, curr_points, curr_outside, curr_frame))
                        break
                    elif prev_track_id == curr_track_id and curr_outside == True:
                        break
                else:
                    temp_label_rows.append((f_path[frame_idx][0], prev_name, prev_track_id, prev_points, prev_outside, frame_idx))
                    
            for curr_idx, curr_label_row in enumerate(curr_label_rows):
                curr_path, curr_name, curr_track_id, curr_points, curr_outside, curr_frame = curr_label_row
                if curr_outside == False:
                    temp_label_rows.append((curr_path, curr_name, curr_track_id, curr_points, curr_outside, curr_frame))
                    
            temp_label_rows = list(set(map(tuple, temp_label_rows)))
                    
            curr_label_rows = temp_label_rows.copy()
            frame_labels[frame_idx] = curr_label_rows.copy()
            frame_cnt = frame_idx
        for k, v in frame_labels.items():
            for row in v:
                path, name, _, points, _, frame = row
                appended = (path, name, points)
                track_label_rows.append(appended)
            
        res = res+shape_label_rows
        res = res+track_label_rows
        
    df = pd.DataFrame(data=res, columns=['file_name', 'class', 'points'])
    if os.path.exists("/home/sjpark/all_labels.csv"):
        df.to_csv(f"/home/sjpark/all_labels.csv", header=False, mode = 'w')
    else:
        df.to_csv(f"/home/sjpark/all_labels.csv", header=False, mode="a")

def create_hive_table():
    hm = HiveServer2Hook(hiveserver2_conn_id="hiveserver2_warehouse")
    hql = """
        CREATE EXTERNAL TABLE IF NOT EXIST unvalid(
            file_name STRING,
            class STRING,
            points STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        SORTED AS TEXTFILE
    """
    
def start_func():
    print('DAG start')

def end_func():
    print('DAG finished')
    
start_task = PythonOperator(
    task_id = "get_unvalid_start_task",
    python_callable = start_func,
    dag = dag
)

get_annotation = PythonOperator(
    task_id = "get_unvalid_labels",
    python_callable = get_annotations,
    dag = dag
)

remove_csv_hdfs = BashOperator(
    task_id = "unvalid_remove_hdfs",
    bash_command= "/home/sjpark/hadoop-3.2.4/bin/hdfs dfs -rm /home/sjpark/warehouse/all_labels.csv",
    dag = dag
)

save_csv_hdfs = BashOperator(
    task_id = "unvaild_save_hdfs",
    bash_command = "/home/sjpark/hadoop-3.2.4/bin/hdfs dfs -put /home/sjpark/all_labels.csv /home/sjpark/warehouse",
    dag = dag
)

create_hive_table = PythonOperator(
    task_id = "unvalid_create_hive_table",
    python_callable = create_hive_table,
    dag = dag
)

spark = SparkSession.builder.appName("get_unvalidated").config("spark.sql.warehouse.dir", "/home/sjpark/warehouse").enableHiveSupport().getOrCreate()

end_task = PythonOperator(
    task_id = "end_task",
    python_callable = end_func,
    dag = dag
)

start_task >> get_annotation >> remove_csv_hdfs >> save_csv_hdfs >> create_hive_table >> end_task