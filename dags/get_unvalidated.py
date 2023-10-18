import os
from os.path import expanduser, join, abspath
import io

import pandas as pd

from datetime import datetime

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
    schedule_interval   = "@once"
)

def get_annotations():
    hook = PostgresHook(postgres_conn_id='cvat_postgres')
    job_ids = hook.get_records(
        """
            SELECT x.id FROM public.engine_job x, public.engine_segment y, public.engine_task z, public.engine_project w
            WHERE z.project_id = 32 AND w.id = 32
            AND y.task_id = z.id
            AND x.segment_id = y.id
        """
    )#
    
    res = []
    for job_id in job_ids:
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
        res = res+shape_label_rows
        res = res+track_label_rows
        
    df = pd.DataFrame(data=res, columns=['file_name', 'class', 'points'])
    df.to_csv(f"/home/sjpark/unvalidated_labels{ datetime.now().strftime('%Y%m%d') }.csv", header=False, mode = 'a')
    print('job ids : ', len(job_ids))
    print("shape : ", len(shape_label_rows))
    print("track : ", len(track_label_rows))

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
    bash_command= "/home/sjpark/hadoop-3.2.4/bin/hdfs dfs -rm /home/sjpark/warehouse/unvalidated_labels{{ execution_date.strftime('%Y%m%d') }}.csv",
    dag = dag
)

save_csv_hdfs = BashOperator(
    task_id = "unvaild_save_hdfs",
    bash_command = "/home/sjpark/hadoop-3.2.4/bin/hdfs dfs -put /home/sjpark/unvalidated_labels{{ execution_date.strftime('%Y%m%d') }}.csv /home/sjpark/warehouse",
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