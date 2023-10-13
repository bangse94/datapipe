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
    try:
        origin = pd.read_csv(f"/home/sjpark/test{datetime.now().strftime('%Y%m%d')}.csv", index_col=0)
        df = pd.concat([origin, df], ignore_index=True)
        df.drop_duplicates(subset=None, keep='first', inplace=True)
        df.reset_index()
    except:
        pass    
    df.to_csv(f"/home/sjpark/test{datetime.now().strftime('%Y%m%d')}.csv",header=True, mode='w')
    
def create_hdfs_table():
    hm = HiveServer2Hook(hiveserver2_conn_id = "hiveserver2_warehouse")
    hql = """
        CREATE EXTERNAL TABLE IF NOT EXIST labes(
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
today = datetime.now().strftime('%Y%m%d')
save_csv_hdfs = BashOperator(
    task_id = "save_hdfs",
    bash_command = "/home/sjpark/hadoop-3.2.4/bin/hdfs dfs -put /home/sjpark/test{{ execution_date.strftime('%Y%m%d') }}.csv /home/sjpark/warehouse",
    dag = dag
)

crate_hive_table = PythonOperator(
    task_id = "create_hive_table",
    python_callable = create_hdfs_table,
    dag = dag
)

spark = SparkSession \
    .builder \
        .appName("cvat-spark") \
            .config("spark.sql.warehouse.dir", "/home/sjpark/warehouse") \
                .enableHiveSupport() \
                    .getOrCreate()
                    
#df = spark.read.csv(f"/home/sjpark/test{datetime.now().strftime('%Y%m%d')}.csv", header=True)
#
#labels = df.select('file_name', 'class', 'points') \
#    .dropDuplicates(['file_name', 'class', 'points'])        
#
#labels.write.mode("append").insertInto("labels")
#
#spark_processing = SparkSubmitOperator(
#    task_id = "spark_processing",
#    application = "/home/sjpark/airflow/dags/spark_processing.py",
#    conn_id = "spark_master",
#    verbose = False,
#)

end_task = PythonOperator(
    task_id = "end_task",
    python_callable = end_func,
    dag = dag
)

#start_task >> get_annotation >> save_csv_hdfs >> crate_hive_table >> spark_processing >> end_task
start_task >> get_annotation >> save_csv_hdfs >> crate_hive_table >> end_task