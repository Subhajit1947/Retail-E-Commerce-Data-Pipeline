from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime

from include.utils.helper import run_daily_pipeline
import os 
import pandas as pd

s3_bucket=os.environ["S3_BUCKET"]
aws_key=os.environ["AWS_ACCESS_KEY"]
aws_secret=os.environ["AWS_SECRET_KEY"]
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME','/opt/airflow')



def upload_to_s3(filename, key):
    hook = S3Hook()
    hook.load_file(filename=filename, key=key, bucket_name=s3_bucket, replace=True)


dag=DAG(
    dag_id="ecommerce_daily_pipeline",
    schedule="0 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False
)

task=PythonOperator(
    task_id="daily_data_load_dag",
    python_callable=run_daily_pipeline,
    op_kwargs={
        "s3_bucket":s3_bucket,
        "aws_key":aws_key,
        "aws_secret":aws_secret
    },
    dag=dag
)

customer_script_upload_task = PythonOperator(
    task_id= 'Cust_Script_To_S3',
    python_callable= upload_to_s3,
    op_kwargs=dict(
        filename = AIRFLOW_HOME+"/dags/include/silver_scripts/customer_transformation.py", 
        key = "Scripts/customer_transformation.py"
    )
)


product_script_upload_task = PythonOperator(
    task_id= 'Cust_Script_To_S3',
    python_callable= upload_to_s3,
    op_kwargs=dict(
        filename = AIRFLOW_HOME+"/dags/include/silver_scripts/product_transformation.py", 
        key = "Scripts/product_transformation.py"
    )
)

