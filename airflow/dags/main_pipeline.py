from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
# from airflow.providers.standard.operators.empty import EmptyOperator
# from airflow.hooks.s3_hook import S3Hook

# from airflow.contrib.operators.emr_create_job_flow_operator import (
#     EmrCreateJobFlowOperator
# )
# from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
# from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
# from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
# from airflow.contrib.operators.emr_terminate_job_flow_operator import (
#     EmrTerminateJobFlowOperator
# )

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor

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


JOB_FLOW_OVERRIDES = {
    "Name": "Retail Batch",
    "ReleaseLabel": "emr-6.4.0",
    "Applications": [{"Name": "Spark"}],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm4.xlarge',
                'InstanceCount': 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    'VisibleToAllUsers': True
}

SPARK_STEPS = [
    {
        "Name": "{{params.BATCH_NAME}}",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "s3://{{ params.BUCKET_NAME }}/{{ params.SCRIPT_KEY }}",
                "--bucket",
                "{{ params.BUCKET_NAME }}"
            ],
        },
    },
]



dag=DAG(
    dag_id="ecommerce_daily_pipeline",
    schedule="0 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False
)

generate_data_and_upload_to_s3=PythonOperator(
    task_id="daily_data_load_dag",
    python_callable=run_daily_pipeline,
    op_kwargs={
        "s3_bucket":s3_bucket,
        "aws_key":aws_key,
        "aws_secret":aws_secret
    },
    dag=dag
)

order_detail_script_upload_task = PythonOperator(
    task_id= 'Order_Details_Script_To_S3',
    python_callable= upload_to_s3,
    op_kwargs=dict(
        filename = AIRFLOW_HOME+"/dags/include/silver_scripts/orderdetails_transformation.py", 
        key = "Scripts/orderdetails_transformation.py"
    ),
    dag=dag
)
order_script_upload_task = PythonOperator(
    task_id= 'Order_Script_To_S3',
    python_callable= upload_to_s3,
    op_kwargs=dict(
        filename = AIRFLOW_HOME+"/dags/include/silver_scripts/order_tranformation.py", 
        key = "Scripts/order_transformation.py"
    ),
    dag=dag
)
customer_script_upload_task = PythonOperator(
    task_id= 'Cust_Script_To_S3',
    python_callable= upload_to_s3,
    op_kwargs=dict(
        filename = AIRFLOW_HOME+"/dags/include/silver_scripts/customer_transformation.py", 
        key = "Scripts/customer_transformation.py"
    ),
    dag=dag
)


product_script_upload_task = PythonOperator(
    task_id= 'Product_Script_To_S3',
    python_callable= upload_to_s3,
    op_kwargs=dict(
        filename = AIRFLOW_HOME+"/dags/include/silver_scripts/product_transformation.py", 
        key = "Scripts/product_transformation.py"
    ),
    dag=dag
)

create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="Create_EMR_Cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        emr_conn_id="emr_default",
        dag=dag
    )

is_emr_cluster_created=EmrJobFlowSensor(
    task_id="Is_EMR_Created",
    job_flow_id="{{task_instance.xcom_pull(task_ids='Create_EMR_Cluster',key='return_value)}}",
    target_states={"WAITING"},
    timeout=3600,
    poke_interval=5,
    mode='poke',
    aws_conn_id="aws_default",
    dag=dag
)


order_details_silver_job = EmrAddStepsOperator(
        task_id="Submitting_Spark_Job_Order_Details",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_Cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=SPARK_STEPS,
        params={ # these params are used to fill the paramterized values in SPARK_STEPS json
            "BUCKET_NAME": s3_bucket,
            "SCRIPT_KEY": "Scripts/orderdetails_transformation.py",
            "BATCH_NAME": "Order Details Silver Batch",
        },
        dag=dag
    )


order_silver_job = EmrAddStepsOperator(
        task_id="Submitting_Spark_Job_Order",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_Cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=SPARK_STEPS,
        params={
            "BUCKET_NAME": s3_bucket,
            "SCRIPT_KEY": "Scripts/order_transformation.py",
            "BATCH_NAME": "Order Silver Batch"
        },
        dag=dag
)
product_silver_job = EmrAddStepsOperator(
        task_id="Submitting_Spark_Job_Product",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_Cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=SPARK_STEPS,
        params={
            "BUCKET_NAME": s3_bucket,
            "SCRIPT_KEY": "Scripts/product_transformation.py",
            "BATCH_NAME": "Product Silver Batch",
        },
        dag=dag
    )
customer_silver_job  = EmrAddStepsOperator(
    task_id="Submitting_Spark_Job_customer",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_Cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    params={
        "BUCKET_NAME": s3_bucket,
        "SCRIPT_KEY": "Scripts/customer_transformation.py",
        "BATCH_NAME":"Customer Silver Batch"
    },
    dag=dag
)


is_order_job_completed = EmrStepSensor(
    task_id="Running_Spark_Order_Job",
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_Cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Submitting_Spark_Job_Order', key='return_value')[0] }}",
    aws_conn_id="aws_default",
    dag=dag
)

is_order_details_job_completed = EmrStepSensor(
    task_id="Running_Spark_Order_Details_Job",
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_Cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Submitting_Spark_Job_Order_Details', key='return_value')[0] }}",
    aws_conn_id="aws_default",
    dag=dag
)
is_product_job_completed = EmrStepSensor(
    task_id="Running_Spark_Product_Job",
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_Cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Submitting_Spark_Job_Product', key='return_value')[0] }}",
    aws_conn_id="aws_default",
    dag=dag
)
is_Customer_job_completed = EmrStepSensor(
    task_id="Running_Spark_Customer_Job",
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_Cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Submitting_Spark_Job_customer', key='return_value')[0] }}",
    aws_conn_id="aws_default",
    dag=dag
)

terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="Terminate_EMR_Cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_Cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        trigger_rule="all_done"
    )


generate_data_and_upload_to_s3>>[
    order_detail_script_upload_task,
    order_script_upload_task,
    customer_script_upload_task,
    product_script_upload_task
]>>create_emr_cluster>>is_emr_cluster_created>>[
    order_details_silver_job,
    order_silver_job,
    product_silver_job,
    customer_silver_job
]

order_details_silver_job >> is_order_details_job_completed >> terminate_emr_cluster
order_silver_job >> is_order_job_completed >> terminate_emr_cluster
product_silver_job >> is_product_job_completed >> terminate_emr_cluster
customer_silver_job >> is_Customer_job_completed >> terminate_emr_cluster


