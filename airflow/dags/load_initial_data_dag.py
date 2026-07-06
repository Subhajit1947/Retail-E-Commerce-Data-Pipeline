
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import logging
import os
import pandas as pd
from include.generators.customer_generator import CustomerGenerator
from include.generators.product_generator import ProductGenerator
from include.generators.order_generator import OrderGenerator
from include.utils.s3_helper import S3BronzeUploader
from include.config.data_config import INITIAL_LOAD


logger = logging.getLogger(__name__)

s3_bucket=os.environ["S3_BUCKET"]
aws_key=os.environ["AWS_ACCESS_KEY"]
aws_secret=os.environ["AWS_SECRET_KEY"]
AIRFLOW_HOME='/opt/airflow'


def run_initial_load(s3_bucket: str, aws_key: str = None, aws_secret: str = None):
    """
    INITIAL LOAD - Run once to populate historical data.
    
    This simulates a company that has been operating for 1 year.
    We generate all historical data and load it into the DWH.
    """
    print("=" * 70)
    print("🚀 INITIAL LOAD - Historical Data Generation")
    print("=" * 70)
    print(s3_bucket, aws_key, aws_secret)
    uploader = S3BronzeUploader(s3_bucket, aws_key, aws_secret)
    base_date = datetime(2025, 1, 1)  # Historical start
    
    # ==========================================
    # 1. GENERATE CUSTOMERS (10K over past year)
    # ==========================================
    print("\\n📋 Step 1: Generating Customers...")
    cust_gen = CustomerGenerator(seed_date=base_date)
    customers_df = cust_gen.generate_initial_customers(
        count=INITIAL_LOAD['customer_count'],
        days_back=365
    )
    
    # Upload to S3 Bronze
    uploader.upload_csv(customers_df, 'customers', '2025-01-01', 'customers_initial.csv')
    
    # ==========================================
    # 2. GENERATE PRODUCTS (500 catalog items)
    # ==========================================
    print("\\n📋 Step 2: Generating Products...")
    prod_gen = ProductGenerator(seed_date=base_date)
    products_df = prod_gen.generate_initial_catalog(
        count=INITIAL_LOAD['product_count']
    )
    
    uploader.upload_csv(products_df, 'products', '2025-01-01', 'products_initial.csv')
    
    # ==========================================
    # SUMMARY
    # ==========================================
    print("\\n" + "=" * 70)
    print("✅ INITIAL LOAD COMPLETE")
    print("=" * 70)
    print(f"   Customers: {len(customers_df):,}")
    print(f"   Products: {len(products_df):,}")
    


dag=DAG(
    dag_id="ecommerce_initial_load",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False
)

task=PythonOperator(
    task_id="initial_data_load_dag",
    python_callable=run_initial_load,
    op_kwargs={
        "s3_bucket":s3_bucket,
        "aws_key":aws_key,
        "aws_secret":aws_secret
    },
    dag=dag
)


