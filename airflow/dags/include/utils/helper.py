"""
Realistic E-commerce Data Generator
====================================

TWO MODES:
1. INITIAL LOAD (Run Once)
   python main.py --mode initial --bucket my-bucket
   
   Generates:
   - 10,000 customers (spread over past year)
   - 500 products (catalog)
   - ~182,500 historical orders (past year)
   - ~500K order details

2. DAILY PIPELINE (Run Daily via Airflow)
   python main.py --mode daily --date 2024-01-15 --bucket my-bucket
   
   Generates:
   - 30-200 new customers
   - 0-3 new products (weekly)
   - 0-25 price updates (weekly)
   - 500-2000 orders (from existing customers)

DESIGN PRINCIPLES:
- UUID for all IDs (no state, distributed-safe)
- Deterministic by date (idempotent, backfill-safe)
- Reads existing data from DWH (realistic)
- No state files (memory-safe)
"""
import argparse
import pandas as pd
from datetime import datetime, timedelta
import logging
from ..generators.customer_generator import CustomerGenerator
from ..generators.product_generator import ProductGenerator
from ..generators.order_generator import OrderGenerator
from .s3_helper import S3BronzeUploader

from ..config.data_config import INITIAL_LOAD
import os


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

LAUNCH_DATE = datetime(2025, 1, 1)


def run_initial_load(s3_bucket: str, aws_key: str = None, aws_secret: str = None):
    """
    INITIAL LOAD - Run once to populate historical data.
    
    This simulates a company that has been operating for 1 year.
    We generate all historical data and load it into the DWH.
    """
    print("=" * 70)
    print("🚀 INITIAL LOAD - Historical Data Generation")
    print("=" * 70)
    
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
    # 3. GENERATE HISTORICAL ORDERS (past year)
    # ==========================================
    print("\\n📋 Step 3: Generating Historical Orders...")
    order_gen = OrderGenerator(seed_date=base_date)
    
    all_orders = []
    all_order_details = []
    
    # Generate orders for each day of the past year
    for day_offset in range(365):
        order_date = base_date + timedelta(days=day_offset)
        
        # Only use customers who signed up before this date
        eligible_customers = customers_df[customers_df['signup_date'] <= order_date.date()]
        
        if len(eligible_customers) < 100:
            continue  # Not enough customers yet
        
        daily_orders, daily_details = order_gen.generate_daily_orders(
            target_date=order_date,
            customers_df=eligible_customers,
            products_df=products_df
        )
        
        if not daily_orders.empty:
            all_orders.append(daily_orders)
            all_order_details.append(daily_details)
        
        if (day_offset + 1) % 30 == 0:
            print(f"   Progress: {day_offset + 1}/365 days processed")
    
    # Combine all historical orders
    if all_orders:
        historical_orders = pd.concat(all_orders, ignore_index=True)
        historical_details = pd.concat(all_order_details, ignore_index=True)
        
        # Upload in monthly batches
        uploader.upload_csv(historical_orders, 'orders', '2025-01-01', 'orders_historical.csv')
        uploader.upload_csv(historical_details, 'order_details', '2025-01-01', 'order_details_historical.csv')
        
        print(f"\\n✅ Historical Orders: {len(historical_orders)} orders, {len(historical_details)} items")
    
    # ==========================================
    # SUMMARY
    # ==========================================
    print("\\n" + "=" * 70)
    print("✅ INITIAL LOAD COMPLETE")
    print("=" * 70)
    print(f"   Customers: {len(customers_df):,}")
    print(f"   Products: {len(products_df):,}")
    

def get_day_number(target_date: datetime) -> int:
    """Calculate business day number from date"""
    return max(1, (target_date - LAUNCH_DATE).days)

def run_daily_pipeline(s3_bucket: str, aws_key: str = None, aws_secret: str = None,**context):
    """
    DAILY PIPELINE - Run every day via Airflow.
    
    This is the real-world scenario:
    - Read existing customers/products from DWH
    - Generate new transactions
    - Handle updates (SCD Type 2)
    """
    target_date=context["ds"]
    target_date = datetime.strptime(target_date, "%Y-%m-%d")
    date_str = target_date.strftime('%Y-%m-%d')
    # date_str=target_date
    day_number=get_day_number(target_date)
    print("\\n" + "=" * 70)
    print(f"📅 DAILY PIPELINE - {date_str} (Day {day_number})")
    print("=" * 70)
    
    uploader = S3BronzeUploader(s3_bucket, aws_key, aws_secret)
    
    product_df_s3=uploader.read_csv("Bronze/products")
    
    # NOTE: In production, replace this with actual DWH query
    # For demonstration, we'll generate a sample of existing customers
    # In your Airflow DAG, use PostgresOperator or PythonOperator with psycopg2
    
    # Simulate reading from DWH (in production, query actual DWH)
    print("   (In production: SELECT * FROM dwh.dim_customers WHERE is_current = TRUE)")
    print("   (In production: SELECT * FROM dwh.dim_products WHERE is_current = TRUE)")
    
    # For now, we'll generate fresh customers/products for demo
    # In production, these come from DWH
    
    # ==========================================
    # 2. GENERATE NEW CUSTOMERS
    # ==========================================
    print("\\n📋 Step 2: Generating New Customers...")
    cust_gen = CustomerGenerator(seed_date=target_date)
    new_customers = cust_gen.generate_daily_new_customers(target_date, day_number)
    
    if not new_customers.empty:
        uploader.upload_csv(new_customers, 'customers', date_str, 'customers_new.csv')
    
    # ==========================================
    # 3. GENERATE CUSTOMER UPDATES (SCD Type 2)
    # ==========================================
    print("\\n📋 Step 3: Generating Customer Updates...")
    # In production: read existing customers from DWH, apply updates
    # For demo, we'll skip (implement when you have DWH connection)
    print("   (Requires DWH connection - implement in production)")
    
    # ==========================================
    # 4. GENERATE NEW PRODUCTS (Weekly)
    # ==========================================
    print("\\n📋 Step 4: Checking for New Products...")
    prod_gen = ProductGenerator(seed_date=target_date)
    
    new_products = pd.DataFrame()
    
    if day_number % 7 == 0:  # Weekly
        new_products = prod_gen.generate_new_products(target_date, count=3)
        if not new_products.empty:
            uploader.upload_csv(new_products, 'products', date_str, 'products_new.csv')
    else:
        print("   (Not a product update day)")
    
    
    # ==========================================
    # 5. GENERATE PRICE UPDATES (Weekly)
    # ==========================================
    print("\\n📋 Step 5: Checking for Price Updates...")
    
    price_updates = pd.DataFrame()
    if day_number % 7 == 3:  # Mid-week price update
        # In production: read existing products from DWH
        price_updates = prod_gen.generate_price_updates(
            product_df_s3, target_date, update_rate=0.05
        )
        if not price_updates.empty:
            uploader.upload_csv(price_updates, 'products', date_str, 'products_price_updates.csv')
    else:
        print("   (Not a price update day)")
    
    # ==========================================
    # 6. GENERATE DAILY ORDERS
    # ==========================================
    print("\\n📋 Step 6: Generating Daily Orders...")
    
    order_gen = OrderGenerator(seed_date=target_date)
    
    # In production: read from DWH
    # For demo: generate sample existing customers and products
    # These would normally come from DWH query
    
    # # Simulate existing customer base (grows over time)
    # existing_customer_count = min(10000, 1000 + day_number * 30)
    # print(f"   Simulating {existing_customer_count} active customers from DWH")
    
    # # Generate a representative sample of existing customers
    # existing_customers = cust_gen.generate_initial_customers(
    #     count=existing_customer_count,
    #     days_back=min(day_number, 365)
    # )
    
    # Use all products (catalog)
    all_customer=uploader.read_csv("Bronze/customers")
    all_products=uploader.read_csv("Bronze/products")
    # all_products = prod_gen.generate_initial_catalog(count=500)
    
    # Generate orders
    daily_orders, daily_details = order_gen.generate_daily_orders(
        target_date=target_date,
        customers_df=all_customer,
        products_df=all_products
    )
    
    if not daily_orders.empty:
        uploader.upload_csv(daily_orders, 'orders', date_str, 'orders.csv')
    
    if not daily_details.empty:
        uploader.upload_csv(daily_details, 'order_details', date_str, 'order_details.csv')
    
    # ==========================================
    # SUMMARY
    # ==========================================
    print("\\n" + "=" * 70)
    print("✅ DAILY PIPELINE COMPLETE")
    print("=" * 70)
    print(f"   New Customers: {len(new_customers)}")
    print(f"   New Products: {len(new_products)}")
    print(f"   Price Updates: {len(price_updates)}")
    print(f"   Orders: {len(daily_orders)}")
    print(f"   Order Details: {len(daily_details)}")
    
    print("=" * 70)





