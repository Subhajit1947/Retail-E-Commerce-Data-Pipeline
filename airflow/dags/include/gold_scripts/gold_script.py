from pyspark.sql.functions import current_timestamp,current_date,lit,concat_ws,col,sha2,split,element_at
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
import sys
from datetime import datetime
def get_arg(flag, default=None):
    if flag in sys.argv:
        return sys.argv[sys.argv.index(flag) + 1]
    return default

bucket = get_arg("--bucket")
process_date=get_arg("--process-date")
ec2_ip=get_arg("--ec2-ip")
database=get_arg("--database")
db_username=get_arg("--db-username")
db_password=get_arg("--db-password")

spark=SparkSession.builder \
    .appName("Retail Customer Data Gold Layer") \
    .getOrCreate()
    

customer_df =spark.read.format("parquet")\
        .load(f"s3://{bucket}/Silver/customers/ingestion_date={process_date}")


#comment order_df and order_details_df for initial load
order_year=datetime.strptime(process_date,"%Y-%m-%d").year
order_df =spark.read.format("parquet")\
        .load(f"s3://{bucket}/Silver/orders/order_year={order_year}")

order_details_df=spark.read.format("parquet")\
        .load(f"s3://{bucket}/Silver/order_details/ingestion_date={process_date}")

jdbc_url = f"jdbc:postgresql://{ec2_ip}:5432/{database}"
connection_properties = {
    "user": db_username,
    "password": db_password,
    "driver": "org.postgresql.Driver",
}

customer_df.write.jdbc(
    url=jdbc_url,
    table="sales.stage_dim_customer",
    mode="append",
    properties=connection_properties,
)
try:
    product_df =spark.read.format("parquet")\
        .load(f"s3://{bucket}/Silver/products/ingestion_date={process_date}")
    product_df.write.jdbc(
        url=jdbc_url,
        table="sales.stage_dim_product",
        mode="append",
        properties=connection_properties,
    )
except Exception as e:
    print("no new or updated product")
order_df.write.jdbc(
    url=jdbc_url,
    table="sales.fact_orders",
    mode="append",
    properties=connection_properties,
)

order_details_df.write.jdbc(
    url=jdbc_url,
    table="sales.fact_order_details",
    mode="append",
    properties=connection_properties,
)
