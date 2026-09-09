from pyspark.sql.functions import current_timestamp,current_date,lit,concat_ws,col,sha2,split,element_at
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
import sys
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


order_df =spark.read.format("parquet")\
        .option("basePath",f"s3://{bucket}/Silver/orders/")\
        .load(f"s3://{bucket}/Silver/orders/order_year={process_date[:4]}/ingestion_date={process_date}")

order_df=order_df.withColumn("ingestion_date",lit(process_date))


order_details_df=spark.read.format("parquet")\
        .load(f"s3://{bucket}/Silver/order_details/ingestion_date={process_date}")
order_details_df=order_details_df.withColumn("ingestion_date",lit(process_date))



jdbc_url = f"jdbc:postgresql://{ec2_ip}:5432/{database}"
connection_properties = {
    "user": db_username,
    "password": db_password,
    "driver": "org.postgresql.Driver",
}


def delete_by_date(table, date):
    # We use the Spark JVM to execute a raw SQL delete before appending
    connection = spark._jsparkSession._jvm.java.sql.DriverManager.getConnection(jdbc_url, connection_properties)
    stmt = connection.createStatement()
    stmt.executeUpdate(f"DELETE FROM {table} WHERE ingestion_date = '{date}'")
    stmt.close()
    connection.close()



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

delete_by_date("sales.fact_orders", process_date)
delete_by_date("sales.fact_order_details", process_date)

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
