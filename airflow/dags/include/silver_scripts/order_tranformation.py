from pyspark.sql.functions import current_date,col,to_date,year,month
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
import sys

def get_arg(flag, default=None):
    if flag in sys.argv:
        return sys.argv[sys.argv.index(flag) + 1]
    return default

bucket = get_arg("--bucket")
process_date=get_arg("--process-date")
if not bucket:
    raise ValueError("Missing required --bucket argument")

spark=SparkSession.builder \
    .appName("Retail Customer Data") \
    .getOrCreate()
    
order_df=spark.read.format("csv")\
                        .option("inferSchema","true")\
                        .option("header","true")\
                        .load(f"s3://{bucket}/Bronze/orders/date={process_date}")


if order_df.count()>0:
    orders_with_date=order_df.withColumn("orderDate",to_date(col("orderDate").cast(DateType())))
    renamed_orders = orders_with_date.withColumnRenamed("orderId","order_id")\
                        .withColumnRenamed("orderCustomerId","order_customer_id")\
                        .withColumnRenamed("orderDate","order_date")\
                        .withColumnRenamed("paymentMethod","payment_method")\
                        .withColumnRenamed("orderPlatform", "order_platform")\
                        .drop("op")
    orders_final_df=renamed_orders\
                    .withColumn("order_year", year(col("order_date")))\
                    .withColumn("order_month", month(col("order_date")))\
                    .withColumn("ingestion_date", current_date())\
                    .orderBy(col("order_date").desc())
    orders_final_df.write \
        .partitionBy("order_year")\
        .mode("overwrite") \
        .parquet(f"s3://{bucket}/Silver/orders/")



else:
        print("No new records found in the source data. Skipping further processing.")
