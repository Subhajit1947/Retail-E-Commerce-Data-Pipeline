from pyspark.sql.functions import current_date,col,to_date,year,month
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
import os

spark=SparkSession.builder \
    .appName("Retail Customer Data") \
    .getOrCreate()
    
order_df=spark.read.format("csv")\
                        .option("inferSchema","true")\
                        .option("header","true")\
                        .load(f"s3://{os.environ['S3_BUCKET']}/Bronze/orders/")


if order_df.count()>0:
    orders_with_date=order_df.withColumn("orderDate",to_date(col("orderDate").cast(DateType())))
    renamed_orders = orders_with_date.withColumnRenamed("orderId","order_id ")\
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
        .parquet(f"s3://{os.environ['S3_BUCKET']}/Silver/orders/")



else:
        print("No new records found in the source data. Skipping further processing.")
