from pyspark.sql.functions import current_timestamp,current_date,lit,concat_ws,col,sha2,split,element_at
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
import sys

def get_arg(flag, default=None):
    if flag in sys.argv:
        return sys.argv[sys.argv.index(flag) + 1]
    return default

bucket = get_arg("--bucket")

spark=SparkSession.builder \
    .appName("Retail Customer Data") \
    .getOrCreate()
    
customer_df=spark.read.format("csv")\
                        .option("inferSchema","true")\
                        .option("header","true")\
                        .load(f"s3://{bucket}/Bronze/customers/")
if customer_df.count()>0:
    renamed_customer=customer_df.withColumnRenamed("customerId","customer_id")\
        .withColumnRenamed("op","cdc_operation")\
        .withColumnRenamed("name","cust_name")\
        .withColumnRenamed("phone","cust_phone")\
        .withColumnRenamed("address","cust_address")\
        .withColumnRenamed("country","cust_country")\
        .withColumnRenamed("city","cust_city")\
        .withColumnRenamed("email","cust_email")\
    
    current_ts=current_timestamp()
    record_end_ts=lit('9999-12-31').cast(TimestampType())
    active_flag=lit(1)
    concatenated_customer_fields = concat_ws(''
                                        , col("cust_phone")
                                        , col("cust_address")
                                        , col("cust_country")
                                        , col("cust_city")
                                        , col("cust_name"))
    
    customer_final_df = renamed_customer.withColumn("hash_value",sha2(concatenated_customer_fields, 256))\
                        .withColumn("record_start_ts",current_ts)\
                        .withColumn("record_end_ts",record_end_ts)\
                        .withColumn("ingestion_date", current_date())\
                        .withColumn("active_flag",active_flag)\
                        .withColumn("cust_first_name", element_at(split(col("cust_name"), " "), 1))\
                        .withColumn("cust_last_name", element_at(split(col("cust_name"), " "), 2))\
                        .drop("cust_name")
    
    customer_final_df.write \
        .partitionBy("ingestion_date") \
        .mode("overwrite") \
        .parquet(f"s3://{bucket}/Silver/customers/")

else:
    print("No new records found in the source data. Skipping further processing.")
    

