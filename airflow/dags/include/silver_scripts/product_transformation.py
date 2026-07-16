from pyspark.sql.functions import current_timestamp,current_date,lit,concat_ws,col,sha2,round
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
    
product_df=spark.read.format("csv")\
                        .option("inferSchema","true")\
                        .option("header","true")\
                        .load(f"s3://{bucket}/Bronze/products/")

if product_df.count()>0:
    renamed_products=product_df.withColumnRenamed("productId","product_id")\
        .withColumnRenamed("productName","product_name")\
        .withColumnRenamed("brandName","brand_name")\
        .withColumnRenamed("op","cdc_operation")\
        .withColumnRenamed("productDescription","product_description")\
        .withColumnRenamed("productCategory","product_category")\
        .withColumnRenamed("price","product_price")
    
    concatenated_product_fields=concat_ws("",
                                        col("product_name"),
                                        col("brand_name"),
                                        col("product_description"),
                                        col("product_category"),
                                        col("product_price")
                                        )
    
    current_ts=current_timestamp()
    record_end_ts=lit('9999-12-31').cast(TimestampType())
    active_flag=lit(1)

    product_final_df = renamed_products.withColumn("hash_value",sha2(concatenated_product_fields, 256))\
                        .withColumn("record_start_ts",current_ts)\
                        .withColumn("record_end_ts",record_end_ts)\
                        .withColumn("ingestion_date",current_date())\
                        .withColumn("active_flag",active_flag)\
                        .withColumn("product_price",round(col("product_price"),2))

    product_final_df.write \
        .partitionBy("ingestion_date") \
        .mode("overwrite") \
        .parquet(f"s3://{bucket}/Silver/products/")


else:
    print("No records found in the source data. Skipping further processing.")
    

