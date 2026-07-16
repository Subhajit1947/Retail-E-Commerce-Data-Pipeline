from pyspark.sql.functions import current_timestamp,current_date,lit,concat_ws,col,sha2,round
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
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
    
order_details_df=spark.read.format("csv")\
                        .option("inferSchema","true")\
                        .option("header","true")\
                        .load(f"s3://{bucket}/Bronze/order_details/date={process_date}/")


if order_details_df.count()>0:
    renamed_order_details = order_details_df.withColumnRenamed("orderDetailsId","order_details_id ")\
                        .withColumnRenamed("orderId","order_id")\
                        .withColumnRenamed("productid","product_id")\
                        .withColumnRenamed("Quantity","product_quantity")\
                        .drop("op")
    
    

    #create dataframe with new columns using withColumn()
    order_details_final_df = renamed_order_details.withColumn("ingestion_date",current_date())

    order_details_final_df.write \
        .partitionBy("ingestion_date") \
        .mode("overwrite") \
        .parquet(f"s3://{bucket}/Silver/order_details/")

else:
    print("No new records found in the source data. Skipping further processing.")

