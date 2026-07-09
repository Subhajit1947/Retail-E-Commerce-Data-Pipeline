from pyspark.sql.functions import current_timestamp,current_date,lit,concat_ws,col,sha2,round
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
import os

spark=SparkSession.builder \
    .appName("Retail Customer Data") \
    .getOrCreate()
    
order_details_df=spark.read.format("csv")\
                        .option("inferSchema","true")\
                        .option("header","true")\
                        .load(f"s3://{os.environ['S3_BUCKET']}/Bronze/order_details/")


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
        .parquet(f"s3://{os.environ['S3_BUCKET']}/Silver/order_details/")

else:
    print("No new records found in the source data. Skipping further processing.")

