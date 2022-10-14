from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession \
    .builder \
    .appName("DemoJob") \
    .getOrCreate()


def main():
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    snowflake_database = "ABHI_DB"
    snowflake_schema = "PUBLIC"
    source_table_name = "AGENTS"
    snowflake_options = {
    "sfUrl":"xn65019.ap-south-1.aws.snowflakecomputing.com",
    "sfUser": "ABHISHEK",
    "sfPassword": "Abhi@123",
    "sfDatabase": "ABHI_DB",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH"
    }
    df = spark.read\
        .format('parquet').load('s3a://mybucket-untouch//curated//part-00000-f1e03f5e-c014-4853-99ee-f2a1421f85e4-c000.snappy.parquet')
    df1 = df.select("id","clientip","datetime_confirmed","method_GET","status_code","size","user_agent","referer_present")
    df1.write.format("snowflake")\
        .options(**snowflake_options)\
        .option("dbtable", "curated").mode("overwrite")\
        .save()


main()
