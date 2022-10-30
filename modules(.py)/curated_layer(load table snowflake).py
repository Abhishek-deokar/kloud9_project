from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local').appName('clans_layer').getOrCreate()
cleansed = spark.read.format('csv').load("s3a://s3-sink-abhi//cleans_df//part-00000*.csv")
cleansed.show()

curated=cleansed.drop('referer')
curated.show(truncate=False)
# curated.write.mode('overwrite').save('s3a://s3-sink-abhi//curated')


######*********### LOAD TABLE TO SNOWFLAKE *******###############

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
        .format('csv').load('s3a://s3-sink-abhi//curated//part-00000*.csv')
    df1 = df.select("id","clientip","datetime_confirmed","method_GET","status_code","size","user_agent","referer_present")
    df1.write.format("snowflake")\
        .options(**snowflake_options)\
        .option("dbtable", "curated").mode("overwrite")\
        .save()


main()



