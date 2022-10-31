from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local').appName('curated_layer').getOrCreate()
cleansed_df = spark.read.format('csv').load("s3a://s3-sink-abhi//clensed//part-00000*.csv")
cleansed_df.show()


######### CURATED LAYER **********#@*******************
curated=cleansed_df.drop('referer')
curated.show(truncate=False)
# curated.write.mode('overwrite').save('s3a://mybucket-untouch//curated')



########## AGG PER AND ACROSS DEVICE #***********#*********
# device_pt = r'(Mozilla|Dalvik|Goog|torob|Bar).\d\S+\s\((\S\w+;?\s\S+(\s\d\.(\d\.)?\d)?)'
device_curated = curated
# device_curated = device_curated.withColumn('device',regexp_extract(col('user_agent'),device_pt,2))
device_curated.show()

device_agg= device_curated.withColumn("GET",when(col("method_GET")=="GET","GET"))\
                          .withColumn("HEAD",when(col("method_GET")=="HEAD","HEAD"))\
                           .withColumn("POST",when(col("method_GET")=="POST","POST"))\
                            .withColumn('hour', hour(col('datetime_confirmed')))
device_agg.show(50,truncate=False)

per_de=device_agg.groupBy("clientip").agg(count('GET').alias("GET"),count('POST').alias("POST")
                                 ,count('HEAD').alias("HEAD"),first("hour").alias('hour')
                                 ,count('clientip').alias('no_of_client'))
per_de.show(400)
print(per_de.rdd.getNumPartitions())
per_de.repartition(1).write.mode("overwrite").format("csv").option("header","True").save("C://Users//abhishek.dd//Desktop//Anmol//git//per")
# per_de.write.mode("overwrite").format("csv").option("header","True").save("C://Users//abhishek.dd//Desktop//Anmol//git//per")
# per_de.write.mode('overwrite').save('s3a://s3-sink-abhi//per_device')

across_de=device_agg.agg(count('GET').alias("no_get"),count('POST').alias("no_post")\
                         ,count('HEAD').alias("no_head"),first("hour").alias('day_hour'),count('clientip').alias("no_of_clinets"))\
                        .withColumn('row_id',monotonically_increasing_id())
across_de.show()
across_de.repartition(1)
across_de.write.mode("overwrite").format("csv").option("header","True").save("C://Users//abhishek.dd//Desktop//Anmol//git//across")
# across_de.write.mode('overwrite').save('s3a://s3-sink-abhi//across_device')


### WRITTING THE CURATED DATASET INTO HIVE TABLES **************
per_de.write.mode('overwrite').saveAsTable('log_agg_per_device')
across_de.write.mode('overwrite').saveAsTable('log_agg_across_device')
curated.write.mode("overwrite").saveAsTable("curated")




 ######*********### LOAD TABLE TO SNOWFLAKE *******###############


def main():
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    snowflake_database = "ABHI_DB"
    snowflake_schema = "PUBLIC"
    source_table_name = "AGENTS"
    snowflake_options = {
    "sfUrl":"******.snowflakecomputing.com",
    "sfUser": "********",
    "sfPassword": "********",
    "sfDatabase": "ABHI_DB",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH"
    }
    df = spark.read\
        .format('parquet').load('s3a://s3-sink-abhi//curated//part-00000*.csv')
    df1 = df.select("id","clientip","datetime_confirmed","method_GET","status_code","size","user_agent","referer_present")
    df1.write.format("snowflake")\
        .options(**snowflake_options)\
        .option("dbtable", "curated").mode("overwrite")\
        .save()


main()
