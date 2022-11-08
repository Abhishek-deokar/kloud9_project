from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local').appName('curated_layer').getOrCreate()
cleansed_df = spark.read.format('csv').load("s3a://s3-sink-abhi//clensed//part-00000*.csv")
cleansed_df.show()



######### CURATED LAYER **********#@*******************
curated=cleansed_df.drop('referer')
# curated.repartition(1).write.mode('overwrite').save('s3a://mybucket-untouch//curated')


curated = curated.withColumn("day",to_date("datetime_confirmed"))\
    .withColumn("hour",hour("datetime_confirmed"))\
    .withColumn("day_hour",concat(col("day"), lit(" "),col("hour")))
curated.show(truncate=False)
# curated.show(truncate=False)




########## AGG PER AND ACROSS DEVICE #***********#*********
# device_pt = r'(Mozilla|Dalvik|Goog|torob|Bar).\d\S+\s\((\S\w+;?\s\S+(\s\d\.(\d\.)?\d)?)'
device_curated = curated
# device_curated = device_curated.withColumn('device',regexp_extract(col('user_agent'),device_pt,2))
# device_curated.show()

device_agg= device_curated.withColumn("GET",when(col("method_GET")=="GET","GET"))\
                          .withColumn("HEAD",when(col("method_GET")=="HEAD","HEAD"))\
                           .withColumn("POST",when(col("method_GET")=="POST","POST"))\
                            .withColumn('hour', hour(col('datetime_confirmed')))
device_agg.show(50,truncate=False)

per_de=device_agg\
    .groupBy("day_hour","clientip").agg(count('GET').alias("GET"),count('POST').alias("POST")
                                 ,count('HEAD').alias("HEAD"),first("hour").alias('hour')
                                 ,count('clientip').alias('no_of_client'))
per_de.show(400)
print(per_de.rdd.getNumPartitions())
per_de.createOrReplaceTempView("df")
per_de = spark.sql("select row_number() over(order by 'day_hour')as id, * from df")
per_de.show(120,truncate=False)
per_de.repartition(1).write.mode("overwrite").format("csv").option("header","True").save("C://Users//abhishek.dd//Desktop//Anmol//git//per")
# per_de.write.mode("overwrite").format("csv").option("header","True").save("C://Users//abhishek.dd//Desktop//Anmol//git//per")
# per_de.write.mode('overwrite').save('s3a://s3-sink-abhi//per_device')


across_de=device_agg.groupBy("day_hour").agg(count('GET').alias("no_get"),count('POST').alias("no_post")\
                         ,count('HEAD').alias("no_head"),count('clientip').alias("no_of_clients"))
across_de.show()
across_de.createOrReplaceTempView("dff")
across_de = spark.sql("select row_number() over(order by 'day_hour')as id, * from dff")
across_de.show(120,truncate=False)
across_de.repartition(1).write.mode("overwrite").format("csv").option("header","True").save("C://Users//abhishek.dd//Desktop//Anmol//git//across")
# across_de.write.mode('overwrite').save('s3a://s3-sink-abhi//across_device')


### WRITTING THE CURATED DATASET INTO HIVE TABLES **************
per_de.write.mode('overwrite').saveAsTable('log_agg_per_device')
across_de.write.mode('overwrite').saveAsTable('log_agg_across_device')
curated.write.mode("overwrite").saveAsTable("curated")




 ######*********### LOAD TABLE TO SNOWFLAKE *******###############


def write_to_snowflake():
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    snowflake_database = "abhishekdb"
    snowflake_schema = "public"
    target_table_name = "curated_log_details"
    snowflake_options = {
        "sfUrl": "######################################",
        "sfUser": "************",
        "sfPassword": "**********",
        "sfDatabase": snowflake_database,
        "sfSchema": snowflake_schema,
        "sfWarehouse": "curated_snowflake"
    }
    spark = SparkSession.builder \
        .appName("Demo_Project").enableHiveSupport().getOrCreate()
    df_raw = spark.read.format("csv").option("header","True").load(
        "s3://abhi-db//raw.csv")
    raw = df_raw.select("*")
    raw.write.format("snowflake")\
        .options(**snowflake_options) \
        .option("dbtable", "raw_log_details") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()
    df_cleans = spark.read.format("csv").option("header","True").load(
        "s3://abhi-db//cleans.csv")
    cleans = df_cleans.select("*")
    cleans.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "cleans_log_details") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()
    df_curate = spark.read.format("csv").option("header","True").load(
        "s3://abhi-db//curate.csv")
    curate = df_curate.select("*")
    curate.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "curate_log_details") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()
    df_per = spark.read.format("csv").option("header","True").load(
        "s3://abhi-db//per.csv")
    per = df_per.select("*")
    per.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "log_agg_per_details") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()
    df_across = spark.read.format("csv").option("header","True").load(
        "s3://abhi-db//across.csv")
    across = df_across.select("*")
    across.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "log_agg_across_details") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()

write_to_snowflake()
