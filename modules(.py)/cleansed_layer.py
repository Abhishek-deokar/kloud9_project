from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local').appName('clans_layer').getOrCreate()
# raw_df = spark.read.format('csv').load("s3a://s3-sink-abhi//raw_df//part-00000*.csv")
raw_df = spark.read.format("csv").option("header","True").load("C:\\Users\\abhishek.dd\\Desktop\\Anmol\\git\\raw")
raw_df.show()

spe_char = r'[%,|"-&.=?-]'
raw_df = raw_df.na.fill("NA")
######## CLEANING THE DATA WITH DATATYPE AND ADDING NEW COLUMN ***********************
cleansed = raw_df.withColumn("id", col('id').cast('int')) \
    .withColumn("datetime_confirmed", to_timestamp("datetime_confirmed", 'dd/MMM/yyyy:HH:mm:ss')) \
    .withColumn('status_code', col('status_code').cast('int')) \
    .withColumn('size', col('size').cast('int')) \
    .withColumn("request", regexp_replace("request", spe_char, "")) \
    .withColumn("size", round(col("size") / 1024, 2)) \
    .withColumn('referer_present', when(col('referer') == "NA", 'N') \
                .otherwise('Y'))


cleansed.printSchema()
cleansed.show(truncate=False)
print(cleansed.count())

# cleansed.write.format("csv").mode('overwrite').save('s3a://s3-sink-abhi//cleansed')
cleansed.write.mode('overwrite').saveAsTable("cleansed")
spark.sql("show tables;").show()
spark.sql("select count(*) from cleansed;").show()
