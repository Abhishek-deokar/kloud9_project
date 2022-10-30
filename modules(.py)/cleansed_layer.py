from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local').appName('clans_layer').getOrCreate()
raw_df = spark.read.format('csv').load("s3a://s3-sink-abhi//raw_df//part-00000*.csv")
raw_df.show()

spe_char = r'[%,|"-&.=?-]'

######## CLEANING THE DATA WITH DATATYPE AND ADDING NEW COLUMN ***********************
cleansed = raw_df.withColumn("id", col('id').cast('int')) \
    .withColumn("datetime_confirmed", to_timestamp("datetime_confirmed", 'dd/MMM/yyyy:HH:mm:ss')) \
    .withColumn('status_code', col('status_code').cast('int')) \
    .withColumn('size', col('size').cast('int')) \
    .withColumn("request", regexp_replace("request", spe_char, "")) \
    .withColumn("size", round(col("size") / 1024, 2)) \
    .withColumn('referer_present', when(col('referer') == '', 'N') \
                .otherwise('Y'))
cleansed = cleansed.withColumn("datetime_confirmed", split(cleansed["datetime_confirmed"], ' ')\
                    .getItem(0)).withColumn("datetime_confirmed",to_timestamp("datetime_confirmed",'dd/MMM/yyyy:hh:mm:ss'))\
                                .withColumn("datetime_confirmed",to_timestamp("datetime_confirmed",'MMM/dd/yyyy:hh:mm:ss'))
cleansed.printSchema()
cleansed.na.fill("NA").show()
#
# cleansed.write.format("csv").mode('overwrite').save('s3a://s3-sink-abhi//cleansed')
# cleansed.write.mode('overwrite').saveAsTable("cleansed")
