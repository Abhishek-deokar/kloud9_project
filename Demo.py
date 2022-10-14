from pyspark.sql import *
from pyspark.sql.functions import *

### BUILDING THE SPARK SESSION *************************
spark = SparkSession.builder.master('local').appName('unstructured').enableHiveSupport().getOrCreate()
data = 'D:\\bigdata\\log_data_ip_request.txt'
df = spark.read.format('text').load('s3a://mybucket-untouch//log_data_ip_request.txt')

### CREATING THE REGULAR EXP PATTERNS FOR EXTRACTING THE REQUIRE DATA *********************
host_p = r'([\S+\.]+)'
time_pattern = r'(\d+/\w+/\d+[:\d]+)'
GET=r'GET|POST|HEAD'
request_p=r'\s\S+\sHTTP/1.1"'
status_p=r'\s\d{3}\s'
size_p=r'\s(\d+)\s"'
ree=r'("https(\S+)")'
usery=r'"(Mozilla|Dalvik|Goog|torob|Bar).\d\S+\s\((\S\w+;?\s\S+(\s\d\.(\d\.)?\d)?)'

### ARRANGING THE EXTRACTING DATA INTO DATAFRAME ***************************
raw_df=df.withColumn("id",monotonically_increasing_id())\
      .select('id',regexp_extract('value',host_p,1).alias('clientip')
      ,regexp_extract('value', time_pattern,1).alias('datetime_confirmed')
      ,regexp_extract('value',GET,0).alias("method_GET")
      ,regexp_extract('value',request_p,0).alias('request')
      ,regexp_extract('value',status_p,0).alias('status_code')
      ,regexp_extract('value',size_p,1).alias('size')
      ,regexp_extract('value',ree,1).alias('referer')
      ,regexp_extract('value',usery,0).alias('user_agent'))
raw_df.show(truncate=False)
raw_df.write.mode('overwrite').save('s3a://mybucket-untouch//raw_df')

### CLEANING THE DATA WITH DATATYPE AND ADDING NEW COLUMN ***********************
cleansed = raw_df.withColumn("id", col('id').cast('int'))\
    .withColumn("datetime_confirmed",to_timestamp("datetime_confirmed",'dd/MMM/yyyy:HH:mm:ss'))\
    .withColumn('status_code',col('status_code').cast('int'))\
    .withColumn('size', col('size').cast('int'))\
    .withColumn('referer_present',when(col('referer') == '','N')\
             .otherwise('Y'))
cleansed.printSchema()
cleansed.show()
cleansed.write.mode('overwrite').save('s3a://mybucket-untouch//cleansed')

device_pt = r'(Mozilla|Dalvik|Goog|torob|Bar).\d\S+\s\((\S\w+;?\s\S+(\s\d\.(\d\.)?\d)?)'
curated=cleansed.drop('referer')
curated.show()
curated.write.mode('overwrite').save('s3a://mybucket-untouch//curated')

device_curated = curated
device_curated = device_curated.withColumn('device',regexp_extract(col('user_agent'),device_pt,2))
device_curated.show()

### ARRANGING THE DATA FOR AGG PER DEVICE AND ACROSS DEVICE *************
device_agg= device_curated.withColumn("GET",when(col("method_GET")=="GET","GET"))\
                          .withColumn("HEAD",when(col("method_GET")=="HEAD","HEAD"))\
                           .withColumn("POST",when(col("method_GET")=="POST","POST"))\
                            .withColumn('hour', hour(col('datetime_confirmed')))
per_de=device_agg.groupBy("device").agg(count('GET').alias("GET"),count('POST').alias("POST")
                                 ,count('HEAD').alias("HEAD"),first("hour").alias('hour')
                                 ,count('clientip').alias('clientip'))
per_de.show()
per_de.write.mode('overwrite').save('s3a://mybucket-untouch//per_device')

across_de=device_agg.agg(count('GET').alias("no_get"),count('POST').alias("no_post")\
                         ,count('HEAD').alias("no_head"),first("hour").alias('day_hour'),count('clientip').alias("no_of_clinets"))\
                        .withColumn('row_id',monotonically_increasing_id())
across_de.show()
across_de.write.mode('overwrite').save('s3a://mybucket-untouch//across_device')


### WRITTING THE CURATED DATASET INTO HIVE TABLES **************
per_de.write.mode('overwrite').saveAsTable('log_agg_per_device')
across_de.write.mode('overwrite').saveAsTable('log_agg_across_device')


### WRITING THE DATA INTO SNOWFLAKE AND READING S3 BUCKET

