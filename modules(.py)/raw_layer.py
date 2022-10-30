from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local').appName('raw_layer').getOrCreate()
data = ("C:\\Users\\abhishek.dd\\Desktop\\Anmol\\Anmol_arrived\\100.txt")
df = spark.read.format('text').load(data)
# df = spark.read.format("text").load("s3://sinking-data/sinking-data/msk-topics-latest/0/299999.text")
df.show(truncate=False)

### CREATING THE REGULAR EXP PATTERNS FOR EXTRACTING THE REQUIRE DATA *********************
host_p = r'([\S+\.]+)'
time_pattern = r'(\d+/\w+/\d+[:\d]+)'
GET = r'GET|POST|HEAD'
request_p = r'\s\S+\sHTTP/1.1"'
status_p = r'\s\d{3}\s'
size_p = r'\s(\d+)\s"'
ree = r'("https(\S+)")'
usery = r'"(Mozilla|Dalvik|Goog|torob|Bar).\d\S+\s\((\S\w+;?\s\S+(\s\d\.(\d\.)?\d)?)'
spe_char = r'[%,|"-&.=?-]'

### ARRANGING THE EXTRACTING DATA INTO DATAFRAME ***************************
raw_df = df.withColumn("id", monotonically_increasing_id()) \
    .select('id', regexp_extract('value', host_p, 1).alias('clientip')
            , regexp_extract('value', time_pattern, 1).alias('datetime_confirmed')
            , regexp_extract('value', GET, 0).alias("method_GET")
            , regexp_extract('value', request_p, 0).alias('request')
            , regexp_extract('value', status_p, 0).alias('status_code')
            , regexp_extract('value', size_p, 1).alias('size')
            , regexp_extract('value', ree, 1).alias('referer')
            , regexp_extract('value', usery, 0).alias('user_agent'))
raw_df.show(truncate=False)
# raw_df.count()
# raw_df.write.format('csv').mode('overwrite').save('s3a://s3-sink-abhi//raw_log_details.csv')
# raw_df.write.mode("overwrite").saveAsTable("raw_df")
