import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, Row
import datetime
from awsglue import DynamicFrame
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql import Row
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *

def get_previous_key(date_in,id):
    import datetime
    dt5before=date_in-datetime.timedelta(seconds=5)
    dt5after=date_in+datetime.timedelta(seconds=5)
    return str(dt5before.strftime("%Y-%m-%dT%H:%M:%S"))+" "+str(dt5after.strftime("%Y-%m-%dT%H:%M:%S")) + "_"+str(id)

def get_current_key(date_start,date_end,id):
    return str(date_start.strftime("%Y-%m-%dT%H:%M:%S"))+" "+str(date_end.strftime("%Y-%m-%dT%H:%M:%S")) + "_"+str(id)
    


get_prev_lookup_key = udf(get_previous_key, StringType())
generate_current_key = udf(get_current_key, StringType())

pythonSchema = StructType().add("dt",TimestampType()).add("id",StringType()).add("c1", LongType()).add("c2", LongType())


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
kinesisDF=spark.readStream.format("kinesis").option("streamName","STREAM_NAME").option("endpointUrl", "https://kinesis.REGION.amazonaws.com").option("startingPosition", "TRIM_HORIZON").load()

dataDevicesDF=kinesisDF.selectExpr('CAST(data AS STRING)').select(from_json('data', pythonSchema).alias('data')).select('data.dt','data.id','data.c1','data.c2')
queryResults=dataDevicesDF.withWatermark("dt", "5 seconds").groupBy(col("id"),window(col("dt"),"10 seconds","5 seconds")).sum("c1","c2")
renamedResults = queryResults.withColumn("start_ts", to_timestamp(col("window.start"))).withColumn("end_ts", to_timestamp(col("window.end"))).withColumn("sum_c1", col("`sum(c1)`")).withColumn("sum_c2", col("`sum(c2)`"))

renamedResultsWithKey=renamedResults.withColumn("prev_lookup_key",get_prev_lookup_key(col("start_ts"),col("id"))).withColumn("curr_lookup_key",generate_current_key(col("start_ts"),col("end_ts"),col("id")))

write = renamedResultsWithKey.writeStream.format("json").option("checkpointLocation", "s3a://BUCKET/checkpoint/cpds93/").start("s3a://BUCKET/ds93/")
write.awaitTermination() 
