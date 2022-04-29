'''
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
from pyspark.sql.types import LongType
from pyspark.sql.functions import udf
from pyspark.sql import Row
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *

from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.functions import pandas_udf, PandasUDFType

import boto3
import uuid

import random

import json
import pandas as pd

def setBroadcast(sparkContext,x):
    if ('broadcast_map' not in globals()):
        globals()['broadcast_map'] = sparkContext.broadcast(x)
    return globals()['broadcast_map']

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
bc = setBroadcast(spark.sparkContext,{"hello":"world"})
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

pythonSchema = StructType().add("dt",TimestampType()).add("id",StringType()).add("c1", LongType()).add("c2", LongType())
kinesis_data=spark.readStream.format("kinesis").option("streamName","MY-STREAM-NAME9").option("endpointUrl", "https://kinesis.MY-REGION-NAME.amazonaws.com").option("startingPosition", "TRIM_HORIZON").load().selectExpr('CAST(data AS STRING)').select(from_json('data', pythonSchema).alias('data')).select('data.dt','data.id','data.c1','data.c2')


def to_single_map(list_of_maps):
    single_map={}
    for e in list_of_maps:
        single_map.update(json.loads(e[0]))
    return single_map
    
@pandas_udf("string")
def to_map_string(id_series: pd.Series,start_ts: pd.Series,sum_c1_series: pd.Series,sum_c2_series: pd.Series) -> pd.Series:
    vals=zip(id_series,start_ts,sum_c1_series,sum_c2_series)
    return_list=[]
    dict_new={}
    for id,sts,c1,c2 in vals:
        dict_new[str(id)+"_"+str(sts)+".0"]=str(c1)+"_"+str(c2)
        return_list.append(json.dumps(dict_new))
    return pd.Series(return_list)
        
        
    
@pandas_udf("string")
def look_up_pandas(id_series: pd.Series,start_ts_series: pd.Series, cached_map_series: pd.Series) -> pd.Series:
    cached_map=json.loads(str(cached_map_series.iloc[0]))
    idts = zip(id_series,start_ts_series)
    looked_up_vals = []
    for id, start_ts in idts:
        previous_window_start=start_ts-datetime.timedelta(seconds=10)
        lookup_key = id+"_"+str(previous_window_start)+".0"
        cached_value = cached_map.pop(lookup_key, "NA")
        looked_up_vals.append(cached_value)
    return pd.Series(looked_up_vals) 
    

def processBatch(data_frame, batchId):
    if data_frame.count() > 0:
        bc = globals()['broadcast_map']
        cached_map_string = json.dumps(bc.value)
        aggregated_frame=data_frame.withWatermark("dt", "20 seconds").groupBy(col("id"),window(col("dt"),"20 seconds","10 seconds")).sum("c1","c2")
        renamed_frame = aggregated_frame.withColumn("start_ts", to_timestamp(col("window.start"))).withColumn("end_ts", to_timestamp(col("window.end"))).withColumn("sum_c1", col("`sum(c1)`")).withColumn("sum_c2", col("`sum(c2)`"))
        drop_columns=['window','sum(c1)','sum(c2)']
        final_frame = renamed_frame.drop(*drop_columns)
        lookedup_frame = final_frame.withColumn("cached_value",look_up_pandas(col("id"),col("start_ts"),lit(cached_map_string)))
        lookedup_frame.cache()
        lookup_map_cols=lookedup_frame.select(to_map_string(col("id"),col("start_ts"),col("sum_c1"),col("sum_c2")).alias("map_string")).collect()
        globals()['broadcast_map'] = spark.sparkContext.broadcast(to_single_map(lookup_map_cols))
        frame_to_write = DynamicFrame.fromDF(lookedup_frame, glueContext, "frame_to_write")
        s3sink = glueContext.write_dynamic_frame.from_options(frame = frame_to_write, connection_type = "s3", connection_options = {"path": "s3a://MY-BUCKET-NAME/glue/structured_streaming/spark_ss_tests0221_23/"}, format = "json", transformation_ctx = "s3sink")

glueContext.forEachBatch(
    frame=kinesis_data,
    batch_function=processBatch,
    options={
        "windowSize": "10 seconds",
        "checkpointLocation": "s3a://MY-BUCKET-NAME/glue/structured_streaming/checkpoint/spark_ss_tests0221_23/",
    },
)
job.commit()
'''

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
from pyspark.sql.types import LongType
from pyspark.sql.functions import udf
from pyspark.sql import Row
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *

from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.functions import pandas_udf, PandasUDFType

import boto3
import uuid

import random

import json
import pandas as pd

def setBroadcast(sparkContext,x):
    if ('broadcast_map' not in globals()):
        globals()['broadcast_map'] = sparkContext.broadcast(x)
    return globals()['broadcast_map']

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
bc = setBroadcast(spark.sparkContext,{"hello":"world"})
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

pythonSchema = StructType().add("dt",TimestampType()).add("id",StringType()).add("c1", LongType()).add("c2", LongType())
kinesis_data=spark.readStream.format("kinesis").option("streamName","MY-STREAM-NAME9").option("endpointUrl", "https://kinesis.MY-REGION-NAME.amazonaws.com").option("startingPosition", "TRIM_HORIZON").load().selectExpr('CAST(data AS STRING)').select(from_json('data', pythonSchema).alias('data')).select('data.dt','data.id','data.c1','data.c2')


def to_single_map(list_of_maps):
    single_map={}
    for e in list_of_maps:
        #single_map.update(json.loads(e[0]))
        single_map.update(e[0])
    return single_map
    
@pandas_udf(ArrayType(StringType()))
def look_up_pandas_df(id_series: pd.Series,start_ts_series: pd.Series, end_ts_series: pd.Series, sum_c1_series: pd.Series, sum_c2_series: pd.Series, cached_map_series: pd.Series) -> pd.Series:
    cached_map=json.loads(str(cached_map_series.iloc[0]))
    idts = zip(id_series,start_ts_series,end_ts_series,sum_c1_series,sum_c2_series)
    outerArray = []
    for id, start_ts, end_ts, sum_c1, sum_c2 in idts:
        innerArray=[]
        previous_window_start=start_ts-datetime.timedelta(seconds=10)
        lookup_key = id+"_"+str(previous_window_start)+".0"
        cached_value = cached_map.pop(lookup_key, "NA")
        innerArray.append(id)
        innerArray.append(str(start_ts))
        innerArray.append(str(end_ts))
        innerArray.append(str(sum_c1))
        innerArray.append(str(sum_c2))
        innerArray.append(cached_value)
        innerArray.append(str(id)+"_"+str(start_ts)+".0")
        innerArray.append(str(sum_c1)+"_"+str(sum_c2))
        outerArray.append(innerArray)
    return pd.Series(outerArray)
    

def processBatch(data_frame, batchId):
    if data_frame.count() > 0:
        bc = globals()['broadcast_map']
        cached_map_string = json.dumps(bc.value)
        aggregated_frame=data_frame.withWatermark("dt", "20 seconds").groupBy(col("id"),window(col("dt"),"20 seconds","10 seconds")).sum("c1","c2")
        renamed_frame = aggregated_frame.withColumn("start_ts", to_timestamp(col("window.start"))).withColumn("end_ts", to_timestamp(col("window.end"))).withColumn("sum_c1", col("`sum(c1)`")).withColumn("sum_c2", col("`sum(c2)`"))
        lookedup_frame_array=renamed_frame.select(look_up_pandas_df(col("id"),col("start_ts"),col("end_ts"),col("sum_c1"),col("sum_c2"),lit(cached_map_string)).alias("looked_up_array"))
        lookedup_frame=lookedup_frame_array.select(col("looked_up_array").getItem(0).alias("id"),col("looked_up_array").getItem(1).alias("start_ts"),col("looked_up_array").getItem(2).alias("end_ts"),col("looked_up_array").getItem(3).alias("sum_c1"),col("looked_up_array").getItem(4).alias("sum_c2"),col("looked_up_array").getItem(5).alias("cached_value"),col("looked_up_array").getItem(6).alias("lookup_key"),col("looked_up_array").getItem(7).alias("lookup_value"))
        lookedup_frame.cache()
        lookup_map_to_cache = to_single_map(lookedup_frame.select(create_map(col("lookup_key"),col("lookup_value")).alias("lookup_map")).collect())
        globals()['broadcast_map'] = spark.sparkContext.broadcast(lookup_map_to_cache)
        frame_to_write = DynamicFrame.fromDF(lookedup_frame, glueContext, "frame_to_write")
        s3sink = glueContext.write_dynamic_frame.from_options(frame = frame_to_write, connection_type = "s3", connection_options = {"path": "s3a://MY-BUCKET-NAME/glue/structured_streaming/spark_ss_tests0221_25/"}, format = "json", transformation_ctx = "s3sink")

glueContext.forEachBatch(
    frame=kinesis_data,
    batch_function=processBatch,
    options={
        "windowSize": "10 seconds",
        "checkpointLocation": "s3a://MY-BUCKET-NAME/glue/structured_streaming/checkpoint/spark_ss_tests0221_25/",
    },
)
job.commit()

