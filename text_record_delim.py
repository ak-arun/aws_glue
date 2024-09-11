import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
read_conf = {}
read_conf["textinputformat.record.delimiter"]="009 00000"
rdd = sc.newAPIHadoopFile("s3://BUCKET/multiline/", "org.apache.hadoop.mapreduce.lib.input.TextInputFormat","org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text",conf=read_conf).map(lambda x: x[1]+"009     00000")

for x in rdd.collect():
    print(x)
    print("-------")        

job.commit()
