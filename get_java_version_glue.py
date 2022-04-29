import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import jaydebeapi
import subprocess
import jpype
import os

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
print("hello")

java = subprocess.check_output(['which', 'java'], stderr=subprocess.STDOUT)
print(java)
print("##########1###########")
print(os.listdir("/usr/lib/jvm/"))
print("##########2###########")
print(os.listdir("/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.282.b08-1.amzn2.0.1.x86_64/jre/bin/"))
print("##########3###########")
which_java = subprocess.check_output(['readlink', '-f','/usr/bin/java'], stderr=subprocess.STDOUT)
print(which_java)
job.commit()
