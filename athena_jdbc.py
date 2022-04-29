import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
x=spark.read.format("jdbc").option("url", "jdbc:awsathena://AwsRegion=my-region-name").option("AwsCredentialsProviderClass","com.simba.athena.amazonaws.auth.InstanceProfileCredentialsProvider").option("S3OutputLocation","s3://BLAH/athena/query-location-BLAH/").option("dbtable", "db1.tbl1").load()
x.show()
job.commit()
