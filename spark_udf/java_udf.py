import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

data = [
    ("Alice", 28, "New York"),
    ("Bob", 35, "San Francisco"),
    ("Charlie", 42, "Chicago"),
    ("Diana", 31, "Houston"),
    ("Ethan", 39, "Seattle")
]

df = spark.createDataFrame(data, schema)

df.createOrReplaceTempView("data")

spark.sql("CREATE TEMPORARY FUNCTION custom_rev AS 'com.aws.glue.udf.CustomRevFunction'")
spark.sql("SELECT *,custom_rev(city) as city_reverse FROM data").show(10,False)

job.commit()
