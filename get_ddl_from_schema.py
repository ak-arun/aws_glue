import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import *
from delta.tables import *
import boto3
args = getResolvedOptions(sys.argv, ['JOB_NAME','INPUT_S3_LOCATION','BASE_TBL_S3_LOCATION','DB_NAME','BASE_TBL_NAME','ATHENA_RESULT_LOCATION'])

output_location = args['BASE_TBL_S3_LOCATION']
database_name=args['DB_NAME']
table_name=args['BASE_TBL_NAME']
athena_query_location=args['ATHENA_RESULT_LOCATION']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
df=spark.read.format("csv").option("header", True).load(args['INPUT_S3_LOCATION'])
df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(output_location)
deltaTable = DeltaTable.forPath(spark,output_location)
deltaTable.generate("symlink_format_manifest")

athena = boto3.client('athena')
athena.start_query_execution(QueryString="create database if not exists "+database_name,ResultConfiguration={'OutputLocation': athena_query_location})
ddl = spark.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromJson(df.schema.json()).toDDL()
ddl_stmt_string = "CREATE EXTERNAL TABLE IF NOT EXISTS "+database_name+"."+table_name+"("+str(ddl)+") ROW FORMAT SERDE \"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe\" STORED AS INPUTFORMAT \"org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat\" OUTPUTFORMAT \"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\" LOCATION \""+output_location+"_symlink_format_manifest/\""
print(ddl_stmt_string)
athena.start_query_execution(QueryString=ddl_stmt_string,ResultConfiguration={'OutputLocation': athena_query_location})
job.commit()
