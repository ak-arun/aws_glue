import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','RAW_DATA_S3_LOCATION'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


hudiOptions = {
'hoodie.table.name': 'base_table_test2',
'hoodie.datasource.write.recordkey.field': 'txn_id',
'hoodie.datasource.write.precombine.field': 'total_sales',
'hoodie.datasource.hive_sync.partition_extractor_class':'org.apache.hudi.hive.NonPartitionedExtractor',
'hoodie.datasource.write.keygenerator.class':'org.apache.hudi.keygen.NonpartitionedKeyGenerator',
'hoodie.datasource.hive_sync.enable':'true',
'hoodie.datasource.hive_sync.use_jdbc':'false',
'hoodie.datasource.hive_sync.database':'default',
'hoodie.datasource.hive_sync.table':'base_table_test2'
}

raw_location = args['RAW_DATA_S3_LOCATION']
base_tbl_location = 's3://BUCKET-NAME/hudi/base_table_test2/'

df=spark.read.format("csv").option("header", True).load(raw_location)

df.write \
.format('org.apache.hudi') \
.option('hoodie.datasource.write.operation', 'insert') \
.options(**hudiOptions) \
.mode('overwrite') \
.save(base_tbl_location)
job.commit()

'''
Dependent JARs path
s3://MY-BUCKET-NAME/hudi/calcite-core-1.16.0.jar,
s3://MY-BUCKET-NAME/hudi/libfb303-0.9.3.jar,
s3://MY-BUCKET-NAME/hudi/hudi-spark3-bundle_2.12-0.9.0.jar,
s3://MY-BUCKET-NAME/hudi/spark-avro_2.12-3.0.1.jar
'''

'''
--conf
spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.jars=s3://MY-BUCKET-NAME/hudi/calcite-core-1.16.0.jar,s3://MY-BUCKET-NAME/hudi/libfb303-0.9.3.jar,s3://MY-BUCKET-NAME/hudi/hudi-spark3-bundle_2.12-0.9.0.jar,s3://MY-BUCKET-NAME/hudi/spark-avro_2.12-3.0.1.jar --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory
'''
