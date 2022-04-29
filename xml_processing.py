import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
import xml.etree.ElementTree as ET



def process_xml(xml_in):
root = ET.fromstring(str(xml_in))
visitor={}
visitors=[]
for child in root:
v=visitor[child.tag]=child.attrib
visitors.append(v)
returnArray=[]
for v in visitors:
returnArray.append(v.pop("id","NA")+"~"+v.pop("age","NA")+"~"+v.pop("sex","NA"))
return returnArray

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

get_xml_strings = udf(process_xml, ArrayType(StringType()))

df = spark.read.json("s3://BUCKET/glue/tests/xml_in/")
#df.printSchema()
#df.show(10,False)
df2 = df.withColumn("visitor",explode(get_xml_strings(col("xml_data"))))
df3 = df2.withColumn("id",split(col("visitor"), '~').getItem(0)).withColumn("age",split(col("visitor"), '~').getItem(1)).withColumn("gender",split(col("visitor"), '~').getItem(2))
df4 = df3.drop(col("xml_data")).drop(col("visitor"))
df4.show(100,False)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

'''
{"name": "babu6","location": "us6","xml_data": "<?xml version=\"1.0\" encoding=\"utf-8\"?> <visitors> <visitor id=\"9620\" age=\"73\" sex=\"F\" /> <visitor id=\"1887\" age=\"39\" sex=\"M\" /> <visitor id=\"5993\" age=\"28\" sex=\"M\" /> </visitors>"}
'''
