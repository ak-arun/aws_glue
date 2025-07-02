import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import StringType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

data = [
    ("John", "Doe", "Software Engineer"),
    ("Jane", "Smith", "Data Scientist"),
    ("Bob", "Johnson", "Product Manager"),
    ("Alice", "Williams", "DevOps Engineer"),
    ("Charlie", "Brown", "Business Analyst")
]

columns = ["first_name", "last_name", "job_title"]
df = spark.createDataFrame(data, columns)

df.show()

@pandas_udf(returnType=StringType())
def reverse_string_udf(series: pd.Series) -> pd.Series:
    return series.str[::-1]

df_with_reversed = df.withColumn("reversed_job_title", reverse_string_udf(col("job_title")))

print("\nDataFrame with reversed job titles:")
df_with_reversed.show(truncate=False)

@pandas_udf(returnType=StringType())
def reverse_full_name_udf(first_name_series: pd.Series, last_name_series: pd.Series) -> pd.Series:
    full_names = first_name_series + " " + last_name_series
    return full_names.str[::-1]

df_final = df_with_reversed.withColumn(
    "reversed_full_name", 
    reverse_full_name_udf(col("first_name"), col("last_name"))
)

print("\nFinal DataFrame with all transformations:")
df_final.show(truncate=False)

print(f"\nTotal records processed: {df_final.count()}")
print("Columns in final DataFrame:", df_final.columns)



spark.udf.register("reverse_string", reverse_string_udf)
df.createOrReplaceTempView("data")
result = spark.sql("SELECT *, reverse_string(job_title) as reversed FROM data")
result.show(truncate=False)

print("Script completed successfully!")


job.commit()
