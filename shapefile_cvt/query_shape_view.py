from awsglue import DynamicFrame
from sedona.spark import SedonaContext

def query_shape_view(self, query,view):
    spark=self.glue_ctx.sparkSession
    sedona = SedonaContext.create(spark)
    df=sedona.sql(query)
    df.createOrReplaceTempView(view)
    #you can return a DyF, but that doesn't implement most methods and will fail. So essentially, don't use, just return 
    return DynamicFrame.fromDF(df, self.glue_ctx, "query_shape_view_txnc")   
DynamicFrame.query_shape_view = query_shape_view

'''
def is_s3_folder_path(path: str) -> bool:
    return bool(path) and path.startswith("s3://") and path.endswith("/")

'''
