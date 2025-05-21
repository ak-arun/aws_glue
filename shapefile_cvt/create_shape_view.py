from awsglue import DynamicFrame
from sedona.spark import SedonaContext
def create_shape_view(self, shape_file_path,shape_view_name):
    spark=self.glue_ctx.sparkSession
    sedona = SedonaContext.create(spark)
    df = sedona.read.format("shapefile").load(shape_file_path)
    df.createOrReplaceTempView(shape_view_name)
    #you can return a DyF, but that doesn't implement most methods and will fail. So essentially, don't use, just return 
    return DynamicFrame.fromDF(df, self.glue_ctx, "create_shape_view_txnc")   
DynamicFrame.create_shape_view = create_shape_view
