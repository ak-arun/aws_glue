from pyspark.sql.functions import input_file_name
import boto3
import ezdxf
import json
from awsglue.dynamicframe import DynamicFrame
import uuid

def get_dxfdf(spark,s3_path):
    no_prefix = s3_path.replace("s3://", "", 1)
    bucket, key = no_prefix.split("/", 1)
    file_name=str(uuid.uuid4())
    s3 = boto3.resource('s3')
    s3.Bucket(bucket).download_file(key, f"/tmp/{file_name}.dxf")
    doc = ezdxf.readfile(f"/tmp/{file_name}.dxf")
    msp = doc.modelspace()
    
    entities = []
    
    for entity in msp:
        if entity.dxftype() == 'LINE':
            entities.append({
                "type": "LINE",
                "start": [entity.dxf.start.x, entity.dxf.start.y],
                "end": [entity.dxf.end.x, entity.dxf.end.y],
                "layer": entity.dxf.layer
            })
        elif entity.dxftype() == 'CIRCLE':
            entities.append({
                "type": "CIRCLE",
                "center": [entity.dxf.center.x, entity.dxf.center.y],
                "radius": entity.dxf.radius,
                "layer": entity.dxf.layer
            })
        elif entity.dxftype() == 'TEXT':
            entities.append({
                "type": "TEXT",
                "text": entity.dxf.text,
                "position": [entity.dxf.insert.x, entity.dxf.insert.y],
                "height": entity.dxf.height,
                "layer": entity.dxf.layer
            })
    
    df = spark.createDataFrame(entities)
    return df
    
def read_dxf(self, s3_path):
    spark=self.glue_ctx.sparkSession
    df=get_dxfdf(spark,s3_path)
    return DynamicFrame.fromDF(df, self.glue_ctx, "read_dxf_txnc")  

DynamicFrame.read_dxf = read_dxf

   
    

