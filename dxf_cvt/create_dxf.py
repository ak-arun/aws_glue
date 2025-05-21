from awsglue import DynamicFrame
import pandas as pd
import ezdxf
import boto3



def split_s3_path(s3_uri: str):
    if not s3_uri or not s3_uri.startswith("s3://"):
        raise ValueError("Invalid S3 URI")

    no_scheme = s3_uri[5:]  # remove 's3://'
    parts = no_scheme.split("/", 1)

    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""

    return bucket, prefix


def store (df,s3_path,s3_filename):
    doc = ezdxf.new(dxfversion='R2010')
    msp = doc.modelspace()
    for _, row in df.iterrows():
        entity_type = row['entity_type']
        layer = row['layer']
    
        if entity_type == "LINE":
            msp.add_line(
                (row['start_x'], row['start_y']),
                (row['end_x'], row['end_y']),
                dxfattribs={"layer": layer}
            )
        elif entity_type == "CIRCLE":
            msp.add_circle(
                center=(row['center_x'], row['center_y']),
                radius=row['radius'],
                dxfattribs={"layer": layer}
            )
        elif entity_type == "TEXT":
            text_entity = msp.add_text(
                              row['text'],
                              dxfattribs={
                                "height": row['text_height'],
                                "layer": layer
                              })
            text_entity.dxf.insert = (row['text_position_x'], row['text_position_y'])
            
            
    bucket, prefix=split_s3_path(s3_path)
    doc.saveas(f"/tmp/{s3_filename}.dxf")
    s3 = boto3.client('s3')
    s3_key = f"{prefix}/{s3_filename}.dxf"
    with open(f"/tmp/{s3_filename}.dxf", 'rb') as f:
        s3.upload_fileobj(f, bucket, s3_key)
    
    
    
def create_dxf(self, s3_path,s3_filename):
    df = self.toDF().toPandas()
    store (df,s3_path,s3_filename)
    return self  
DynamicFrame.create_dxf = create_dxf

   
    

