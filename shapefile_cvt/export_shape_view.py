from awsglue import DynamicFrame
from sedona.spark import SedonaContext
import uuid
import boto3
import os
import fsspec
import json
from shapely.geometry import shape
import shapefile

def get_shape_features(shape_path_tmp):
    
    fs = fsspec.filesystem("s3")
    files = fs.glob(f"{shape_path_tmp.rstrip('/')}/*.json")
    all_features = []
    for file in files:
        with fs.open(file) as f:
            for line in f:
                obj = json.loads(line)
                geom = shape(obj["geometry"])
                props = obj["properties"]
                props["geometry"] = geom
                all_features.append(props)
    return all_features

def delete_s3_folder(bucket, prefix):
    s3 = boto3.resource('s3')
    bucket_obj = s3.Bucket(bucket)
    bucket_obj.objects.filter(Prefix=prefix).delete()
    print(f"Deleted s3://{bucket}/{prefix}")

def split_s3_path(s3_path):
    if not s3_path.startswith("s3://"):
        raise ValueError("Invalid S3 path")

    path = s3_path[5:]
    parts = path.split("/", 1)

    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""

    return bucket, prefix

def export_shape_view(self, view,location,filename=None):
    spark=self.glue_ctx.sparkSession
    sedona = SedonaContext.create(spark)
    df=sedona.sql(f"select * from {view}")
    uuid_dir = str(uuid.uuid4())
    location=location.rstrip("/")
    shape_path_tmp=f"{location}/{uuid_dir}/"
    df.write.format("geojson").save(shape_path_tmp)

    all_features=get_shape_features(shape_path_tmp)

    local_temp_dir=f"/tmp/{uuid_dir}"
    os.makedirs(local_temp_dir, exist_ok=True)
    filename = filename or view
    shapefile_base = os.path.join(local_temp_dir, filename)

    writer = shapefile.Writer(shapefile_base, shapeType=shapefile.POINT)
    writer.field("NAME", "C")

    for feature in all_features:
        geom = feature["geometry"]
        name = feature.get("NAME", "")
        if geom.geom_type == "Point":
            writer.point(geom.x, geom.y)
            writer.record(name)
    writer.close()

    with open(f"{shapefile_base}.prj", "w") as f:
        f.write(
            'GEOGCS["WGS 84",'
            'DATUM["WGS_1984",'
            'SPHEROID["WGS 84",6378137,298.257223563]],'
            'PRIMEM["Greenwich",0],'
            'UNIT["degree",0.0174532925199433]]'
        )
    
    s3 = boto3.client("s3")

    bucket,prefix=split_s3_path(location)

    for ext in ["shp", "shx", "dbf", "prj"]:
        local_path = f"{shapefile_base}.{ext}"
        if os.path.exists(local_path):
            s3.upload_file(local_path, bucket, f"{prefix}/{filename}.{ext}")
        else:
            print(f" Missing: {local_path}")
            
    bucket,prefix=split_s3_path(shape_path_tmp.rstrip("/"))
    delete_s3_folder(bucket,prefix)

    #you can return a DyF, but that doesn't implement most methods and will fail. So essentially, don't use, just return 
    return DynamicFrame.fromDF(df, self.glue_ctx, "export_shape_view_txnc")   
DynamicFrame.export_shape_view = export_shape_view

'''
def is_s3_folder_path(path: str) -> bool:
    return bool(path) and path.startswith("s3://") and path.endswith("/")

'''
