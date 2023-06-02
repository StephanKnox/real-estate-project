from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
import pyspark
from datetime import timedelta, timezone
from ratelimit import limits, sleep_and_retry
import datetime as dt
from datetime import datetime
from minio import Minio
from minio.commonconfig import  CopySource


MINIO_ENDPOINT='localhost:9000'
MINIO_ACCESS_KEY='Cb5bODHLhocuw9gH'
MINIO_SECRET_KEY='3utk358B2rHGwMegTiFY01FUsbBWHcVj'

builder = pyspark.sql.SparkSession.builder.appName("RealEstateApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config('spark.hadoop.fs.s3a.access.key', "Cb5bODHLhocuw9gH")  \
        .config('spark.hadoop.fs.s3a.secret.key', "3utk358B2rHGwMegTiFY01FUsbBWHcVj")  \
        .config('spark.hadoop.fs.s3a.endpoint', "localhost:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', "false") \
        .enableHiveSupport()
        
my_packages = ["org.apache.hadoop:hadoop-aws:3.3.2"]
spark = configure_spark_with_delta_pip(builder, extra_packages=my_packages).getOrCreate()
minio = Minio(
                endpoint=MINIO_ENDPOINT, 
                access_key=MINIO_ACCESS_KEY, 
                secret_key=MINIO_SECRET_KEY, 
                secure=False)

today = dt.datetime.now().date().strftime("%y%m%d")
"""
print(today)
print(int(today.strftime("%Y")))
print(int(today.strftime("%m")))
print(int(today.strftime("%d")))
print(datetime(int(today.strftime("%Y")), 
                                                        int(today.strftime("%m")), 
                                                        int(today.strftime("%d"))))
"""
list_staging = minio.list_objects("staging", prefix="",recursive=True)
print(list_staging)

for _obj in list_staging:       
        result = minio.remove_object("staging", _obj._object_name)
        print(result)

##list_raw = [_obj.object_name 
##            for _obj in minio.list_objects("raw", prefix="",recursive=True) 
##            if today in _obj.object_name]
##print(list_raw)

#for _file in list_raw:
#    if today in _file.object_name:
#        minio.copy_object("staging", _file.object_name, CopySource("raw", _file.object_name),
#    )


#print(result.object_name, result.version_id)

#df = spark.read.format("json").option("compression", "gzip").load(f"s3a://raw/"+new_list)



