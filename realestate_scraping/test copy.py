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

from pyspark.sql.functions import col, column, expr

df = spark.read.csv("")
