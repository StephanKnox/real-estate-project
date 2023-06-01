from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
import pyspark
from datetime import timedelta
from ratelimit import limits, sleep_and_retry


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

df = spark.sql(
            """SELECT DISTINCT propertyDetails_id
                , CAST(propertyDetails_id AS STRING)
                    || '-'
                    || propertyDetails_normalizedPrice AS fingerprint,
                    propertyDetails_searchCity as city
            FROM delta.`s3a://real-estate/lake/bronze/property`
            WHERE propertyDetails_id IN (7636004, 7621229)
            """
            )
 
df.select('city').show()
