from dagster import Definitions, ConfigurableResource, EnvVar
import os
from dagster_aws.s3 import s3_resource
from minio import Minio
from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
#from .assets import core_assets
#import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
import pyspark

from pyspark import SparkConf
from pyspark.sql import SparkSession


"""
conf = SparkConf()
#conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.2')
#conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set('spark.hadoop.fs.s3a.access.key', "Cb5bODHLhocuw9gH") 
conf.set('spark.hadoop.fs.s3a.secret.key', "3utk358B2rHGwMegTiFY01FUsbBWHcVj") 
conf.set('spark.hadoop.fs.s3a.endpoint', "localhost:9000")
conf.set("spark.hadoop.fs.s3a.path.style.access", True)
conf.set('spark.hadoop.fs.s3a.connection.ssl.enabled', "false")
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
"""
# Create a session which can read and write to s3 and to and from delta table on s3
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.access.key', "Cb5bODHLhocuw9gH")  \
    .config('spark.hadoop.fs.s3a.secret.key', "3utk358B2rHGwMegTiFY01FUsbBWHcVj")  \
    .config('spark.hadoop.fs.s3a.endpoint', "localhost:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config('spark.hadoop.fs.s3a.connection.ssl.enabled', "false") \
    #.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    #.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# works well 
#spark = SparkSession.builder.config(conf=conf).getOrCreate()

#spark = configure_spark_with_delta_pip(spark).getOrCreate()
my_packages = ["org.apache.hadoop:hadoop-aws:3.3.2"]
spark = configure_spark_with_delta_pip(builder, extra_packages=my_packages).getOrCreate()

#my_packages = ["org.apache.hadoop:hadoop-aws:3.3.1"]
#spark = configure_spark_with_delta_pip(builder, extra_packages=my_packages).getOrCreate()

#print(f"Spark version = {spark.version}")

# hadoop
#print(f"Hadoop version = {spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")

SCHEMA = StructType(
    [
        StructField('id', StringType(), True),          # ACCIDENT ID
        StructField('data_inversa', StringType(), True),# DATE
        StructField('dia_semana', StringType(), True),  # DAY OF WEEK
        StructField('horario', StringType(), True),     # HOUR
        StructField('uf', StringType(), True),          # BRAZILIAN STATE
        StructField('br', StringType(), True),          # HIGHWAY
        # AND OTHER FIELDS OMITTED TO MAKE THIS CODE BLOCK SMALL
    ]
)

path_to_delta = (f"s3a://raw/datatran2022.csv")


## Read a .csv file into spark dataframe
df_acidentes = (
    spark
    .read.format("csv")
    .option("delimiter", ";")
    .option("header", "true")
    .option("encoding", "ISO-8859-1")
    .schema(SCHEMA)
  .load("s3a://raw/datatran2022.csv")
)
#print(df_acidentes.show(10))
    
## Creating a Delta table from a PySpark dataframe
df_acidentes\
    .write.format("delta")\
    .mode("overwrite")\
    .save("s3a://real-estate/lake/bronze/property")


