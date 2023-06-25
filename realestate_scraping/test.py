from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
import pyspark
from datetime import timedelta
from ratelimit import limits, sleep_and_retry
import pandas as pd
import pandasql as psql
import pyspark.pandas as ps
import numpy as np
import pyspark.sql.functions as F
import pyspark.sql.types as T

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
            """SELECT *
            FROM delta.`s3a://real-estate/lake/bronze/property`
            """
            )
print(df.printSchema())
# Get boolean columns' names
##all_columns = [(col[0], col[1]) for col in df.dtypes]
##for _col in all_columns:
    ##print(_col)

# Cast boolean to Integers
#for col in bool_columns:
#    df = df.withColumn(col, F.col(col).cast(T.IntegerType()))

bool_columns1 = [col[0] for col in df.dtypes if col[1] == 'boolean']
#print(bool_columns1)
for col in bool_columns1:
    df = df.withColumn(col, F.col(col).cast(T.IntegerType()))

df_pandas = df.select("*").toPandas()

print(df_pandas)
#df_pandas.to_csv("./delta_meilen_extract/pandas_extact.csv")
