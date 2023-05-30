from dagster import Definitions, ConfigurableResource, EnvVar
import os
from dagster_aws.s3 import s3_resource
from minio import Minio
from delta.pip_utils import configure_spark_with_delta_pip
import pyspark
#from .assets import core_assets
#import pyspark
from pyspark.sql.types import ArrayType, StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, explode_outer

#from pyspark import SparkConf
#from pyspark.sql import SparkSession


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
  
my_packages = ["org.apache.hadoop:hadoop-aws:3.3.2"]
spark = configure_spark_with_delta_pip(builder, extra_packages=my_packages).getOrCreate()

"""
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
"""

df_json = spark.read.format("json") \
    .option("multiLine", "true") \
    .load("./realestate_scraping/json_example.json")
#df_json.show()
#df_json.printSchema()
#print(df_json.schema.fields)
complex_fields = dict(
    [
    (_field.name, _field.dataType) 
    for _field in df_json.schema.fields 
    if (type(_field.dataType) == ArrayType or type(_field.dataType) == StructType)
    ]
)

print(complex_fields)

col_name = list(complex_fields.keys())[0]
print(col_name)
if type(complex_fields[col_name]) == StructType:
    expanded = [
                    col(col_name + '.' + k).alias(col_name + '_' + k)
                    for k in [n.name for n in complex_fields[col_name]]
                ]
    df_json = df_json.select("*", *expanded).drop(col_name)
    print(expanded)
#while len(complex_fields) != 0:

#    col_name = list(complex_fields.keys())[0]
#   if type(complex_fields[col_name]) == StructType:
#                expanded = [
#                   col(col_name + '.' + k).alias(col_name + '_' + k)
#                    for k in [n.name for n in complex_fields[col_name]]
#                ]
#                df = df.select("*", *expanded).drop(col_name)

            # if ArrayType then add the Array Elements as Rows using the explode function
            # i.e. explode Arrays
#    elif type(complex_fields[col_name]) == ArrayType:
#                df = df.withColumn(col_name, explode_outer(col_name))

"""
path_to_file = (f"s3a://raw/5659897_230414_zuerich_10km.gz")

df_zipped = spark \
    .read \
    .format("json") \
    .option("compression", "gzip") \
    .load(path_to_file)

#print(df_zipped.printSchema())
#print(df_zipped.show(1))

complex_fields = dict(
        [
            (field.name, field.dataType)
            for field in df_zipped.schema.fields
            if (type(field.dataType) == ArrayType or type(field.dataType) == StructType)
            and field.name.startswith('propertyDetails')
        ]
    )

print(complex_fields)

while len(complex_fields) != 0:

        col_name = list(complex_fields.keys())[0]
        print(
            "Processing :" + col_name + " Type : " + str(type(complex_fields[col_name]))
        )

       
            # if StructType then convert all sub element to columns.
            # i.e. flatten structs
        if type(complex_fields[col_name]) == StructType:
                expanded = [
                    col(col_name + '.' + k).alias(col_name + '_' + k)
                    for k in [n.name for n in complex_fields[col_name]]
                ]
                df_zipped = df_zipped.select("*", *expanded).drop(col_name)

            # if ArrayType then add the Array Elements as Rows using the explode function
            # i.e. explode Arrays
        elif type(complex_fields[col_name]) == ArrayType:
                df_zipped = df_zipped.withColumn(col_name, explode_outer(col_name))

        # recompute remaining Complex Fields in Schema
        complex_fields = dict(
            [
                (field.name, field.dataType)
                for field in df_zipped.schema.fields
                if type(field.dataType) == ArrayType or type(field.dataType) == StructType
            ]
        )
        print(
            'count of rows, in case of no errors, count should stay the same. Count: '
            + str(df_zipped.count())
        )

df_zipped.show(1, False)
"""

## Read a .csv file into spark dataframe
"""
df_acidentes = (
    spark
    .read.format("csv")
    .option("delimiter", ";")
    .option("header", "true")
    .option("encoding", "ISO-8859-1")
    .schema(SCHEMA)
  .load("s3a://raw/datatran2022.csv")
)
"""
    
## Creating a Delta table from a PySpark dataframe
#df_acidentes\
#    .write.format("delta")\
#    .mode("overwrite")\
#    .save("s3a://real-estate/lake/bronze/property")


## Reading a Delta table to a PySpark dataframe
#df_acidentes_delta = (
#    spark
#    .read.format("delta")
#    .load("s3a://real-estate/lake/bronze/property")
#)

#print(df_acidentes_delta.show(4))



