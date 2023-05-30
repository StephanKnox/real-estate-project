from dagster import Definitions, ConfigurableResource, EnvVar
import os
from dagster_aws.s3 import s3_resource
from minio import Minio
from delta.pip_utils import configure_spark_with_delta_pip
import pyspark
#from .assets import core_assets
#import pyspark
from pyspark.sql.types import ArrayType, StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, explode_outer, upper


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

## df_json = spark.read.format("json") \
##    .option("multiLine", "true") \
##   .load("/Users/ctac/Desktop/Data Engineer /Projects/realestate_scraping_project/realestate-scraping/realestate_scraping/json_example.json")

path_to_file = (f"s3a://raw/5659897_230414_zuerich_10km.gz")

df_json = spark \
    .read \
    .format("json") \
    .option("compression", "gzip") \
    .load(path_to_file)

#df_json.printSchema()

# 0. Drop unnecessary columns from a data frame
# 1. Get all complex (Struct or Array types) on the 1st lvl
# 2. Expand 1st lvl Struct fields
# 3. Delete 1st lvl expanded struct fields
# 4. Explode all 1st lvl Array fields
# 5. Check if there are more complex fields left on the next lvl
# 6. If yes repeat the procedure until no more complex fields exist
# 7. If no more complex fields exist -> return Spark DataFrame
#print(df_json.schema.fields)


# 0.
#df_json.printSchema()


# 1. Get all complex (Struct or Array types) on the 1st lvl
complex_fields = dict(
    [ 
        (_field.name, _field.dataType) 
        for _field in df_json.schema.fields
        if type(_field.dataType) == ArrayType or type(_field.dataType) == StructType
    ]
)

#print(complex_fields.values())
#print(type(complex_fields["commuteTimes"]) == StructType)

while len(complex_fields) != 0:
    col_name = list(complex_fields.keys())[0]
    #print(f"Column name is {col_name} \n")
    
    #if col_name in context.solid_config['remove_columns']:
            # remove and skip next part
     #       df = df.drop(col_name)
      #  else:
            # if StructType then convert all sub element to columns.
            # i.e. flatten structs

# 2. 1st lvl
    if type(complex_fields[col_name]) == StructType:
        expanded_fields = [
                col(col_name + '.' + k).alias(col_name + '_' + k)
                for k in [n.name for n in complex_fields[col_name]]
                ]
        df_json = df_json.select("*", *expanded_fields).drop(col_name) # what does * mean exactly? all elements in the list?
    #df_json.printSchema()

    # if ArrayType then add the Array Elements as Rows using the explode function
            # i.e. explode Arrays
    elif type(complex_fields[col_name]) == ArrayType:
        df_json = df_json.withColumn(col_name, explode_outer(col_name))

        df_json = df_json \
        .drop("propertyDetails_images") \
        .drop("propertyDetails_pdfs") \
        .drop("propertyDetails_commuteTimes_defaultPois_transportations") \
        .drop("viewData_viewDataWeb_webView_structuredData")

    #df_json.printSchema()

        # recompute remaining Complex Fields in Schema
    complex_fields = dict(
            [
                (field.name, field.dataType)
                for field in df_json.schema.fields
                if type(field.dataType) == ArrayType or type(field.dataType) == StructType
            ]
        )
    print(complex_fields.keys())
        #context.log.debug(
        #    'count of rows, in case of no errors, count should stay the same. Count: '
        #    + str(df.count())
        #)
print("Final df schema is: \n")
df_json.printSchema()
## return df
#print(col_name)


#print(expanded_fields)

