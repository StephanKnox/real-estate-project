from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
import pyspark.sql.functions as F
#from delta import *
from delta.pip_utils import configure_spark_with_delta_pip


# Initialize a Spark session
spark = (
    SparkSession.builder \
    #builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .appName("DeltaLakeFundamentals")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    #.config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1")
)

spark = configure_spark_with_delta_pip(spark).getOrCreate()


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


df_acidentes = (
    spark
    .read.format("csv")
    .option("delimiter", ";")
    .option("header", "true")
    .option("encoding", "ISO-8859-1")
    .schema(SCHEMA)
    .load("realestate-scraping/realestate_scraping/datatran2022.csv")
)

df_acidentes.show(5)



#spark.range(10).write.format("delta").option("path", "/realestate-scraping/realestate_scraping/").saveAsTable("delta")
#df_acidentes.write.format("delta").mode("overwrite").save("realestate-scraping/realestate_scraping/people10m")
df_acidentes\
    .write.format("delta")\
    .mode("overwrite")\
    .save("realestate-scraping/realestate_scraping/delta")