from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from delta.pip_utils import configure_spark_with_delta_pip


## Initialize a Spark session
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


#df_acidentes = (
#    spark
#    .read.format("csv")
#    .option("delimiter", ";")
#    .option("header", "true")
#    .option("encoding", "ISO-8859-1")
#    .schema(SCHEMA)
#   .load("realestate-scraping/realestate_scraping/datatran2022.csv")
#)

#df_acidentes.show(5)



## Creating a Delta table from a PySpark dataframe
#df_acidentes\
#    .write.format("delta")\
#    .mode("overwrite")\
#    .save("realestate-scraping/realestate_scraping/delta")


# Reading from a Delta table into a PySpark dataframe
path_to_delta = 'file:///Users/ctac/Desktop/Data Engineer /Projects/realestate_scraping_project/realestate-scraping/realestate_scraping/delta'
df_acidentes_delta = (
    spark
    .read.format("delta")
    .load(path_to_delta)
)

## Reading from a Delta table using SQL-like interface
spark.sql(f"SELECT * FROM delta.`{path_to_delta}` WHERE dia_semana = 'sábado'").show()

#df_acidentes_delta.select(["id", "data_inversa", "dia_semana", "horario", "uf"]).show(3)

# Print count of number of rows in a Delta table
#print(df_acidentes_delta.count())

#spark.sql("select * from delta.`hdfs://192.168.2.131:9000/Delta_Table/test001`").show()

## READING THE 2021 DATA
#df_acidentes_2021 = (
#    spark
#    .read.format("csv")
#    .option("delimiter", ";")
#    .option("header", "true")
#    .schema(SCHEMA)
#    .load("file:///Users/ctac/Desktop/Data Engineer /Projects/realestate_scraping_project/realestate-scraping/realestate_scraping/datatran2021.csv")
#)

## Append data to a Delta table
#df_acidentes_2021\
#    .write.format("delta")\
#    .mode("append")\
#    .save(path_to_delta)

#print(df_acidentes_delta.count())


## Check Delta table history and versions
#delta_table = DeltaTable.forPath(spark, path_to_delta)
#delta_table.history().show()

#delta_table.history().select("version", "timestamp", "operation", "operationParameters").show(10, False)