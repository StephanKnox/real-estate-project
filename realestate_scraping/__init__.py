from dagster import Definitions, ConfigurableResource, EnvVar
import os
from dagster_aws.s3 import s3_resource
from minio import Minio
from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
from .assets import core_assets
import pyspark

all_assets = [*core_assets]

class S3Credentials(ConfigurableResource):
    access_key: str
    secret_key: str
    endpoint: str
    #bucket_name: str

    #def __init__(self, s3_resource):
     #   self.s3_resource = s3_resource

    def _get_s3_client(self):
        return Minio(
                endpoint=self.endpoint, 
                access_key=self.access_key, 
                secret_key=self.secret_key, 
                secure=False)
    

    def _list_files_s3(self, bucket_name, prefix=""):
        return self._get_s3_client.list_objects(bucket_name, prefix=prefix,recursive=True)    
    #"/lake/bronze/property",

    def _upload_object_to_s3(self, bucket_name, key, filename):
        s3_client = self._get_s3_client()
        s3_client.fput_object(bucket_name, key, filename)


    def _download_object_from_s3(self, bucket_name, key, filename):
        s3_client = self._get_s3_client()
        object = s3_client.fget_object(bucket_name, key, filename)
        return object



class SparkHelper(ConfigurableResource):
    path_to_delta: str
    #s3 : S3Credentials()

    def _get_spark_session(self):
        #spark = (
           # SparkSession.builder \
    #builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    #.appName("DeltaLakeFundamentals")
           # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            #.config("fs.s3a.endpoint", 'localhost:9000')#self.s3.endpoint)
            #.config("fs.s3a.access.key", 'Cb5bODHLhocuw9gH')#self.s3.access_key)
            #.config("fs.s3a.secret.key", '3utk358B2rHGwMegTiFY01FUsbBWHcVj')#self.s3.secret_key)
    #.config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1")
#)
        #spark = configure_spark_with_delta_pip(spark).getOrCreate()
        #spark = SparkSession.builder.appName("MinioTest").getOrCreate()
        #sc = spark.sparkContext
        #spark.conf.set("spark.hadoop.fs.s3a.endpoint","localhost:9000")
        #spark.conf.set("spark.hadoop.fs.s3a.access.key", "Cb5bODHLhocuw9gH")
        #spark.conf.set("spark.hadoop.fs.s3a.secret.key", "3utk358B2rHGwMegTiFY01FUsbBWHcVj" )
        #spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
        #spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        """
        spark = (SparkSession.builder
             .appName("MinioTest")
             .config("spark.network.timeout", "10000s")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.fast.upload", "true")
             .config("spark.hadoop.fs.s3a.endpoint", "localhost:9000")
             .config("spark.hadoop.fs.s3a.access.key", "Cb5bODHLhocuw9gH")
             .config("spark.hadoop.fs.s3a.secret.key", "3utk358B2rHGwMegTiFY01FUsbBWHcVj")
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.history.fs.logDirectory", "s3a://spark/")
             .config("spark.sql.files.ignoreMissingFiles", "true")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
             .enableHiveSupport()
             .getOrCreate())
        """
        
        builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.access.key", "Cb5bODHLhocuw9gH") \
        .config("spark.hadoop.fs.s3a.secret.key", "3utk358B2rHGwMegTiFY01FUsbBWHcVj") \
        .config("spark.hadoop.fs.s3a.endpoint", "localhost:9000") \
        #.config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        my_packages = ["org.apache.hadoop:hadoop-aws:3.3.2"]
        
        spark = configure_spark_with_delta_pip(builder, extra_packages=my_packages).getOrCreate()
        #spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        #print("Hadoop version: "+spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion())
    

        return spark
        


    def _read_delta_table(self):
        spark = self._get_spark_session()
        ## Reading from a Delta table into a PySpark dataframe
        df_acidentes_delta = (
            spark
            .read.format("delta")
            .load(self.path_to_delta)
        )
        return df_acidentes_delta
    

    def _read_json_properties(self, path):
        spark = self._get_spark_session()
        ## Reading from a s3 bucket into a PySpark dataframe

        df_zipped = spark \
            .read \
            .format("json") \
            .option("compression", "gzip") \
            .option("header", False) \
            .load(path)
        #df_zipped.printSchema()
        
        return df_zipped

    
#deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")

#io_manager = fs_io_manager.configured(
#    {
#        "base_dir": "./realestate_scraping/data/",  
#    }
#)

defs = Definitions(
    assets=all_assets,
    resources={
        "s3": S3Credentials(
            access_key=EnvVar("MINIO_ACCESS_KEY"),
            secret_key=EnvVar("MINIO_SECRET_KEY"),
            endpoint=EnvVar("MINIO_ENDPOINT"),
            #bucket_name=EnvVar("MINIO_RAW_BUCKET")
        ),
        "spark_delta": SparkHelper(
            path_to_delta=EnvVar("DELTA_TABLE_PATH")
        )
    },
)



#defs = Definitions(
#    assets=load_assets_from_package_module(assets),
#    schedules=[
#        ScheduleDefinition(
#            job=define_asset_job(name="daily_refresh", selection="*"),
#           cron_schedule="@daily",
#        )
#    ],
#    resources={"github_api": Github(os.environ["GITHUB_ACCESS_TOKEN"])},
#)