from dagster import Definitions, ConfigurableResource, EnvVar
import os
from dagster_aws.s3 import s3_resource
from minio import Minio
from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
from .assets import core_assets

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
    

    def _list_files_s3(self, bucket_name, prefix):
        return self._get_s3_client.list_objects(bucket_name, prefix=prefix,recursive=True)    
    #"/lake/bronze/property",

    def _upload_file_to_s3(self, bucket_name, key, filename):
        s3_client = self._get_s3_client()
        s3_client.fput_object(bucket_name, key, filename)


class SparkHelper(ConfigurableResource):
    path_to_delta: str
 
    
    def _get_spark_session(self):
        spark = (
            SparkSession.builder \
    #builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    #.appName("DeltaLakeFundamentals")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    #.config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1")
)
        spark = configure_spark_with_delta_pip(spark).getOrCreate()
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