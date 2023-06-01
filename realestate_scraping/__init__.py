from dagster import Definitions, ConfigurableResource, EnvVar, IOManager, io_manager, ConfigurableIOManager
import os, requests
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
    path_to_raw: str


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
    path_to_raw: str
    #s3 : S3Credentials()
    access_key: str
    secret_key: str
    endpoint: str


    def _get_spark_session(self):
        builder = pyspark.sql.SparkSession.builder.appName("RealEstateApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config('spark.hadoop.fs.s3a.access.key', self.access_key)  \
        .config('spark.hadoop.fs.s3a.secret.key', self.secret_key)  \
        .config('spark.hadoop.fs.s3a.endpoint', self.endpoint) \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', "false") \
        .enableHiveSupport()
        
        my_packages = ["org.apache.hadoop:hadoop-aws:3.3.2"]
        spark = configure_spark_with_delta_pip(builder, extra_packages=my_packages).getOrCreate()

        return spark

    def _read_delta_table(self):
        spark = self._get_spark_session()
        ## Reading from a Delta table into a PySpark dataframe
        df = (
            spark
            .read.format("delta")
            .load(self.path_to_delta)
        )
        return df.select("*")
    

    def _read_json_properties(self):
        spark = self._get_spark_session()
        ## Reading from a s3 bucket into a PySpark dataframe

        df_zipped = spark \
            .read \
            .format("json") \
            .option("compression", "gzip") \
            .load(self.path_to_raw+"*.gz")
        #df_zipped.printSchema()
        
        return df_zipped
    
    # TO change    
    def handle_output(self, context, obj):
        #obj.write.parquet(self._get_path(context))
        obj.write.parquet('./data')

    # TO change
    def load_input(self, context):
        spark = SparkHelper()._get_spark_session
        #return spark.read.parquet(self._get_path(context.upstream_output))
        return spark.read.parquet('./data')


class LocalParquetIOManager(ConfigurableIOManager):    
    path_to_delta: str
    path_to_raw: str
    #s3 : S3Credentials()
    access_key: str
    secret_key: str
    endpoint: str


    def _get_spark_session(self):
        builder = pyspark.sql.SparkSession.builder.appName("RealEstateApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config('spark.hadoop.fs.s3a.access.key', self.access_key)  \
        .config('spark.hadoop.fs.s3a.secret.key', self.secret_key)  \
        .config('spark.hadoop.fs.s3a.endpoint', self.endpoint) \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', "false") \
        .enableHiveSupport()
        my_packages = ["org.apache.hadoop:hadoop-aws:3.3.2"]
        spark = configure_spark_with_delta_pip(builder, extra_packages=my_packages).getOrCreate()

        return spark
    

    # TO chage
    def _get_path(self, context):
        return os.path.join(context.run_id, context.step_key, context.name)
        

    # TO change    
    def handle_output(self, context, obj):
        #session = SparkHelper()
        #spark = session._get_spark_session()
        
        #obj.write.parquet(self._get_path(context))
        obj.write.mode("overwrite").parquet('./data')


    # TO change
    def load_input(self, context):
        #spark = SparkSession.builder.getOrCreate()#SparkHelper()._get_spark_session
        #return spark.read.parquet(self._get_path(context.upstream_output))
        spark_delta = self._get_spark_session()

        return spark_delta.read.parquet('./data')
#deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")



@io_manager
def local_parquet_io_manager():
    return LocalParquetIOManager(ConfigurableIOManager)

defs = Definitions(
    assets=all_assets,
    resources={
        "s3": S3Credentials(
            access_key=EnvVar("MINIO_ACCESS_KEY"),
            secret_key=EnvVar("MINIO_SECRET_KEY"),
            endpoint=EnvVar("MINIO_ENDPOINT"),
            path_to_raw=EnvVar("MINIO_RAW_BUCKET")
        ),
        "spark_delta": SparkHelper(
            access_key=EnvVar("MINIO_ACCESS_KEY"),
            secret_key=EnvVar("MINIO_SECRET_KEY"),
            endpoint=EnvVar("MINIO_ENDPOINT"),
            path_to_delta=EnvVar("DELTA_TABLE_PATH"),
            path_to_raw=EnvVar("MINIO_RAW_BUCKET")
        ),
        "local_parquet_io_manager": LocalParquetIOManager(
            access_key=EnvVar("MINIO_ACCESS_KEY"),
            secret_key=EnvVar("MINIO_SECRET_KEY"),
            endpoint=EnvVar("MINIO_ENDPOINT"),
            path_to_delta=EnvVar("DELTA_TABLE_PATH"),
            path_to_raw=EnvVar("MINIO_RAW_BUCKET")
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