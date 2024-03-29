from dagster import Definitions, ConfigurableResource, EnvVar, IOManager, io_manager, ConfigurableIOManager
import os, requests
from dagster_aws.s3 import s3_resource
from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
from .assets import core_assets
import pyspark
from datetime import datetime, timezone
from minio import Minio
from minio.commonconfig import  CopySource



all_assets = [*core_assets]

class S3Credentials(ConfigurableResource):
    access_key: str
    secret_key: str
    endpoint: str
    path_to_raw: str
    path_to_stg: str


    def _get_s3_client(self):
        return Minio(
                endpoint=self.endpoint, 
                access_key=self.access_key, 
                secret_key=self.secret_key, 
                secure=False)
    

    def _list_files_s3(self, bucket_name, prefix=""):
        minio = self._get_s3_client()
        return minio.list_objects(bucket_name, prefix=prefix,recursive=True)    
    #"/lake/bronze/property",

    def _upload_object_to_s3(self, bucket_name, key, filename):
        s3_client = self._get_s3_client()
        s3_client.fput_object(bucket_name, key, filename)


    def _download_object_from_s3(self, bucket_name, key, filename):
        s3_client = self._get_s3_client()
        object = s3_client.fget_object(bucket_name, key, filename)
        return object
    

    def _delete_objects_from_s3_bucket(self, bucket_name):
        objects_list = self._list_files_s3(bucket_name)
        minio = self._get_s3_client()

        for _obj in objects_list:       
            minio.remove_object(bucket_name, _obj._object_name)


    def _move_object_between_buckets(self, src_bucket, tgt_bucket):
        s3_client = self._get_s3_client()
        today = datetime.now().date().strftime("%y%m%d")

        [ s3_client.copy_object(tgt_bucket, _obj.object_name, CopySource(src_bucket, _obj.object_name))
        for _obj in self._list_files_s3(src_bucket)
        if today in _obj.object_name ]
        

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
        return df_zipped
    
    
    def handle_output(self, context, obj):
        obj.write.parquet('./data')


    def load_input(self, context):
        spark = SparkHelper()._get_spark_session
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
    

    def _get_path(self, context):
        return os.path.join(context.run_id, context.step_key, context.name)
        
  
    def handle_output(self, context, obj):
        obj.write.mode("overwrite").parquet('./data')


    def load_input(self, context):
        spark_delta = self._get_spark_session()
        return spark_delta.read.parquet('./data')



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
            path_to_raw=EnvVar("MINIO_RAW_BUCKET"),
            path_to_stg=EnvVar("MINIO_STG_BUCKET")
        ),
        "spark_delta": SparkHelper(
            access_key=EnvVar("MINIO_ACCESS_KEY"),
            secret_key=EnvVar("MINIO_SECRET_KEY"),
            endpoint=EnvVar("MINIO_ENDPOINT"),
            path_to_delta=EnvVar("DELTA_TABLE_PATH"),
            path_to_raw=EnvVar("PATH_TO_RAW")
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
