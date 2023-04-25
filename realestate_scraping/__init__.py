from dagster import Definitions, ConfigurableResource, EnvVar
import os
from dagster_aws.s3 import s3_resource
from minio import Minio

from .assets import core_assets

all_assets = [*core_assets]

class S3Credentials(ConfigurableResource):
    access_key: str
    secret_key: str
    endpoint: str
    bucket_name: str

    #def __init__(self, s3_resource):
     #   self.s3_resource = s3_resource

    def _get_s3_client(self):
        return Minio(
                endpoint=self.endpoint, 
                access_key=self.access_key, 
                secret_key=self.secret_key, 
                secure=False)
    
    def _list_files_s3(self):
        return self._get_s3_client.list_objects(self.bucket_name, prefix="/lake/bronze/property",recursive=True)    
    
    
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
            bucket_name=EnvVar("MINIO_RAW_BUCKET")
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