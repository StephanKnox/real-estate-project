from dagster import Definitions, fs_io_manager
import os

from .assets import core_assets

all_assets = [*core_assets]

#deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")

io_manager = fs_io_manager.configured(
    {
        "base_dir": "./realestate_scraping/data/",  
    }
)

defs = Definitions(
    assets=all_assets,
    resources={
        "io_manager": io_manager,
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