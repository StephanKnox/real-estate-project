from dagster import asset, get_dagster_logger, Output
from bs4 import BeautifulSoup
from . import helper_functions as hf
from datetime import datetime
from minio import Minio
import os


REALESTATE_BASE_URL = 'https://www.immoscout24.ch/en/real-estate/buy/city-'
REALESTATE_CITY = 'zuerich'
REALESTATE_RADIUS = '1'
LOCAL_PATH = './realestate_scraping/data/'


@asset(
    #group_name="scraping",
    io_manager_key="io_manager", 
)
def download_pages(context):
    date_today = datetime.today().strftime('%y%m%d')
    last_page_number = hf.get_last_page_number(REALESTATE_BASE_URL, REALESTATE_CITY, REALESTATE_RADIUS)

    for idx in range(1, last_page_number + 1):
        url = (
            REALESTATE_BASE_URL 
            + REALESTATE_CITY
            + '?pn='  # pn= page site
            + str(idx)
            + '&r='
            + str(REALESTATE_RADIUS)
            + '&map=1'  # only property with prices (otherwise mapping id to price later on does not map)
            + ''
        )
        context.log.info(url)
        filename = date_today+f"_real_estate_data_{REALESTATE_CITY}_{REALESTATE_RADIUS}_km_part_"+str(idx)+".html"
        driver = hf.init_webdriver()
        driver.implicitly_wait(5) # seconds
        try:
            driver.get(url)
            page = str(driver.page_source)
            with open(LOCAL_PATH + filename, "w") as f:
                f.write(page)
            return [1, 2, 3]
        except ConnectionError as e :
            context.log.error(f"Connection Error: Could not connect to {page}")


# TO DO: Change return type to a property dataframe? -> PropertyDataFrame, where to define PropertyDataFrame type?
@asset
def scrape_pages(context, download_pages):
    dict_ids_prices = {}
    pages_to_scrap = hf.get_pages_from_local(LOCAL_PATH)

    for page in pages_to_scrap:
        ids = []
        prices = []
        with open(page, 'r') as f:
            soup = BeautifulSoup(f, "html.parser")
            ids = hf.parse_ids(soup)
            prices = hf.parse_prices(soup)

            for _idx in range(len(ids)):
                dict_ids_prices[ids[_idx]] = prices[_idx]
        
    dict_prop_df = []
    for _idx in dict_ids_prices:
        last_normalized_price = dict_ids_prices[_idx]
        dict_prop_df.append(
            {
            'id': _idx,
            'fingerprint': str(_idx) + '-' + str(last_normalized_price),
            'city': REALESTATE_CITY,
            'radius': REALESTATE_RADIUS,
            'last_normalized_price': last_normalized_price
            }
        )
    #context.log.info(dict_prop_df)
    return dict_prop_df  


# @asset
#def upload_to_s3(files_list):
#
#   minio_url = "localhost:9000"
#   minio_access_key = "Cb5bODHLhocuw9gH"
#   minio_secret_key = "3utk358B2rHGwMegTiFY01FUsbBWHcVj"
#   minio_bucket_name = "delta"
#
#   minio_client = Minio(endpoint=minio_url, access_key=minio_access_key, secret_key=minio_secret_key, secure=False)
#   for _file in files_list:
#       filename = os.path.basename(_file)
#       filepath = os.path.abspath(_file)
#       minio_client.fput_object(bucket_name=minio_bucket_name, object_name=filename, file_path=filepath)