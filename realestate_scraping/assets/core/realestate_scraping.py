from dagster import asset, get_dagster_logger, Output, FileHandle, Definitions
from bs4 import BeautifulSoup
from . import helper_functions as hf
from datetime import datetime
import os, time
from pyspark.sql import  DataFrame, Row
from pyspark.sql.types import StructField, StructType, StringType, ArrayType
from pyspark.sql.functions import explode_outer, col
from delta.tables import DeltaTable
import pandas as pd
import pandasql as psql
import pyspark.pandas as ps
import pyarrow
from botocore.exceptions import NoCredentialsError
from delta.pip_utils import configure_spark_with_delta_pip
from dotenv import load_dotenv


load_dotenv() 

REALESTATE_BASE_URL = os.getenv('REALESTATE_BASE_URL')
REALESTATE_CITY = os.getenv('REALESTATE_CITY')
REALESTATE_RADIUS = os.getenv('REALESTATE_RADIUS')
LOCAL_PATH = os.getenv('LOCAL_PATH')
LOCAL_PATH_OUT = os.getenv('LOCAL_PATH_OUT')
BUCKET_RAW = os.getenv('BUCKET_RAW')
API_ENDPOINT = os.getenv('API_ENDPOINT_REALESTATE')


@asset()
def download_pages(context): 
    """Downloads .html pages from realestate website to a local ./data/html/ folder"""
    
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
            + '&map=1'  # only property with prices
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
                context.log.info(f"File {filename} written to {LOCAL_PATH}")
        except ConnectionError as e :
            context.log.error(f"Connection Error: Could not connect to {page}")


@asset ()
def scrape_pages(context, download_pages):
    """Scrapes downloaded .html pages on local ./data/html/ folder for ids and prices.
    Only pages which were created today() are scraped"""
    dict_ids_prices = {}
    pages_to_scrap = hf.get_pages_from_local(LOCAL_PATH)
    context.log.info(f"Pages to scrap: {pages_to_scrap}")

    for cnt, page in enumerate(pages_to_scrap):
        ids = []
        prices = []
        with open(page, 'r') as f:
            soup = BeautifulSoup(f, "html.parser")
            ids = hf.parse_ids(soup)
            prices = hf.parse_prices(soup)

            for _idx in range(len(ids)):
                dict_ids_prices[ids[_idx]] = prices[_idx]
    context.log.info(f"{cnt+1} pages processed.")

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
    context.log.info(f"{len(dict_prop_df)} properties scraped.")
    return dict_prop_df  


@asset(required_resource_keys={"spark_delta", "s3"})
def get_new_or_changed_props(context, scrape_pages):
    """Custom CDC task which compares scraped property ids and prices and determines
    if the property needs to be requested from an API.
    Only new properties and existing properies with changed prices are returned in a form
    of a dictionary"""

    ids: list = [p['id'] for p in scrape_pages]
    ids: str = ', '.join(ids)
    spark_session = context.resources.spark_delta._get_spark_session()

    cols_PropertyDataFrame = [
        'id', 'fingerprint',
#       'is_prefix', 'rentOrBuy',
        'city',
#       'propertyType',
        'radius', 'last_normalized_price',
    ]
    cols_props = ['propertyDetails_id', 'fingerprint']

    pd_scraped_properties = pd.DataFrame(scrape_pages, columns=cols_PropertyDataFrame)
    existing_props: list = (spark_session.sql(
            """SELECT DISTINCT propertyDetails_id
                , CAST(propertyDetails_id AS STRING)
                    || '-'
                    || propertyDetails_normalizedPrice AS fingerprint
            FROM delta.`s3a://real-estate/lake/bronze/property`
            WHERE propertyDetails_id IN ( {ids} )
            """.format(
                ids=ids
            )
        )
        .select('propertyDetails_id', 'fingerprint')
        .collect()
    )

    pd_existing_properties = pd.DataFrame(existing_props, columns=cols_props)
    pd_changed_properties = psql.sqldf(""" 
    SELECT 
        p.id, p.fingerprint, p.city, p.radius, p.last_normalized_price
    FROM pd_scraped_properties p 
    LEFT JOIN pd_existing_properties e
    ON p.id = e.propertyDetails_id
    WHERE p.fingerprint != e.fingerprint OR e.fingerprint IS NULL
    """)

    if pd_changed_properties.empty:
        context.log.info("No property of [{}] changed".format(ids))
    else:
        changed_properties = []
        for index, row in pd_changed_properties.iterrows():
            changed_properties.append(row.to_dict())

        ids_changed = ', '.join(str(e) for e in pd_changed_properties['id'].tolist())

        context.log.info(f"Number of new or changed properties: {len(changed_properties)}")
        context.log.info("New or changed properties ids: {}".format(ids_changed))

    return changed_properties
        

@asset()
def cache_properties(context, get_new_or_changed_props):  
        """Makes a request to an realestate API with properties returned by the CDC tasks.
        Data files are gzipped and saved on local ./data/out/"""
        
        tot_len = len(get_new_or_changed_props)

        for idx, _property in enumerate(get_new_or_changed_props):  
            context.log.info(_property['id'])
            result = hf.get_property_from_api(API_ENDPOINT, _property['id'])
            if result.ok:
                hf.cache_property_from_api(result, _property, LOCAL_PATH_OUT)
            elif result.status_code == 503:
                time.sleep(600)
                result = hf.get_property_from_api(API_ENDPOINT, _property['id'])
                if result.ok:
                    hf.cache_property_from_api(result, _property, LOCAL_PATH_OUT)
            else:
                context.log.error(f"An error has occured, fetched {idx+1} properties out of {tot_len}.")
                context.log.error(result.status_code)

        context.log.info(f"Finished caching properties / there are {idx+1} properties cached out of {tot_len}.")   


@asset(
    required_resource_keys={"s3"}
)
def upload_to_s3(context, cache_properties): ## 
    """Uploads files retrieved from an realestate API from local /data/out/ folder to S3 WAR bucket on MINIO.
    Only files created today() are taken."""

    pages_to_upload = hf.get_pages_from_local(LOCAL_PATH_OUT)
    
    for _obj in pages_to_upload:
        filename = os.path.basename(_obj)
        raw_bucket_path = context.resources.s3.path_to_raw
        try:
            context.resources.s3._upload_object_to_s3(raw_bucket_path, filename, _obj)
            context.log.info(f"File {_obj} uploaded to '{raw_bucket_path}' bucket")
        except FileNotFoundError:
         context.log.error("The file was not found")
        except NoCredentialsError:
            context.log.error("Credentials not available")

@asset(
        required_resource_keys={"spark_delta", "s3"},
        io_manager_key="local_parquet_io_manager"
)
def json_to_flat_properties(context) -> DataFrame:
    """ 
1. Get all complex (Struct or Array types) on the 1st lvl
2. Expand 1st lvl Struct fields
3. Delete 1st lvl expanded struct fields
4. Explode all 1st lvl Array fields
5. Check if there are more complex fields left on the next lvl
6. If yes repeat the procedure until no more complex fields exist
7. If no more complex fields exist -> return Spark DataFrame
"""
    context.log.info(context.log.info(context.resources.spark_delta.path_to_raw))
    spark_session = context.resources.spark_delta._get_spark_session()
    df = spark_session.read \
        .format("json") \
        .option("compression", "gzip") \
        .load("s3a://staging/*.gz")
    
    context.log.info(f"{df.count()} files were read into a data frame.")
    
    complex_fields = dict(
    [ 
        (_field.name, _field.dataType) 
        for _field in df.schema.fields
        if type(_field.dataType) == StructType
    ]
    )
  
    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]

    # 2. 1st lvl
        if type(complex_fields[col_name]) == StructType:
            expanded_fields = [
                col(col_name + '.' + k).alias(col_name + '_' + k)
                for k in [n.name for n in complex_fields[col_name]]
                ]
            df = df.select("*", *expanded_fields).drop(col_name) # what does * mean exactly? all elements in the list?

    # if ArrayType then add the Array Elements as Rows using the explode function
            # i.e. explode Arrays
        ##elif type(complex_fields[col_name]) == ArrayType:
            ##df = df.withColumn(col_name, explode_outer(col_name))

        complex_fields = dict(
            [
                (field.name, field.dataType)
                for field in df.schema.fields
                ##if type(field.dataType) == ArrayType or type(field.dataType) == StructType
                if type(field.dataType) == StructType
            ]
        )
    #print(complex_fields.keys())
    
    df = df \
        .drop("propertyDetails_images") \
        .drop("propertyDetails_pdfs") \
        .drop("propertyDetails_commuteTimes_defaultPois_transportations") \
        .drop("viewData_viewDataWeb_webView_structuredData")
    
    context.log.info(df.count())
    return df
    

@asset(
        required_resource_keys={"spark_delta"},
        io_manager_key="local_parquet_io_manager"
)
def create_delta_table(context, json_to_flat_properties: DataFrame):
    df = json_to_flat_properties
    spark_session = context.resources.spark_delta._get_spark_session()
    spark_session.sql("""CREATE DATABASE IF NOT EXISTS realestate""")
    spark_session.sql("""DROP TABLE IF EXISTS realestate.property""")

    df.write.format("delta")\
        .mode("overwrite")\
        .option("mergeSchema", "true") \
        .save("s3a://real-estate/lake/bronze/property")
    
    df_delta = spark_session.read.format("delta") \
        .load("s3a://real-estate/lake/bronze/property")

    context.log.info(f"Schema of the delta table: \n{df_delta.count()}")


@asset(
    required_resource_keys={"s3"}
)
def move_from_raw_to_stg(context, upload_to_s3):
    """Moves files from RAW bucket on MINIO S3 to STAGING bucket on MINIO S3.
    TO DO: Take only files uploaded today()"""

    raw_bucket = context.resources.s3.path_to_raw
    stg_bucket = context.resources.s3.path_to_stg
    
    try:
        context.resources.s3._delete_objects_from_s3_bucket(stg_bucket)
    except NoCredentialsError:
        context.log.error("Credentials not available")
    except Exception as e:
        context.log.error(f"Error occurred while deleting objects from {stg_bucket} bucket" + str(e))
    
    context.resources.s3._move_object_between_buckets(raw_bucket, stg_bucket)


@asset(
        required_resource_keys={"spark_delta"},
        io_manager_key="local_parquet_io_manager"
)
def merge_delta(context, json_to_flat_properties: DataFrame):
    df_input = json_to_flat_properties
    df_input.createOrReplaceTempView("new_properties")
    context.log.info(df_input.select("propertyDetails_id").count())

    spark = context.resources.spark_delta._get_spark_session()

    spark.sql(
            """
           MERGE INTO delta.`s3a://real-estate/lake/bronze/property` trg
            USING new_properties AS src
            ON trg.propertyDetails_id = src.propertyDetails_id
            WHEN MATCHED THEN
            UPDATE SET *
            WHEN NOT MATCHED THEN
            INSERT *
            """)



