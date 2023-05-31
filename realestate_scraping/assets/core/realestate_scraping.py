from dagster import asset, get_dagster_logger, Output, FileHandle, Definitions
from bs4 import BeautifulSoup
from . import helper_functions as hf
from datetime import datetime
import os
from pyspark.sql import  DataFrame, Row
from pyspark.sql.types import StructField, StructType, StringType, ArrayType
from pyspark.sql.functions import explode_outer, col
from delta.tables import DeltaTable
#from delta.pip_utils import configure_spark_with_delta_pip
import pandas as pd
import pandasql as psql
import pyspark.pandas as ps
import pyarrow
from botocore.exceptions import NoCredentialsError
from delta.pip_utils import configure_spark_with_delta_pip
#from minio.error import ResponseError


REALESTATE_BASE_URL = 'https://www.immoscout24.ch/en/real-estate/buy/city-'
REALESTATE_CITY = 'zuerich'
REALESTATE_RADIUS = '15'
LOCAL_PATH = './realestate_scraping/data/'
LOCAL_PATH_TMP = '/Users/ctac/Desktop/Data Engineer /Projects/GPU-Prices-Scrapper/out/data'
BUCKET_RAW = 'raw'


    #group_name="scraping",
    #io_manager_key="io_manager", 
@asset(
        #io_manager_key="fs_io_manager"
        )
def download_pages(context): #-> FileHandle:
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
                context.log.info(f"File {filename} written to {LOCAL_PATH}")
        except ConnectionError as e :
            context.log.error(f"Connection Error: Could not connect to {page}")


# TO DO: Change return type to a property dataframe? -> PropertyDataFrame, where to define PropertyDataFrame type?
@asset (
        #io_manager_key="local_parquet_io_manager"
        )
def scrape_pages(context, download_pages):
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
    context.log.info(f"{cnt} pages processed.")

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
def filter_for_new_or_changed_properties(context, scrape_pages):
    #context.log.info(scrape_pages)
    ids: list = [p['id'] for p in scrape_pages]
    ids: str = ', '.join(ids)
    #context.log.info(ids)

    spark_session = context.resources.spark_delta._get_spark_session()

    cols_PropertyDataFrame = [
        'id',
        'fingerprint',
 #       'is_prefix',
#        'rentOrBuy',
        'city',
 #       'propertyType',
        'radius',
        'last_normalized_price',
    ]
    cols_props = ['propertyDetails_id', 'fingerprint']

    pd_scraped_properties = pd.DataFrame(scrape_pages, columns=cols_PropertyDataFrame)
    #df_changed = psql.sqldf(
    #    """ 
    #    SELECT id, fingerprint, city
    #    FROM pd_scraped_properties
    #    """)
    #context.log.info(df_changed)
    ##df_existing = spark_session.sql(""" """)
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
    context.log.info(f"Existing properties : {pd_existing_properties}")

    pd_changed_properties = psql.sqldf(""" 
    SELECT 
        p.id, p.fingerprint, p.city, p.radius, p.last_normalized_price
    FROM pd_scraped_properties p 
    LEFT OUTER JOIN pd_existing_properties e
    ON p.id = e.propertyDetails_id
    WHERE p.fingerprint != e.fingerprint OR e.fingerprint IS NULL""")

    if pd_changed_properties.empty:
        context.log.info("No property of [{}] changed".format(ids))
    else:
        changed_properties = []
        for index, row in pd_changed_properties.iterrows():
            changed_properties.append(row.to_dict())

        ids_changed = ', '.join(str(e) for e in pd_changed_properties['id'].tolist())

        context.log.info(f"Number of new or changed properties: {len(ids_changed)}")
        context.log.info("New or changed properties ids: {}".format(ids_changed))
    #context.lof.info(pd_changed_properties.count())


    #context.log.info(f"Existing properties: {pd_existing_props}")
#defs = Definitions(assets=[scrape_pages])

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


@asset(
    required_resource_keys={"s3"}
)
def upload_to_s3(context):
    #s3_client = context.resources.s3._get_s3_client()
    #objects = s3_client.list_objects(context.resources.s3.bucket_name ,recursive=True)
    pages_to_upload = hf.get_pages_from_local(LOCAL_PATH_TMP)
    
    for _obj in pages_to_upload:
        context.log.info(_obj)
        filename = os.path.basename(_obj)
        try:
        #s3_client.fput_object(context.resources.s3.bucket_name, filename, _obj)
            context.resources.s3._upload_file_to_s3(BUCKET_RAW, filename, _obj)
        #context.log.info(f"File {_obj} uploaded to '{bucket}' bucket")
        except FileNotFoundError:
         context.log.error("The file was not found")
        except NoCredentialsError:
            context.log.error("Credentials not available")
    #for obj in objects:
     #   context.log.info(#obj.bucket_name
      #       obj.object_name.encode('utf-8')) #obj.last_modified +' '+
            #+' '+obj.etag  obj.size, obj.content_type)
    #context.log.info(s3_client.list_buckets())
    

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
    spark_session = context.resources.spark_delta._get_spark_session()
    df = spark_session.read \
        .format("json") \
        .option("compression", "gzip") \
        .load(context.resources.spark_delta.path_to_raw+"*.gz")
    
    context.log.info(f"{df.count()} files were read into a data frame.")
    
    complex_fields = dict(
    [ 
        (_field.name, _field.dataType) 
        for _field in df.schema.fields
        if type(_field.dataType) == ArrayType or type(_field.dataType) == StructType
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
        elif type(complex_fields[col_name]) == ArrayType:
            df = df.withColumn(col_name, explode_outer(col_name))

        complex_fields = dict(
            [
                (field.name, field.dataType)
                for field in df.schema.fields
                if type(field.dataType) == ArrayType or type(field.dataType) == StructType
            ]
        )
    #print(complex_fields.keys())
    
    df = df \
        .drop("propertyDetails_images") \
        .drop("propertyDetails_pdfs") \
        .drop("propertyDetails_commuteTimes_defaultPois_transportations") \
        .drop("viewData_viewDataWeb_webView_structuredData")
    
    #context.log.info(os.path.join(context.run_id, context.step_key, context.name))
    context.log.info(df.count())
    return df
    

@asset(
        required_resource_keys={"spark_delta"},
        io_manager_key="local_parquet_io_manager"
)
def create_delta_table(context, json_to_flat_properties: DataFrame):
    #df = context.resources.spark_delta._read_delta_table()
    df = json_to_flat_properties
    context.log.info(df.count())

    spark_session = context.resources.spark_delta._get_spark_session()
    spark_session.sql("""CREATE DATABASE IF NOT EXISTS realestate""")
    spark_session.sql("""DROP TABLE IF EXISTS realestate.property""")
    ##spark_session.sql("""
    ##    CREATE TABLE IF NOT EXISTS {}.{}
    ##    USING DELTA
    ##    LOCATION "{}"
    ##    """)
    
    #spark_session.sql("""DROP TABLE IF EXISTS 's3a://real-estate/lake/bronze/property'""")
    ##df.write.format("delta")\
    ##    .mode("overwrite")\
    ##    .option("mergeSchema", "true") \
    ##    .save("s3a://real-estate/lake/bronze/property")
    df_delta = spark_session.read.format("delta") \
        .load("s3a://real-estate/lake/bronze/property")

    context.log.info(f"Schema of the delta table: \n{df_delta.select('*').take(2)}")
#print(df_acidentes_delta.show(4))


@asset()
def merge_delta(context):
    pass
