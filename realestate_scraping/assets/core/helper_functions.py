import re, requests, os, json, gzip
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import datetime as dt



class SQL:
    def __init__(self, sql, **bindings):
        self.sql = sql
        self.bindings = bindings


def init_webdriver():
    options = webdriver.ChromeOptions()
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--incognito')
    options.add_argument('--headless=new')
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
    
    return driver


def parse_ids(soup):
    ids = []
    links = soup.findAll('a', href=True)
    hrefs = [item['href'] for item in links]
    hrefs_filtered = [href for href in hrefs if href.startswith('/en/d')]
    ids += [re.findall('\d+', item)[0] for item in hrefs_filtered]

    return ids


def parse_prices(soup):
    prices = []
    h3 = soup.findAll('h3')
    for h in h3:
        text = h.getText()
        if 'CHF' in text or 'EUR' in text:
            start = text.find('CHF') + 4
            end = text.find('.â\x80\x94')

            price = text[start:end]
            # remove all characters except numbers --> equals price
            price = re.sub('\D', '', price)
            prices.append(price)

    return prices


def get_last_page_number(REALESTATE_BASE_URL, REALESTATE_CITY, REALESTATE_RADIUS):
    url = (
            REALESTATE_BASE_URL 
            + REALESTATE_CITY
            + '?r='
            + REALESTATE_RADIUS
            + '&map=1'  # only property with prices (otherwise mapping id to price later on does not map)
            + ''
    )
    driver = init_webdriver()
    driver.get(url)
    page = str(driver.page_source)
    page_nums = []
    soup = BeautifulSoup(page, "html.parser")

    links = soup.findAll('a', href=True)
    hrefs = [item['href'] for item in links]
    hrefs_filtered = [href for href in hrefs if 'pn=' in href ]
    page_nums += [int( re.findall('pn=\d+', item)[0][3:] ) for item in hrefs_filtered]
    last_page = sorted(page_nums)[-1]

    driver.quit()

    return last_page


def get_pages_from_local(LOCAL_PATH):
    files_list = []
    today = dt.datetime.now().date()
  
    for root, directories, files in os.walk(LOCAL_PATH):
        for _file in files:
            filetime = dt.datetime.fromtimestamp(os.path.getctime(os.path.join(root, _file)))
            if filetime.date() == today:
                files_list.append(os.path.join(root, _file))
    return files_list





def json_to_gzip(json_data):
    json_bytes = json_data.encode('utf-8')
    return gzip.compress(json_bytes)


def json_zip_writer(j, target_key):
    f = gzip.open(target_key, 'wb')
    f.write(json.dumps(j).encode('utf-8'))
    f.close()


def cache_property_from_api(response, property, target_key):
    filename = ''
    search_date = dt.datetime.now().date().strftime("%y%m%d")# dt.today().strftime("%y%m%d")

    json_prop = response.json()
    json_prop['propertyDetails']['propertyId'] = property['id']
    json_prop['propertyDetails']['searchCity'] = property['city']
    json_prop['propertyDetails']['searchRadius'] = property['radius']
    json_prop['propertyDetails']['searchDate'] = search_date

    filename = (
        json_prop['propertyDetails']['propertyId'] 
        + '_' +
        json_prop['propertyDetails']['searchDate']
        + '_' +
        json_prop['propertyDetails']['searchCity']
        + '_' +
        json_prop['propertyDetails']['searchRadius']
        + 'km' + '.gz'
    )
    try:
        json_zip_writer(json_prop, target_key + filename)
    except Exception as load_exception:
        raise Exception(f"Error occurred during caching the file {filename} " + str(load_exception)) from load_exception
    return True