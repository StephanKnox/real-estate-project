import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import os


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
   for root, directories, files in os.walk(LOCAL_PATH):
       for _file in files:
           files_list.append(os.path.join(root, _file))
   return files_list