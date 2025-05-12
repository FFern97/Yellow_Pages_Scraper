import os
import csv
import json
import time
import logging
from time import sleep
from urllib.parse import urlencode
import concurrent.futures
from selenium import webdriver
from selenium.webdriver.common.by import By
from dataclasses import dataclass, field, fields, asdict

OPTIONS = webdriver.ChromeOptions()
OPTIONS.add_argument("--headless")

API_KEY = ""

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    API_KEY = config["api_key"]


def get_scrapeops_url(url, location="us"):
    payload = {
        "api_key": API_KEY,
        "url": url,
        "country": location,
        "residential": True,
        "wait": 2000
        }
    proxy_url = "https://proxy.scrapeops.io/v1/?" + urlencode(payload)
    print(proxy_url)
    return proxy_url

#========================================= LOGGING ===========================================


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



#======================================== DATA CLASS =========================================

@dataclass
class SearchData:
    name: str = ""
    url: str = ""


    def __post_init__(self):
        self.check_string_fields()
        
    def check_string_fields(self):
        for field in fields(self):
            # Check string fields
            if isinstance(getattr(self, field.name), str):
                # If empty set default text
                if getattr(self, field.name) == "":
                    setattr(self, field.name, f"No {field.name}")
                    continue
                # Strip any trailing spaces, etc.
                value = getattr(self, field.name)
                setattr(self, field.name, value.strip())




#======================================== PIPELINE CLASS/ FUNCION PARA GUARDAR DATOS EXTRAIDOS EN .CSV =========================================

class DataPipeline:
    
    def __init__(self, csv_filename="", storage_queue_limit=50):
        self.names_seen = []
        self.storage_queue = []
        self.storage_queue_limit = storage_queue_limit
        self.csv_filename = csv_filename
        self.csv_file_open = False
    
    def save_to_csv(self):
        self.csv_file_open = True
        data_to_save = []
        data_to_save.extend(self.storage_queue)
        self.storage_queue.clear()
        if not data_to_save:
            return

        keys = [field.name for field in fields(data_to_save[0])]
        file_exists = os.path.isfile(self.csv_filename) and os.path.getsize(self.csv_filename) > 0
        with open(self.csv_filename, mode="a", newline="", encoding="utf-8") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=keys)

            if not file_exists:
                writer.writeheader()

            for item in data_to_save:
                writer.writerow(asdict(item))

        self.csv_file_open = False
                    
    def is_duplicate(self, input_data):
        if input_data.name in self.names_seen:
            logger.warning(f"Duplicate item found: {input_data.name}. Item dropped.")
            return True
        self.names_seen.append(input_data.name)
        return False
            
    def add_data(self, scraped_data):
        if self.is_duplicate(scraped_data) == False:
            self.storage_queue.append(scraped_data)
            if len(self.storage_queue) >= self.storage_queue_limit and self.csv_file_open == False:
                self.save_to_csv()
                       
    def close_pipeline(self):
        if self.csv_file_open:
            time.sleep(3)
        if len(self.storage_queue) > 0:
            self.save_to_csv()



#=======================================                 ===========================================


def scrape_search_results(keyword, location, page_number, data_pipeline=None, retries=3):
    formatted_keyword = keyword.replace(" ", "+")
    base_url = f"https://www.yelp.com/search?find_desc={formatted_keyword}&find_loc={location}&start={page_number*10}"
    url = get_scrapeops_url(base_url)
    
    tries = 0
    success = False
    
    while tries <= retries and not success:
        driver = webdriver.Chrome(options=OPTIONS)
        driver.implicitly_wait(10) # Esperar hasta 10 segundos para que los elementos estén disponibles
        try:
            driver.get(url)
            sleep(5)
            logger.info(f"Fetched {url}")
                
            ## Extract Data            
            div_cards = driver.find_elements(By.CSS_SELECTOR, "div[data-testid='serp-ia-card']")
            logger.info(f"Encontradas {len(div_cards)} tarjetas")

            for div_card in div_cards:
                
                img = div_card.find_element(By.CSS_SELECTOR, "img")
                title = img.get_attribute("alt")

                a_element = div_card.find_element(By.CSS_SELECTOR, "a")
                link = a_element.get_attribute("href").replace("https://proxy.scrapeops.io", "")
                yelp_url = f"https://www.yelp.com{link}"

                search_data = SearchData(name=title, url=yelp_url) 
                
                if data_pipeline is not None:
                    data_pipeline.add_data(search_data)

                logger.info(f"Successfully parsed data from: {url}")

            success = True


        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")

        finally:
            driver.quit()

    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")
    

#========================================             ==========================================



def start_scrape(keyword, pages, location, data_pipeline=None, max_threads=5, retries=3):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        for page in range(pages):
            futures.append(
                executor.submit(
                    scrape_search_results,
                    keyword=keyword,
                    location=location,
                    page_number=page,
                    data_pipeline=data_pipeline,  # ← Ahora sí llegará
                    retries=retries
                )
            )
        
        for future in concurrent.futures.as_completed(futures):
            future.result()  # Espera a que cada hilo termine


#=====================================        ============================================


if __name__ == "__main__":

    MAX_RETRIES = 3
    MAX_THREADS = 4
    PAGES = 1
    LOCATION = "Fairfield County, CT, United States"

    logger.info(f"Crawl starting...")

    ## INPUT ---> List of keywords to scrape
    keyword_list = ["Chiropractors"]
    aggregate_files = []

    ## Job Processes
    for keyword in keyword_list:
        filename = keyword.replace(" ", "-")

        crawl_pipeline = DataPipeline(csv_filename=f"{filename}.csv")
        start_scrape(keyword, PAGES, LOCATION, data_pipeline=crawl_pipeline, max_threads=MAX_THREADS, retries=MAX_RETRIES)
        crawl_pipeline.close_pipeline()
        aggregate_files.append(f"{filename}.csv")
    logger.info(f"Crawl complete.")

    