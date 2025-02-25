import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import psycopg2
from psycopg2 import sql
import time
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DriverManager:
    def __init__(self):
        self.driver = None

    def start_driver(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        logger.info("Chrome driver initialized.")

    def quit_driver(self):
        if self.driver:
            self.driver.quit()
            logger.info("Driver quit.")

class DatabaseManager:
    def __init__(self, dbname, user, password, host, port):
        self.conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.create_table()

    def create_table(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS companies (
                    id SERIAL PRIMARY KEY,
                    full_company_name TEXT,
                    incorporated_in TEXT,
                    incorporated_on TEXT,
                    isin_code TEXT,
                    registered_office TEXT,
                    telephone TEXT,
                    fax TEXT,
                    email TEXT,
                    secretary TEXT,
                    link_to_website TEXT,
                    listing TEXT,
                    listing_board TEXT,
                    other_stock_exchange_listings TEXT,
                    registrars_transfer_agents TEXT,
                    auditors TEXT
                )
            ''')
            self.conn.commit()

    def insert_data(self, data):
        with self.conn.cursor() as cursor:
            cursor.execute('''
                INSERT INTO companies (
                    full_company_name, incorporated_in, incorporated_on, isin_code,
                    registered_office, telephone, fax, email, secretary, link_to_website,
                    listing, listing_board, other_stock_exchange_listings,
                    registrars_transfer_agents, auditors
                ) VALUES (
                    %(full_company_name)s, %(incorporated_in)s, %(incorporated_on)s, %(isin_code)s,
                    %(registered_office)s, %(telephone)s, %(fax)s, %(email)s, %(secretary)s, %(link_to_website)s,
                    %(listing)s, %(listing_board)s, %(other_stock_exchange_listings)s,
                    %(registrars_transfer_agents)s, %(auditors)s
                )
            ''', data)
            self.conn.commit()

class ParsingLinks:
    def __init__(self, driver_manager):
        self.driver_manager = driver_manager

    def extract_links(self, selector):
        driver = self.driver_manager.driver

        elements = driver.find_elements(By.CSS_SELECTOR, selector)
        links = []
        for element in elements:
            link = element.get_attribute('href')
            if link:
                links.append(link)
        logger.info(f"Extracted {len(links)} links.")
        return links

    def main(self, start_url, links_selector, bio_selector, pages_amount, output_file):
        self.driver_manager.start_driver()
        driver = self.driver_manager.driver

        all_links = []
        page_size = 100
        for i in range(pages_amount):
            current_url = start_url.format(page_num=i + 1, page_size=page_size)
            try:
                driver.get(current_url)
                logger.info(f"Navigating to {current_url}")
                # Явное ожидание появления элементов
                WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, links_selector))
                )
                page_links = self.extract_links(links_selector)
                all_links.extend(page_links)
            except Exception as e:
                logger.error(f"An error occurred: {e}")
        # self.driver_manager.quit_driver()
        return all_links

class parsing_companies_bio():
    def __init__(self,driver_manager):
        self.driver_manager = driver_manager

    def parsing_data(self, links):
        driver = self.driver_manager.driver
        all_data = []
        for link in links:
            driver.get(link)
            logger.info(f'Started parsing {link}')
            terms = driver.find_elements(By.CSS_SELECTOR, 'dl dt')
            definitions = driver.find_elements(By.CSS_SELECTOR, 'dl dd')
            data = {}
            for term, definition in zip(terms, definitions):
                term_text = term.text.strip(' :')
                definition_text = definition.text.strip()
                data[term_text] = definition_text
            all_data.append(data)
            logger.info(f"Extracted {len(data)} items.")

            formatted_data = {
                "full_company_name": data.get("Full Company Name"),
                "incorporated_in": data.get("Incorporated in"),
                "incorporated_on": data.get("Incorporated on"),
                "isin_code": data.get("ISIN Code"),
                "registered_office": data.get("Registered Office"),
                "telephone": data.get("Telephone"),
                "fax": data.get("Fax"),
                "email": data.get("Email"),
                "secretary": data.get("Secretary"),
                "link_to_website": data.get("Link to Internet Website"),
                "listing": data.get("LISTING"),
                "listing_board": data.get("LISTING BOARD"),
                "other_stock_exchange_listings": data.get("OTHER STOCK EXCHANGE LISTINGS"),
                "registrars_transfer_agents": data.get("REGISTRARS / TRANSFER AGENTS & ADDRESS"),
                "auditors": data.get("AUDITORS")
            }

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=4)
        logger.info(f"Data saved to {output_file}.")

        return data

    pass


if __name__ == "__main__":
    start_url = 'https://www.sgx.com/securities/corporate-information?page={page_num}&pagesize={page_size}'  # Замените на URL вашего сайта
    links_selector = 'a.article-list-result-item-title'
    bio_selector = 'div.announcement-group:nth-child(n)'
    output_file = 'output_data.json'
    pages_amount = 6

    driver_manager = DriverManager()
    parser = ParsingLinks(driver_manager)
    links = parser.main(start_url, links_selector, bio_selector, pages_amount, output_file)
    logger.info(f"Collected {len(links)} links in total\nStarted parsing items")
    if links:
        bio_parser = parsing_companies_bio(driver_manager)
        bio_data = bio_parser.parsing_data(links)
