import json
import logging
import psycopg2
from psycopg2 import sql
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DriverManager:
    def __init__(self):
        self.driver = None

    def start_driver(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')  # Запуск в headless режиме (без открытия окна браузера)
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

    def close(self):
        self.conn.close()

class ParsingLinks:
    def __init__(self, driver_manager, db_manager):
        self.driver_manager = driver_manager
        self.db_manager = db_manager

    def parsing_data(self, links):
        driver = self.driver_manager.driver
        for link in links:
            driver.get(link)
            terms = driver.find_elements(By.CSS_SELECTOR, 'dl dt')
            definitions = driver.find_elements(By.CSS_SELECTOR, 'dl dd')
            data = {}
            for term, definition in zip(terms, definitions):
                term_text = term.text.strip(' :')
                definition_text = definition.text.strip()
                data[term_text] = definition_text

            # Преобразование данных в формат, соответствующий таблице
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

            # Вставка данных в базу данных
            self.db_manager.insert_data(formatted_data)
            logger.info(f"Inserted data for {link}.")

    def main(self, start_url, selector, pages_amount):
        self.driver_manager.start_driver()
        driver = self.driver_manager.driver

        all_links = []
        page_size = 3
        for i in range(pages_amount):
            current_url = start_url.format(page_num=i+1, page_size=page_size)
            try:
                driver.get(current_url)
                logger.info(f"Navigating to {current_url}")
                # Явное ожидание появления элементов
                WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                )
                page_links = self.extract_links(selector)
                all_links.extend(page_links)
            except Exception as e:
                logger.error(f"An error occurred: {e}")

        self.parsing_data(all_links)
        self.driver_manager.quit_driver()
        self.db_manager.close()
        return all_links

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

if __name__ == "__main__":
    start_url = 'https://www.example.com/items?page={page_num}&pagesize={page_size}'  # Замените на URL вашего сайта
    selector = 'a.article-list-result-item-title'
    pages_amount = 3

    # Настройки базы данных
    db_config = {
        "dbname": "your_dbname",
        "user": "your_user",
        "password": "your_password",
        "host": "your_host",
        "port": "your_port"
    }

    driver_manager = DriverManager()
    db_manager = DatabaseManager(**db_config)
    parser = ParsingLinks(driver_manager, db_manager)
    links = parser.main(start_url, selector, pages_amount)
    for link in links:
        logger.info(f"Collected link: {link}")
