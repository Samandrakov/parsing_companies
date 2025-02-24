import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time

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

    def main(self, start_url, selector, pages_amount):
        self.driver_manager.start_driver()
        driver = self.driver_manager.driver

        all_links = []
        page_size = 3
        for i in range(pages_amount):
            current_url = start_url.format(page_num=i + 1, page_size=page_size)
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
        # self.driver_manager.quit_driver()
        return all_links

class parsing_companies_bio():
    def __init__(self,driver_manager):
        self.driver_manager = driver_manager
        # self.links = links

    def parsing_data(self, links):
        driver = self.driver_manager.driver
        for link in links:
            driver.get(link)
    pass

if __name__ == "__main__":
    start_url = 'https://www.sgx.com/securities/corporate-information?page={page_num}&pagesize={page_size}'  # Замените на URL вашего сайта
    selector = 'a.article-list-result-item-title'
    pages_amount = 3

    driver_manager = DriverManager()
    parser = ParsingLinks(driver_manager)
    links = parser.main(start_url, selector, pages_amount)
    for link in links:
        logger.info(f"Collected link: {link}")
    if links:
        bio_parser = parsing_companies_bio(driver_manager)
        bio_data = bio_parser.parsing_data(links)
