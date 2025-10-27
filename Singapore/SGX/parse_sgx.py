import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver import DriverManager
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ParsingLinks:
    def __init__(self, driver_manager):
        self.driver_manager = driver_manager

    def extract_links(self, selector):
        driver = self.driver_manager.driver
        elements = driver.find_elements(By.CSS_SELECTOR, selector)
        links = [element.get_attribute('href') for element in elements if element.get_attribute('href')]
        logger.info(f"Extracted {len(links)} links.")
        return links

    def main(self, start_url, links_selector, pages_amount, output_file, page_size):
        self.driver_manager.start_driver()
        driver = self.driver_manager.driver
        all_links = []

        for i in range(pages_amount):
            current_url = start_url.format(page_num=i + 1, page_size=page_size)
            try:
                driver.get(current_url)
                logger.info(f"Navigating to {current_url}")
                WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, links_selector))
                )
                page_links = self.extract_links(links_selector)
                all_links.extend(page_links)
            except Exception as e:
                logger.error(f"An error occurred: {e}")

        return all_links

class ParsingCompaniesBio:
    def __init__(self, driver_manager):
        self.driver_manager = driver_manager

    def parsing_data(self, links, output_file, save_every=1):
        driver = self.driver_manager.driver
        all_data = []

        for index, link in enumerate(links):
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

            if save_every and (index + 1) % save_every == 0:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(all_data, f, ensure_ascii=False, indent=4)
                logger.info(f"Data saved to {output_file} after {index + 1} iterations.")

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=4)
        logger.info(f"Data saved to {output_file}.")

        return all_data

class DatabaseManager:
    pass

if __name__ == "__main__":
    start_url = 'https://www.sgx.com/securities/corporate-information?page={page_num}&pagesize={page_size}'
    links_selector = 'a.article-list-result-item-title'
    output_file = 'output_data.json'

    driver_manager = DriverManager()
    parser = ParsingLinks(driver_manager)

    pages_amount = 7
    page_size = 100
    links = parser.main(start_url, links_selector, pages_amount, output_file, page_size)
    logger.info(f"Collected {len(links)} links in total\nStarted parsing items")

    if links:
        bio_parser = ParsingCompaniesBio(driver_manager)
        bio_data = bio_parser.parsing_data(links, output_file)
