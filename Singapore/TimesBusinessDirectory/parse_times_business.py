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

    def main(self, start_url, links_selector, page_number, output_file):
        self.driver_manager.start_driver()
        driver = self.driver_manager.driver
        all_links = []

        for i in range(page_number):
            current_url = start_url.format(page_number=i + 1)
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

    def parsing_data(self, links, output_file, bio_definitions_selector_dict, save_every=1):
        driver = self.driver_manager.driver
        all_data = []

        for index, link in enumerate(links):
            driver.get(link)
            logger.info(f'Started parsing {link}')
            data = {}

            for term, selector in bio_definitions_selector_dict.items():
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        definition_text = " ".join(element.text.strip() for element in elements)
                        data[term] = definition_text
                    else:
                        data[term] = None
                except Exception as e:
                    logger.warning(f"Could not find elements for {term}: {e}")
                    data[term] = None

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
    start_url = 'https://www.timesbusinessdirectory.com/company-listings?page={page_number}'
    links_selector = 'h3 a'
    # bio_terms_selector = ''
    # bio_definitions_selector = ''
    output_file = 'output_data.json'
    bio_definitions_selector_dict = {
        "Full Company Name": "div.col-md-9 h3",
        "ISIN Code": "p.company-reg",
        "Registered Office": "div.col-md-7 p:nth-of-type(1)",
        "Telephone": "div.valuephone a",
        "Fax": "div.valuefax a",
        "Email": "span a",
        "Link to Internet Website": "a#textwebsite",
        "DESCRIPTION" : "div.row:nth-of-type(4) div.company-description",
        "CATEGORY" : "ul ul li"
    }

    driver_manager = DriverManager()
    parser = ParsingLinks(driver_manager)

    #in total 8862 companies
    page_number = 887
    links = parser.main(start_url, links_selector, page_number, output_file)
    logger.info(f"Collected {len(links)} links in total\nStarted parsing items")

    if links:
        bio_parser = ParsingCompaniesBio(driver_manager)
        bio_data = bio_parser.parsing_data(links, output_file, bio_definitions_selector_dict)
