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

        dropdown = driver.find_element(By.CSS_SELECTOR, "select[aria-label='Search for option']")
        dropdown.click()

        option = driver.find_element(By.CSS_SELECTOR, "select[name='perPageSelect'] option[value='-1']")
        option.click()

        dropdown = driver.find_element(By.CSS_SELECTOR, "select[name='perPageSelect']")
        dropdown.click()

        option = driver.find_element(By.CSS_SELECTOR, "select[name='perPageSelect'] option[value='-1']")
        option.click()

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
                        definition_text = elements[0].text.strip()
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
    start_url = 'https://www.idx.co.id/id/perusahaan-tercatat/profil-perusahaan-tercatat/'
    show_all_selector = '#vgt-select-rpp-1456607969269'
    links_selector = 'tr:nth-of-type(n) a'
    output_file = 'idx_bonds.json'
    bio_definitions_selector_dict = {
        "Full Company Name": "div.bzg_c:nth-of-type(1) tr:nth-of-type(1) span",
        "ISIN Code": "div.bzg_c:nth-of-type(1) tr:nth-of-type(2) span",
        "Registered Office": "div.bzg_c:nth-of-type(1) tr:nth-of-type(3) span",
        "Telephone": "div.bzg_c:nth-of-type(1) tr:nth-of-type(5) span",
        "Fax": "div.bzg_c:nth-of-type(1) tr:nth-of-type(6) span",
        "Email": "div.bzg_c:nth-of-type(1) tr:nth-of-type(4) span",
        "Taxpayer Number": "div.bzg_c:nth-of-type(1) tr:nth-of-type(7) span",
        "Link to Internet Website": "td.td-content a",
        "Main Business Fields":"div.bzg_c:nth-of-type(2) tr:nth-of-type(3) span",
        "Sector": "div.bzg_c:nth-of-type(2) tr:nth-of-type(4) span",
        "Subsector": "div.bzg_c:nth-of-type(2) tr:nth-of-type(5) span",
        "Industry": "div.bzg_c:nth-of-type(2) tr:nth-of-type(6) span",
        "Sub-industry": "div.bzg_c:nth-of-type(2) tr:nth-of-type(7) span",
        "Securities Administration Bureau": "div.bzg_c:nth-of-type(2) tr:nth-of-type(8) span",
        "Recording Date": "div.bzg_c:nth-of-type(2) tr:nth-of-type(1) span",
        "Recording Board": "div.bzg_c:nth-of-type(2) tr:nth-of-type(2) span",
        "DESCRIPTION": "div.row:nth-of-type(4) div.company-description",
        "CATEGORY": ""
    }

    driver_manager = DriverManager()
    parser = ParsingLinks(driver_manager)

    links = parser.main(start_url=start_url, links_selector=links_selector, output_file=output_file , page_size=10, pages_amount=1)
    logger.info(f"Collected {len(links)} links in total\nStarted parsing items")

    if links:
        bio_parser = ParsingCompaniesBio(driver_manager)
        bio_data = bio_parser.parsing_data(links, output_file, bio_definitions_selector_dict)