import logging
import json
import os
import sys
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from webdriver import DriverManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ParsingLinks:
    """Extract company links from Cambodia Securities Exchange."""

    def __init__(self, driver_manager):
        self.driver_manager = driver_manager

    def extract_links(self):
        driver = self.driver_manager.driver
        url = 'https://www.csx.com.kh/data/listed/listPosts.do?ESSION=english'
        driver.get(url)
        logger.info(f"Navigating to {url}")

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'table, .list, .company'))
        )
        time.sleep(3)

        links = []
        seen = set()

        # CSX is very small (~60 companies), all likely on one page
        anchors = driver.find_elements(By.CSS_SELECTOR, 'a')
        for a in anchors:
            href = a.get_attribute('href') or ''
            text = a.text.strip()
            # Look for company detail links
            if href and href not in seen and text:
                if ('viewPost' in href or 'company' in href.lower() or
                    'detail' in href.lower() or 'stock' in href.lower()):
                    seen.add(href)
                    links.append({'url': href, 'name': text})

        # Try table-specific extraction
        if not links:
            table_links = driver.find_elements(By.CSS_SELECTOR, 'table a, .list-item a')
            for a in table_links:
                href = a.get_attribute('href') or ''
                text = a.text.strip()
                if href and href not in seen and text:
                    seen.add(href)
                    links.append({'url': href, 'name': text})

        logger.info(f"Extracted {len(links)} company links from CSX.")
        return links


class ParsingCompaniesBio:
    def __init__(self, driver_manager):
        self.driver_manager = driver_manager

    def parsing_data(self, links_data, output_file, save_every=1):
        driver = self.driver_manager.driver
        all_data = []

        for index, item in enumerate(links_data):
            url = item['url']
            name = item.get('name', '')

            try:
                driver.get(url)
                logger.info(f'Parsing {name} ({index+1}/{len(links_data)})')

                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'body'))
                )
                time.sleep(2)

                data = {
                    'Full Company Name': name,
                    'Source_URL': url,
                    'Incorporated in': 'CAMBODIA'
                }

                # Strategy 1: dt/dd pairs
                terms = driver.find_elements(By.CSS_SELECTOR, 'dt')
                definitions = driver.find_elements(By.CSS_SELECTOR, 'dd')
                for term, defn in zip(terms, definitions):
                    t = term.text.strip(' :')
                    d = defn.text.strip()
                    if t:
                        data[t] = d

                # Strategy 2: table rows
                rows = driver.find_elements(By.CSS_SELECTOR, 'table tr')
                for row in rows:
                    cells = row.find_elements(By.CSS_SELECTOR, 'td, th')
                    if len(cells) >= 2:
                        label = cells[0].text.strip()
                        value = cells[1].text.strip()
                        if label and value and label not in data:
                            data[label] = value

                # Strategy 3: any labeled content
                for el in driver.find_elements(By.CSS_SELECTOR, 'div, p, span'):
                    text = el.text.strip()
                    if ':' in text and len(text) < 200:
                        parts = text.split(':', 1)
                        key = parts[0].strip()
                        val = parts[1].strip()
                        if key and val and key not in data and len(key) < 50:
                            data[key] = val

                # Company name from heading
                headings = driver.find_elements(By.CSS_SELECTOR, 'h1, h2, h3')
                for h in headings:
                    text = h.text.strip()
                    if text and len(text) > 3:
                        data['Full Company Name'] = text
                        break

                all_data.append(data)
                logger.info(f"Extracted {len(data)} fields.")

            except Exception as e:
                logger.error(f"Error parsing {url}: {e}")
                all_data.append({'Full Company Name': name, 'error': str(e)})

            if save_every and (index + 1) % save_every == 0:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(all_data, f, ensure_ascii=False, indent=4)

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=4)
        logger.info(f"All data saved to {output_file}.")
        return all_data


if __name__ == "__main__":
    output_file = 'csx_companies.json'

    driver_manager = DriverManager()
    driver_manager.start_driver()

    link_parser = ParsingLinks(driver_manager)
    links = link_parser.extract_links()
    logger.info(f"Collected {len(links)} company links.")

    if links:
        bio_parser = ParsingCompaniesBio(driver_manager)
        bio_data = bio_parser.parsing_data(links, output_file)

    driver_manager.quit_driver()
