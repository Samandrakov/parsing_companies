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
    """Extract stock symbols from the SET listed company page."""

    def __init__(self, driver_manager):
        self.driver_manager = driver_manager

    def extract_links(self):
        driver = self.driver_manager.driver
        base_url = 'https://www.set.or.th/en/market/product/stock/listed-company'
        driver.get(base_url)
        logger.info(f"Navigating to {base_url}")

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'table tbody tr'))
        )
        time.sleep(3)

        # Try to show all companies at once
        try:
            selects = driver.find_elements(By.CSS_SELECTOR, 'select')
            for select_el in selects:
                options = select_el.find_elements(By.CSS_SELECTOR, 'option')
                for opt in options:
                    if opt.get_attribute('value') in ['-1', '0', '999', '9999']:
                        opt.click()
                        time.sleep(3)
                        break
        except Exception:
            logger.info("No 'show all' option found, using pagination")

        symbols = set()
        profile_links = []

        # Extract links containing /quote/ from the table
        anchors = driver.find_elements(By.CSS_SELECTOR, 'table tbody tr a')
        for a in anchors:
            href = a.get_attribute('href') or ''
            if '/quote/' in href:
                symbol = href.split('/quote/')[-1].split('/')[0]
                if symbol and symbol not in symbols:
                    symbols.add(symbol)
                    profile_links.append(
                        f'https://www.set.or.th/en/market/product/stock/quote/{symbol}/company-profile/information'
                    )

        # If no links found, try pagination
        if not profile_links:
            profile_links = self._extract_with_pagination(driver)

        logger.info(f"Extracted {len(profile_links)} company links.")
        return profile_links

    def _extract_with_pagination(self, driver):
        links = []
        symbols = set()
        page = 1
        while True:
            logger.info(f"Processing page {page}...")
            anchors = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/quote/"]')
            new_found = 0
            for a in anchors:
                href = a.get_attribute('href') or ''
                if '/quote/' in href:
                    symbol = href.split('/quote/')[-1].split('/')[0]
                    if symbol and symbol not in symbols:
                        symbols.add(symbol)
                        links.append(
                            f'https://www.set.or.th/en/market/product/stock/quote/{symbol}/company-profile/information'
                        )
                        new_found += 1

            if new_found == 0:
                break

            # Try to click "Next" button
            try:
                next_btn = driver.find_element(By.CSS_SELECTOR, 'button[aria-label="Next"], a.next, li.next a')
                next_btn.click()
                time.sleep(2)
                page += 1
            except Exception:
                break

        return links


class ParsingCompaniesBio:
    def __init__(self, driver_manager):
        self.driver_manager = driver_manager

    def parsing_data(self, links, output_file, save_every=5):
        driver = self.driver_manager.driver
        all_data = []

        for index, link in enumerate(links):
            try:
                driver.get(link)
                symbol = link.split('/quote/')[-1].split('/')[0]
                logger.info(f'Parsing {symbol} ({index+1}/{len(links)})')

                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'body'))
                )
                time.sleep(2)

                data = {'Ticker': symbol, 'Source_URL': link}

                # Strategy 1: dt/dd pairs
                terms = driver.find_elements(By.CSS_SELECTOR, 'dt')
                definitions = driver.find_elements(By.CSS_SELECTOR, 'dd')
                for term, defn in zip(terms, definitions):
                    t = term.text.strip(' :')
                    d = defn.text.strip()
                    if t:
                        data[t] = d

                # Strategy 2: table rows with label-value cells
                if len(data) < 5:
                    rows = driver.find_elements(By.CSS_SELECTOR, 'table tr')
                    for row in rows:
                        cells = row.find_elements(By.CSS_SELECTOR, 'td, th')
                        if len(cells) >= 2:
                            label = cells[0].text.strip()
                            value = cells[1].text.strip()
                            if label and value and label not in data:
                                data[label] = value

                # Strategy 3: labeled divs/spans
                if len(data) < 5:
                    labels = driver.find_elements(By.CSS_SELECTOR, '[class*="label"], [class*="title"], [class*="key"]')
                    values = driver.find_elements(By.CSS_SELECTOR, '[class*="value"], [class*="detail"], [class*="val"]')
                    for lbl, val in zip(labels, values):
                        t = lbl.text.strip()
                        v = val.text.strip()
                        if t and v and t not in data:
                            data[t] = v

                # Try to get company name from heading
                headings = driver.find_elements(By.CSS_SELECTOR, 'h1, h2')
                for h in headings:
                    text = h.text.strip()
                    if text and len(text) > 3:
                        data['Full Company Name'] = text
                        break

                all_data.append(data)
                logger.info(f"Extracted {len(data)} fields for {symbol}.")

            except Exception as e:
                logger.error(f"Error parsing {link}: {e}")
                all_data.append({'Ticker': link.split('/quote/')[-1].split('/')[0], 'error': str(e)})

            if save_every and (index + 1) % save_every == 0:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(all_data, f, ensure_ascii=False, indent=4)
                logger.info(f"Saved after {index + 1} companies.")

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=4)
        logger.info(f"All data saved to {output_file}.")
        return all_data


if __name__ == "__main__":
    output_file = 'set_companies.json'

    driver_manager = DriverManager()
    driver_manager.start_driver()

    link_parser = ParsingLinks(driver_manager)
    links = link_parser.extract_links()
    logger.info(f"Collected {len(links)} company links.")

    if links:
        bio_parser = ParsingCompaniesBio(driver_manager)
        bio_data = bio_parser.parsing_data(links, output_file)

    driver_manager.quit_driver()
