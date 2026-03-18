import logging
import json
import os
import sys
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from webdriver import DriverManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def extract_all_companies(driver_manager, output_file):
    """Extract all PSE companies from the directory table.

    PSE directory table has: Company Name, Stock Symbol, Sector, Subsector, Listing Date.
    Links use JavaScript handlers (#company), so we extract data from the table directly
    and paginate through all pages.
    """
    driver = driver_manager.driver
    url = 'https://edge.pse.com.ph/companyDirectory/search.ax'
    driver.get(url)
    logger.info(f"Navigating to {url}")

    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'table'))
    )
    time.sleep(3)

    all_companies = []
    page = 1

    while True:
        logger.info(f"Processing page {page}...")
        rows = driver.find_elements(By.CSS_SELECTOR, 'table tbody tr')

        new_count = 0
        for row in rows:
            cells = row.find_elements(By.CSS_SELECTOR, 'td')
            if len(cells) >= 5:
                name = cells[0].text.strip()
                ticker = cells[1].text.strip()
                sector = cells[2].text.strip()
                subsector = cells[3].text.strip()
                listing_date = cells[4].text.strip()

                if name and ticker:
                    all_companies.append({
                        'Full Company Name': name,
                        'Ticker': ticker,
                        'Sector': sector,
                        'Subsector': subsector,
                        'Listing Date': listing_date,
                        'Incorporated in': 'PHILIPPINES',
                        'Exchange': 'PSE',
                    })
                    new_count += 1

        logger.info(f"  Found {new_count} companies on page {page}. Total: {len(all_companies)}")

        # Save progress
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_companies, f, ensure_ascii=False, indent=4)

        # Try to go to next page
        try:
            # Look for "Next" or ">" button
            next_btns = driver.find_elements(
                By.XPATH, "//a[contains(text(),'Next')] | //a[contains(text(),'>')] | //input[@value='Next']"
            )
            if next_btns:
                next_btns[0].click()
                time.sleep(2)
                page += 1
                continue

            # Try numbered pagination
            page_links = driver.find_elements(By.CSS_SELECTOR, 'a')
            clicked = False
            for pl in page_links:
                txt = pl.text.strip()
                if txt == str(page + 1):
                    pl.click()
                    time.sleep(2)
                    page += 1
                    clicked = True
                    break
            if not clicked:
                # No more pages
                break
        except Exception as e:
            logger.info(f"Pagination ended: {e}")
            break

    logger.info(f"Total companies extracted: {len(all_companies)}")
    return all_companies


def enrich_with_details(driver_manager, companies, output_file, save_every=5):
    """Click on each company in the table to get detailed information.

    This is optional and slower — run it after initial extraction.
    """
    driver = driver_manager.driver
    base_url = 'https://edge.pse.com.ph/companyDirectory/search.ax'

    for i, company in enumerate(companies):
        ticker = company.get('Ticker', '')
        if company.get('Detail_Parsed'):
            continue

        try:
            driver.get(base_url)
            time.sleep(2)

            # Find and click on the company row
            links = driver.find_elements(By.CSS_SELECTOR, 'table tbody tr a')
            clicked = False
            for link in links:
                if link.text.strip() == ticker:
                    link.click()
                    time.sleep(3)
                    clicked = True
                    break

            if not clicked:
                continue

            # Extract detail data from the popup/new page
            # Strategy 1: dt/dd pairs
            terms = driver.find_elements(By.CSS_SELECTOR, 'dt')
            definitions = driver.find_elements(By.CSS_SELECTOR, 'dd')
            for term, defn in zip(terms, definitions):
                t = term.text.strip(' :')
                d = defn.text.strip()
                if t and d and t not in company:
                    company[t] = d

            # Strategy 2: table detail rows
            detail_rows = driver.find_elements(By.CSS_SELECTOR, '.company-detail tr, .modal tr, .popup tr')
            for row in detail_rows:
                cells = row.find_elements(By.CSS_SELECTOR, 'td')
                if len(cells) >= 2:
                    label = cells[0].text.strip()
                    value = cells[1].text.strip()
                    if label and value and label not in company:
                        company[label] = value

            company['Detail_Parsed'] = True
            logger.info(f"({i+1}/{len(companies)}) Detail enriched: {company.get('Full Company Name', '?')}")

        except Exception as e:
            logger.warning(f"Could not get details for {ticker}: {e}")

        if save_every and (i + 1) % save_every == 0:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(companies, f, ensure_ascii=False, indent=4)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(companies, f, ensure_ascii=False, indent=4)
    logger.info(f"All details saved to {output_file}.")


if __name__ == "__main__":
    output_file = 'pse_companies.json'

    driver_manager = DriverManager()
    driver_manager.start_driver()

    # Step 1: Extract all companies from directory table
    companies = extract_all_companies(driver_manager, output_file)

    # Step 2 (optional): Enrich with detail pages
    # Uncomment to get detailed company info (address, phone, etc.)
    # enrich_with_details(driver_manager, companies, output_file)

    driver_manager.quit_driver()
