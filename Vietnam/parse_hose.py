"""
Parse Vietnamese stock exchange (HOSE + HNX) company data using VNDirect API.

Data sources:
- Stock list: https://api-finfo.vndirect.com.vn/v4/stocks
- Industry classification: https://api-finfo.vndirect.com.vn/v4/industry_classification
- Company profiles: https://api-finfo.vndirect.com.vn/v4/company_profiles

Output: hose_companies.json
"""

import json
import logging
import os
import sys
import time
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = 'https://api-finfo.vndirect.com.vn/v4'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json',
}

# Target exchanges
TARGET_EXCHANGES = ['HOSE', 'HNX']
# Maximum page size
PAGE_SIZE = 3000


def fetch_json(url, params=None, retries=3, delay=2):
    """Fetch JSON from URL with retry logic."""
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=60)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt+1}/{retries} failed for {url}: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
    logger.error(f"Failed to fetch {url} after {retries} attempts")
    return None


def fetch_all_stocks():
    """Fetch all stocks from HOSE and HNX exchanges."""
    all_stocks = []

    for exchange in TARGET_EXCHANGES:
        logger.info(f"Fetching stocks from {exchange}...")
        page = 1
        total_fetched = 0

        while True:
            data = fetch_json(
                f'{BASE_URL}/stocks',
                params={
                    'sort': 'code:asc',
                    'size': PAGE_SIZE,
                    'page': page,
                    'q': f'floor:{exchange}~type:STOCK~status:listed',
                }
            )

            if not data or 'data' not in data:
                logger.error(f"No data received for {exchange} page {page}")
                break

            stocks = data['data']
            if not stocks:
                break

            for stock in stocks:
                stock['_exchange'] = exchange

            all_stocks.extend(stocks)
            total_fetched += len(stocks)

            total_elements = data.get('totalElements', 0)
            total_pages = data.get('totalPages', 1)
            logger.info(f"  {exchange} page {page}/{total_pages}: got {len(stocks)} stocks (total so far: {total_fetched}/{total_elements})")

            if page >= total_pages:
                break
            page += 1
            time.sleep(0.5)

        logger.info(f"Total stocks from {exchange}: {total_fetched}")

    # Also try without the status filter to catch more stocks
    if len(all_stocks) < 100:
        logger.warning("Few stocks found with status:listed filter, retrying without it...")
        all_stocks = []
        for exchange in TARGET_EXCHANGES:
            data = fetch_json(
                f'{BASE_URL}/stocks',
                params={
                    'sort': 'code:asc',
                    'size': PAGE_SIZE,
                    'page': 1,
                    'q': f'floor:{exchange}~type:STOCK',
                }
            )
            if data and 'data' in data:
                for stock in data['data']:
                    stock['_exchange'] = exchange
                all_stocks.extend(data['data'])
                logger.info(f"  {exchange} (no status filter): {len(data['data'])} stocks")

    logger.info(f"Total stocks collected: {len(all_stocks)}")
    return all_stocks


def build_industry_map():
    """
    Build a mapping from ticker -> (sector, industry) using industry_classification API.
    Level 1 = Sector (e.g., "CONSUMER GOODS")
    Level 4 = Industry (e.g., "Food Production")
    Level 3 = Industry group (fallback if level 4 unavailable)
    """
    ticker_to_sector = {}
    ticker_to_industry = {}

    # Fetch level 1 (sectors)
    logger.info("Fetching sector data (level 1)...")
    data = fetch_json(
        f'{BASE_URL}/industry_classification',
        params={'q': 'industryLevel:1', 'size': 100}
    )
    if data and 'data' in data:
        for item in data['data']:
            sector_name = item.get('englishName', '').strip()
            code_list = item.get('codeList', '')
            if code_list:
                for ticker in code_list.split(','):
                    ticker = ticker.strip()
                    if ticker:
                        ticker_to_sector[ticker] = sector_name
        logger.info(f"  Mapped {len(ticker_to_sector)} tickers to sectors")

    # Fetch level 4 (most specific industry)
    logger.info("Fetching industry data (level 4)...")
    data = fetch_json(
        f'{BASE_URL}/industry_classification',
        params={'q': 'industryLevel:4', 'size': 500}
    )
    if data and 'data' in data:
        for item in data['data']:
            industry_name = item.get('englishName', '').strip()
            code_list = item.get('codeList', '')
            if code_list:
                for ticker in code_list.split(','):
                    ticker = ticker.strip()
                    if ticker:
                        ticker_to_industry[ticker] = industry_name
        logger.info(f"  Mapped {len(ticker_to_industry)} tickers to industries (level 4)")

    # Fetch level 3 as fallback
    logger.info("Fetching industry data (level 3, fallback)...")
    data = fetch_json(
        f'{BASE_URL}/industry_classification',
        params={'q': 'industryLevel:3', 'size': 500}
    )
    if data and 'data' in data:
        for item in data['data']:
            industry_name = item.get('englishName', '').strip()
            code_list = item.get('codeList', '')
            if code_list:
                for ticker in code_list.split(','):
                    ticker = ticker.strip()
                    if ticker and ticker not in ticker_to_industry:
                        ticker_to_industry[ticker] = industry_name
        logger.info(f"  Total tickers with industry mapping: {len(ticker_to_industry)}")

    return ticker_to_sector, ticker_to_industry


def fetch_company_profiles_batch(tickers, batch_size=50):
    """
    Fetch company profiles in batches to get English company names.
    The API supports filtering by code with comma-separated values.
    """
    profiles = {}

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        codes = ','.join(batch)

        logger.info(f"Fetching company profiles batch {i//batch_size + 1} ({len(batch)} tickers)...")

        data = fetch_json(
            f'{BASE_URL}/company_profiles',
            params={
                'q': f'code:{codes}',
                'size': batch_size,
            }
        )

        if data and 'data' in data:
            for profile in data['data']:
                code = profile.get('code', '')
                if code:
                    profiles[code] = profile
            logger.info(f"  Got {len(data['data'])} profiles")
        else:
            logger.warning(f"  No profiles returned for batch starting at index {i}")

        time.sleep(0.3)

    logger.info(f"Total company profiles fetched: {len(profiles)}")
    return profiles


def normalize_sector(sector):
    """Normalize sector names to title case for consistency."""
    if not sector:
        return ''
    # Convert from ALL CAPS to Title Case
    return sector.strip().title()


def normalize_industry(industry):
    """Normalize industry names."""
    if not industry:
        return ''
    return industry.strip()


def build_company_list(stocks, ticker_to_sector, ticker_to_industry, profiles):
    """Build the final company list in the target output format."""
    companies = []

    for stock in stocks:
        ticker = stock.get('code', '')
        exchange = stock.get('_exchange', stock.get('floor', ''))

        # Get English company name - prefer from profiles, fallback to stock list
        en_name = ''
        if ticker in profiles:
            en_name = profiles[ticker].get('enName', '')
        if not en_name:
            en_name = stock.get('companyNameEng', '')
        if not en_name:
            en_name = stock.get('shortNameEng', '')
        if not en_name:
            # Use Vietnamese name as last resort
            en_name = stock.get('companyName', '')

        sector = normalize_sector(ticker_to_sector.get(ticker, ''))
        industry = normalize_industry(ticker_to_industry.get(ticker, ''))

        company = {
            'Ticker': ticker,
            'Full Company Name': en_name,
            'Sector': sector,
            'Industry': industry,
            'Incorporated in': 'VIETNAM',
            'Exchange': exchange,
        }

        companies.append(company)

    # Sort by exchange then ticker
    companies.sort(key=lambda c: (c['Exchange'], c['Ticker']))

    return companies


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_file = os.path.join(script_dir, 'hose_companies.json')

    logger.info("=" * 60)
    logger.info("Vietnamese Stock Exchange Parser (HOSE + HNX)")
    logger.info("Data source: VNDirect API")
    logger.info("=" * 60)

    # Step 1: Fetch all stocks
    logger.info("\n[Step 1/4] Fetching stock list...")
    stocks = fetch_all_stocks()
    if not stocks:
        logger.error("No stocks fetched. Exiting.")
        sys.exit(1)

    # Step 2: Build industry/sector mapping
    logger.info("\n[Step 2/4] Building sector/industry mapping...")
    ticker_to_sector, ticker_to_industry = build_industry_map()

    # Step 3: Fetch company profiles for English names
    logger.info("\n[Step 3/4] Fetching company profiles...")
    tickers = [s['code'] for s in stocks]
    profiles = fetch_company_profiles_batch(tickers)

    # Step 4: Build and save final output
    logger.info("\n[Step 4/4] Building company list...")
    companies = build_company_list(stocks, ticker_to_sector, ticker_to_industry, profiles)

    # Statistics
    hose_count = sum(1 for c in companies if c['Exchange'] == 'HOSE')
    hnx_count = sum(1 for c in companies if c['Exchange'] == 'HNX')
    with_sector = sum(1 for c in companies if c['Sector'])
    with_industry = sum(1 for c in companies if c['Industry'])
    with_name = sum(1 for c in companies if c['Full Company Name'])

    logger.info(f"\nResults:")
    logger.info(f"  Total companies: {len(companies)}")
    logger.info(f"  HOSE: {hose_count}")
    logger.info(f"  HNX: {hnx_count}")
    logger.info(f"  With sector: {with_sector}")
    logger.info(f"  With industry: {with_industry}")
    logger.info(f"  With English name: {with_name}")

    # Save
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(companies, f, ensure_ascii=False, indent=4)
    logger.info(f"\nSaved to {output_file}")

    # Print a few samples
    logger.info("\nSample entries:")
    for c in companies[:5]:
        logger.info(f"  {c['Ticker']} ({c['Exchange']}): {c['Full Company Name']} | {c['Sector']} / {c['Industry']}")


if __name__ == '__main__':
    main()
