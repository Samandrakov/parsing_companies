"""
Parse UPCOM exchange company data using VNDirect API and merge into hose_companies.json.

Data sources:
- Stock list: https://api-finfo.vndirect.com.vn/v4/stocks
- Industry classification: https://api-finfo.vndirect.com.vn/v4/industry_classification
- Company profiles: https://api-finfo.vndirect.com.vn/v4/company_profiles

Appends UPCOM companies to existing hose_companies.json (HOSE + HNX + UPCOM).
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

PAGE_SIZE = 2000


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


def fetch_upcom_stocks():
    """Fetch all stocks from UPCOM exchange."""
    all_stocks = []
    page = 1

    while True:
        logger.info(f"Fetching UPCOM stocks page {page}...")
        data = fetch_json(
            f'{BASE_URL}/stocks',
            params={
                'sort': 'code:asc',
                'size': PAGE_SIZE,
                'page': page,
                'q': 'floor:UPCOM~type:STOCK~status:listed',
            }
        )

        if not data or 'data' not in data:
            logger.error(f"No data received for UPCOM page {page}")
            break

        stocks = data['data']
        if not stocks:
            break

        for stock in stocks:
            stock['_exchange'] = 'UPCOM'

        all_stocks.extend(stocks)
        total_elements = data.get('totalElements', 0)
        total_pages = data.get('totalPages', 1)
        logger.info(f"  UPCOM page {page}/{total_pages}: got {len(stocks)} stocks (total: {len(all_stocks)}/{total_elements})")

        if page >= total_pages:
            break
        page += 1
        time.sleep(0.5)

    # Fallback: try without status filter if we got very few results
    if len(all_stocks) < 50:
        logger.warning("Few UPCOM stocks found with status:listed, retrying without status filter...")
        all_stocks = []
        page = 1
        while True:
            data = fetch_json(
                f'{BASE_URL}/stocks',
                params={
                    'sort': 'code:asc',
                    'size': PAGE_SIZE,
                    'page': page,
                    'q': 'floor:UPCOM~type:STOCK',
                }
            )
            if not data or 'data' not in data:
                break
            stocks = data['data']
            if not stocks:
                break
            for stock in stocks:
                stock['_exchange'] = 'UPCOM'
            all_stocks.extend(stocks)
            total_pages = data.get('totalPages', 1)
            logger.info(f"  UPCOM (no status filter) page {page}/{total_pages}: {len(stocks)} stocks")
            if page >= total_pages:
                break
            page += 1
            time.sleep(0.5)

    # Second fallback: fetch ALL stocks and filter for UPCOM by floor field
    if len(all_stocks) < 50:
        logger.warning("Still few results, fetching all stocks and filtering for UPCOM...")
        all_stocks = []
        page = 1
        while True:
            data = fetch_json(
                f'{BASE_URL}/stocks',
                params={
                    'sort': 'code:asc',
                    'size': PAGE_SIZE,
                    'page': page,
                }
            )
            if not data or 'data' not in data:
                break
            stocks = data['data']
            if not stocks:
                break
            upcom_stocks = [s for s in stocks if s.get('floor') == 'UPCOM']
            for stock in upcom_stocks:
                stock['_exchange'] = 'UPCOM'
            all_stocks.extend(upcom_stocks)
            total_pages = data.get('totalPages', 1)
            logger.info(f"  All stocks page {page}/{total_pages}: {len(upcom_stocks)} UPCOM out of {len(stocks)}")
            if page >= total_pages:
                break
            page += 1
            time.sleep(0.5)

    logger.info(f"Total UPCOM stocks collected: {len(all_stocks)}")
    return all_stocks


def build_industry_map(tickers_set):
    """
    Build a mapping from ticker -> (sector, industry) using industry_classification API.
    Only returns mappings for tickers in the given set.
    """
    ticker_to_sector = {}
    ticker_to_industry = {}

    # Fetch level 1 (sectors)
    logger.info("Fetching sector data (level 1)...")
    page = 1
    while True:
        data = fetch_json(
            f'{BASE_URL}/industry_classification',
            params={'q': 'industryLevel:1', 'size': 100, 'page': page}
        )
        if not data or 'data' not in data or not data['data']:
            break
        for item in data['data']:
            sector_name = item.get('englishName', '').strip()
            code_list = item.get('codeList', '')
            if code_list:
                for ticker in code_list.split(','):
                    ticker = ticker.strip()
                    if ticker and ticker in tickers_set:
                        ticker_to_sector[ticker] = sector_name
        total_pages = data.get('totalPages', 1)
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.3)
    logger.info(f"  Mapped {len(ticker_to_sector)} UPCOM tickers to sectors")

    # Fetch level 4 (most specific industry)
    logger.info("Fetching industry data (level 4)...")
    page = 1
    while True:
        data = fetch_json(
            f'{BASE_URL}/industry_classification',
            params={'q': 'industryLevel:4', 'size': 500, 'page': page}
        )
        if not data or 'data' not in data or not data['data']:
            break
        for item in data['data']:
            industry_name = item.get('englishName', '').strip()
            code_list = item.get('codeList', '')
            if code_list:
                for ticker in code_list.split(','):
                    ticker = ticker.strip()
                    if ticker and ticker in tickers_set:
                        ticker_to_industry[ticker] = industry_name
        total_pages = data.get('totalPages', 1)
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.3)
    logger.info(f"  Mapped {len(ticker_to_industry)} UPCOM tickers to industries (level 4)")

    # Fetch level 3 as fallback
    logger.info("Fetching industry data (level 3, fallback)...")
    page = 1
    while True:
        data = fetch_json(
            f'{BASE_URL}/industry_classification',
            params={'q': 'industryLevel:3', 'size': 500, 'page': page}
        )
        if not data or 'data' not in data or not data['data']:
            break
        for item in data['data']:
            industry_name = item.get('englishName', '').strip()
            code_list = item.get('codeList', '')
            if code_list:
                for ticker in code_list.split(','):
                    ticker = ticker.strip()
                    if ticker and ticker in tickers_set and ticker not in ticker_to_industry:
                        ticker_to_industry[ticker] = industry_name
        total_pages = data.get('totalPages', 1)
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.3)
    logger.info(f"  Total UPCOM tickers with industry mapping: {len(ticker_to_industry)}")

    return ticker_to_sector, ticker_to_industry


def fetch_company_profiles_batch(tickers, batch_size=50):
    """Fetch company profiles in batches to get English company names."""
    profiles = {}

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        codes = ','.join(batch)

        logger.info(f"Fetching company profiles batch {i//batch_size + 1}/{(len(tickers) + batch_size - 1)//batch_size} ({len(batch)} tickers)...")

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

    logger.info(f"Total UPCOM company profiles fetched: {len(profiles)}")
    return profiles


def normalize_sector(sector):
    """Normalize sector names to title case for consistency."""
    if not sector:
        return ''
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

    companies.sort(key=lambda c: c['Ticker'])
    return companies


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_file = os.path.join(script_dir, 'hose_companies.json')

    logger.info("=" * 60)
    logger.info("Vietnamese Stock Exchange Parser - UPCOM")
    logger.info("Data source: VNDirect API")
    logger.info("=" * 60)

    # Step 1: Fetch all UPCOM stocks
    logger.info("\n[Step 1/5] Fetching UPCOM stock list...")
    stocks = fetch_upcom_stocks()
    if not stocks:
        logger.error("No UPCOM stocks fetched. Exiting.")
        sys.exit(1)

    tickers = [s['code'] for s in stocks]
    tickers_set = set(tickers)

    # Step 2: Build industry/sector mapping
    logger.info("\n[Step 2/5] Building sector/industry mapping...")
    ticker_to_sector, ticker_to_industry = build_industry_map(tickers_set)

    # Step 3: Fetch company profiles for English names
    logger.info("\n[Step 3/5] Fetching company profiles...")
    profiles = fetch_company_profiles_batch(tickers)

    # Step 4: Build UPCOM company list
    logger.info("\n[Step 4/5] Building UPCOM company list...")
    upcom_companies = build_company_list(stocks, ticker_to_sector, ticker_to_industry, profiles)

    # Step 5: Merge into existing hose_companies.json
    logger.info("\n[Step 5/5] Merging into hose_companies.json...")

    existing_companies = []
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8') as f:
            existing_companies = json.load(f)
        logger.info(f"  Loaded {len(existing_companies)} existing companies")

    # Remove any old UPCOM entries to avoid duplicates on re-run
    existing_tickers_by_exchange = {}
    non_upcom = []
    for c in existing_companies:
        if c.get('Exchange') != 'UPCOM':
            non_upcom.append(c)
    removed_count = len(existing_companies) - len(non_upcom)
    if removed_count > 0:
        logger.info(f"  Removed {removed_count} old UPCOM entries")

    # Also check for ticker collisions (a ticker moving exchanges)
    existing_tickers = {c['Ticker'] for c in non_upcom}
    new_upcom = []
    skipped = 0
    for c in upcom_companies:
        if c['Ticker'] in existing_tickers:
            logger.debug(f"  Skipping {c['Ticker']}: already exists on another exchange")
            skipped += 1
        else:
            new_upcom.append(c)

    if skipped > 0:
        logger.info(f"  Skipped {skipped} UPCOM tickers that already exist on HOSE/HNX")

    # Merge
    merged = non_upcom + new_upcom
    merged.sort(key=lambda c: (c['Exchange'], c['Ticker']))

    # Statistics
    exchange_counts = {}
    with_sector = 0
    with_industry = 0
    with_name = 0
    for c in merged:
        ex = c.get('Exchange', 'Unknown')
        exchange_counts[ex] = exchange_counts.get(ex, 0) + 1
        if c.get('Sector'):
            with_sector += 1
        if c.get('Industry'):
            with_industry += 1
        if c.get('Full Company Name'):
            with_name += 1

    logger.info(f"\nResults (merged file):")
    logger.info(f"  Total companies: {len(merged)}")
    for ex, count in sorted(exchange_counts.items()):
        logger.info(f"  {ex}: {count}")
    logger.info(f"  With sector: {with_sector}")
    logger.info(f"  With industry: {with_industry}")
    logger.info(f"  With English name: {with_name}")

    upcom_with_sector = sum(1 for c in new_upcom if c.get('Sector'))
    upcom_with_industry = sum(1 for c in new_upcom if c.get('Industry'))
    upcom_with_name = sum(1 for c in new_upcom if c.get('Full Company Name'))
    logger.info(f"\nUPCOM breakdown:")
    logger.info(f"  UPCOM companies added: {len(new_upcom)}")
    logger.info(f"  With sector: {upcom_with_sector}")
    logger.info(f"  With industry: {upcom_with_industry}")
    logger.info(f"  With English name: {upcom_with_name}")

    # Save
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(merged, f, ensure_ascii=False, indent=4)
    logger.info(f"\nSaved to {output_file}")

    # Print some samples
    logger.info("\nSample UPCOM entries:")
    for c in new_upcom[:10]:
        logger.info(f"  {c['Ticker']}: {c['Full Company Name']} | {c['Sector']} / {c['Industry']}")


if __name__ == '__main__':
    main()
