"""
Fast SGX enrichment using Yahoo Finance Search API only.
Gets ticker symbol, sector, and industry for each company.
Does NOT hit the rate-limited yfinance info endpoint.

Run this first, then use enricher.py for full financial data later.
"""

import json
import logging
import time
import requests
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SEARCH_URL = 'https://query1.finance.yahoo.com/v1/finance/search'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}


def search_yahoo(query: str, exchange: str = 'SES') -> dict:
    """Search Yahoo Finance for a company. Returns {symbol, sector, industry} or None."""
    try:
        params = {'q': query, 'quotesCount': 5, 'newsCount': 0}
        r = requests.get(SEARCH_URL, params=params, headers=HEADERS, timeout=10)
        quotes = r.json().get('quotes', [])

        # Prefer target exchange
        for q in quotes:
            if q.get('exchange') == exchange and q.get('quoteType') == 'EQUITY':
                return {
                    'YF_Ticker': q['symbol'],
                    'Sector_YF': q.get('sectorDisp') or q.get('sector'),
                    'Industry_YF': q.get('industryDisp') or q.get('industry'),
                    'YF_Name': q.get('longname') or q.get('shortname'),
                }
        # Fallback: any equity result with .SI suffix
        for q in quotes:
            if q.get('quoteType') == 'EQUITY' and '.SI' in q.get('symbol', ''):
                return {
                    'YF_Ticker': q['symbol'],
                    'Sector_YF': q.get('sectorDisp') or q.get('sector'),
                    'Industry_YF': q.get('industryDisp') or q.get('industry'),
                    'YF_Name': q.get('longname') or q.get('shortname'),
                }
    except Exception as e:
        logger.debug(f"Search failed for '{query}': {e}")
    return None


def enrich_sgx(input_file: str, output_file: str, delay: float = 0.3):
    """Enrich SGX companies with Yahoo Finance search data."""
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    total = len(data)
    found = 0
    not_found = 0

    for i, company in enumerate(data):
        # Skip already enriched
        if company.get('YF_Ticker'):
            found += 1
            continue

        name = company.get('Full Company Name', '')
        if not name:
            not_found += 1
            continue

        result = search_yahoo(name)
        if result:
            company.update(result)
            company['Search_Status'] = 'found'
            found += 1
            logger.info(f"({i+1}/{total}) {name[:40]:40s} -> {result['YF_Ticker']:10s} | {result.get('Sector_YF', '')}")
        else:
            company['Search_Status'] = 'not_found'
            not_found += 1
            logger.info(f"({i+1}/{total}) {name[:40]:40s} -> NOT FOUND")

        time.sleep(delay)

        # Save progress every 50 companies
        if (i + 1) % 50 == 0:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Progress saved: {found} found, {not_found} not found out of {i+1}")

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info(f"\nDone! Found: {found}/{total} ({found/total*100:.1f}%)")
    logger.info(f"Not found: {not_found}/{total}")
    logger.info(f"Saved to {output_file}")

    # Print sector distribution
    sectors = {}
    for c in data:
        s = c.get('Sector_YF', 'Unknown')
        if s:
            sectors[s] = sectors.get(s, 0) + 1
    print(f"\nSector distribution:")
    for s, count in sorted(sectors.items(), key=lambda x: -x[1]):
        print(f"  {s}: {count}")


if __name__ == "__main__":
    input_file = 'output_data.json'
    output_file = 'output_data_enriched.json'
    enrich_sgx(input_file, output_file)
