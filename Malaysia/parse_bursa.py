"""
Bursa Malaysia company parser.
Uses klsescreener.com as data source (Bursa website blocks scraping via Cloudflare).
"""

import logging
import json
import re
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://www.klsescreener.com/v2/screener',
}

MAIN_SECTORS = [
    'Consumer Products & Services', 'Industrial Products & Services',
    'Financial Services', 'Transportation & Logistics',
    'Telecommunications & Media', 'Property', 'Plantation', 'Technology',
    'Construction', 'Energy', 'Healthcare', 'Utilities',
    'Real Estate Investment Trusts', 'Special Purpose Acquisition Company',
    'Closed-End Fund', 'Exchange Traded Fund',
]


def strip_html(s):
    s = re.sub(r'<[^>]+>', ' ', s)
    return ' '.join(s.split()).strip()


def fetch_all_companies(output_file='bursa_companies.json'):
    """Fetch all Bursa Malaysia listed companies from KLSE Screener."""
    url = 'https://www.klsescreener.com/v2/screener/quote_results'
    params = {'board': 'all', 'sector': 'all', 'market_cap_min': 0}

    logger.info(f"Fetching from {url}")
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    html = r.text
    logger.info(f"Got {len(html)} bytes")

    tbody_match = re.search(r'<tbody>(.*?)</tbody>', html, re.DOTALL)
    if not tbody_match:
        logger.error("No table body found")
        return []

    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', tbody_match.group(1), re.DOTALL)
    logger.info(f"Found {len(rows)} table rows")

    companies = []
    for row in rows:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
        if len(cells) < 3:
            continue

        texts = [strip_html(c) for c in cells]
        short_name = texts[0].replace('[s]', '').replace('[ss]', '').strip()
        code = texts[1].strip()

        # Parse sector and board
        sector_board = texts[2]
        parts = [p.strip() for p in sector_board.split(',')]
        board = ''
        sector_raw = ''
        for p in reversed(parts):
            if 'Market' in p:
                board = p
            else:
                sector_raw = (p + ', ' + sector_raw).strip(', ') if sector_raw else p

        # Skip ETFs/funds
        if any(x in sector_raw.lower() for x in ['equity fund', 'commodity fund', 'bond fund']):
            continue

        # Split into sector + industry
        sector = sector_raw
        industry = ''
        for ms in sorted(MAIN_SECTORS, key=len, reverse=True):
            if sector_raw.endswith(' ' + ms):
                sector = ms
                industry = sector_raw[:-len(ms) - 1].strip()
                break
            elif sector_raw == ms:
                sector = ms
                break

        # Fix healthcare
        if 'Health Care' in sector:
            industry = sector.replace(' Health Care', '').replace('Health Care', '').strip()
            sector = 'Healthcare'

        companies.append({
            'Ticker': short_name,
            'Code': code,
            'Full Company Name': short_name,
            'Sector': sector,
            'Industry': industry,
            'Listing Board': board,
            'Incorporated in': 'MALAYSIA',
            'Exchange': 'BURSA',
        })

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(companies, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(companies)} companies to {output_file}")

    # Print sector distribution
    sectors = {}
    for c in companies:
        s = c['Sector']
        sectors[s] = sectors.get(s, 0) + 1
    print(f"\nSector distribution:")
    for s, cnt in sorted(sectors.items(), key=lambda x: -x[1]):
        print(f"  {s}: {cnt}")

    return companies


if __name__ == "__main__":
    fetch_all_companies()
