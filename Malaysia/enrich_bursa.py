"""
Enrich bursa_companies.json with financial data from KLSE Screener.
Fetches: Stock_Price, Volume, Market_Cap_MYR_M, Dividend_Yield, PE_Ratio, Week52_Range

Column mapping (18 <td> cells per row, verified via debug):
  TD[0]  = Ticker (Name)
  TD[1]  = Code
  TD[2]  = Category (Sector, Board)
  TD[3]  = Price + Change (merged due to </td5> typo in HTML; first token = price)
  TD[4]  = Change %
  TD[5]  = 52-week range
  TD[6]  = Volume
  TD[7]  = EPS
  TD[8]  = DPS
  TD[9]  = NTA
  TD[10] = PE
  TD[11] = DY (Dividend Yield %)
  TD[12] = ROE
  TD[13] = PTBV
  TD[14] = MCap (M) -- market cap in MYR millions
  TD[15] = Indicators (flags)
  TD[16] = # Reports
  TD[17] = Info link
"""

import json
import re
import requests
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://www.klsescreener.com/v2/screener',
}

INPUT_FILE = 'Malaysia/bursa_companies.json'
OUTPUT_FILE = 'Malaysia/bursa_companies.json'


def strip_html(s):
    """Remove HTML tags and collapse whitespace."""
    s = re.sub(r'<[^>]+>', ' ', s)
    return ' '.join(s.split()).strip()


def clean_number(s):
    """Clean a number string: remove commas, %, handle '-' as None."""
    s = s.strip().replace(',', '').replace('%', '')
    if s in ('', '-', 'N/A'):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def fetch_klse_financial_data():
    """Fetch all rows from KLSE Screener and extract financial columns."""
    url = 'https://www.klsescreener.com/v2/screener/quote_results'
    params = {'board': 'all', 'sector': 'all', 'market_cap_min': 0}

    logger.info(f"Fetching from {url}")
    r = requests.get(url, headers=HEADERS, params=params, timeout=60)
    r.raise_for_status()
    html = r.text
    logger.info(f"Got {len(html)} bytes")

    tbody_match = re.search(r'<tbody>(.*?)</tbody>', html, re.DOTALL)
    if not tbody_match:
        logger.error("No table body found")
        return {}

    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', tbody_match.group(1), re.DOTALL)
    logger.info(f"Found {len(rows)} table rows")

    # Build a dict: code -> financial data
    financial_data = {}
    for row in rows:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
        if len(cells) < 15:
            continue

        texts = [strip_html(c) for c in cells]

        # Verified column mapping (18 cells):
        # TD[0]=Name, [1]=Code, [2]=Category,
        # TD[3]=Price+Change (merged), [4]=Change%,
        # TD[5]=52w range, [6]=Volume,
        # TD[7]=EPS, [8]=DPS, [9]=NTA,
        # TD[10]=PE, [11]=DY%, [12]=ROE, [13]=PTBV,
        # TD[14]=MCap(M), [15]=Indicators, [16]=#Reports, [17]=link

        code = texts[1].strip()

        # Price: first token in the merged price+change cell
        price_parts = texts[3].split()
        price = clean_number(price_parts[0]) if price_parts else None

        volume = clean_number(texts[6])
        pe_ratio = clean_number(texts[10])
        dividend_yield = clean_number(texts[11])
        market_cap = clean_number(texts[14])
        week52_range = texts[5].strip() if texts[5].strip() not in ('', '-') else None

        financial_data[code] = {
            'Stock_Price': price,
            'Volume': volume,
            'Market_Cap_MYR_M': market_cap,
            'Dividend_Yield': dividend_yield,
            'PE_Ratio': pe_ratio,
            'Week52_Range': week52_range,
        }

    logger.info(f"Extracted financial data for {len(financial_data)} tickers")
    return financial_data


def enrich_companies():
    """Load bursa_companies.json, merge financial data, save back."""
    # Load existing companies
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        companies = json.load(f)
    logger.info(f"Loaded {len(companies)} companies from {INPUT_FILE}")

    # Fetch financial data
    financial_data = fetch_klse_financial_data()

    # Match and merge
    matched = 0
    unmatched_codes = []
    for company in companies:
        code = company.get('Code', '')
        # Try exact match first
        fdata = financial_data.get(code)

        # If no match, try with/without leading zeros
        if fdata is None:
            code_stripped = code.lstrip('0')
            for fcode in financial_data:
                if fcode.lstrip('0') == code_stripped:
                    fdata = financial_data[fcode]
                    break

        if fdata:
            company.update(fdata)
            matched += 1
        else:
            unmatched_codes.append(code)

    logger.info(f"Matched {matched}/{len(companies)} companies")
    if unmatched_codes:
        logger.info(f"Unmatched codes ({len(unmatched_codes)}): {unmatched_codes[:20]}{'...' if len(unmatched_codes) > 20 else ''}")

    # Save
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(companies, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved enriched data to {OUTPUT_FILE}")

    # --- Summary stats ---
    companies_with_mcap = [c for c in companies if c.get('Market_Cap_MYR_M') is not None]
    total_mcap = sum(c['Market_Cap_MYR_M'] for c in companies_with_mcap)
    top10 = sorted(companies_with_mcap, key=lambda c: c['Market_Cap_MYR_M'], reverse=True)[:10]

    print(f"\n{'='*60}")
    print(f"SUMMARY STATS")
    print(f"{'='*60}")
    print(f"Total companies:              {len(companies)}")
    print(f"Companies with market cap:    {len(companies_with_mcap)}")
    print(f"Total market cap:             MYR {total_mcap:,.2f} million")
    print(f"\nTop 10 by Market Cap (MYR million):")
    print(f"{'Rank':<5} {'Ticker':<12} {'Code':<8} {'Market Cap':>14} {'P/E':>8} {'DY%':>8} {'Price':>8}")
    print(f"{'-'*65}")
    for i, c in enumerate(top10, 1):
        pe = f"{c.get('PE_Ratio')}" if c.get('PE_Ratio') is not None else '-'
        dy = f"{c.get('Dividend_Yield')}" if c.get('Dividend_Yield') is not None else '-'
        price = f"{c.get('Stock_Price')}" if c.get('Stock_Price') is not None else '-'
        print(f"{i:<5} {c['Ticker']:<12} {c['Code']:<8} {c['Market_Cap_MYR_M']:>14,.2f} {pe:>8} {dy:>8} {price:>8}")

    return companies


if __name__ == '__main__':
    enrich_companies()
