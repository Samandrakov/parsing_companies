"""
Enrich Vietnamese stock data with financial metrics (price, volume, market cap).

Data sources:
1. CafeF stock handler (center=1/2/9) - stock prices and volume for HOSE/HNX/UPCOM
2. VNDirect ratios API - market cap (51003), revenue (51006), net income (51007)

CafeF field mapping:
  a = ticker, b = close/match price (x1000 VND), e = ref/basic price (x1000 VND)
  totalvolume = total trading volume, v = high price, w = low price

Also adds Market_Tier field: "Main" for HOSE, "Secondary" for HNX, "OTC" for UPCOM.

Output: hose_companies.json (overwrites in place)
"""

import json
import logging
import os
import time
import requests
from collections import Counter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(SCRIPT_DIR, 'hose_companies.json')

BASE_URL = 'https://api-finfo.vndirect.com.vn/v4'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json',
}

MARKET_TIER_MAP = {
    'HOSE': 'Main',
    'HNX': 'Secondary',
    'UPCOM': 'OTC',
}

# CafeF center codes per exchange
CAFEF_CENTERS = {
    'HOSE': '1',
    'HNX': '2',
    'UPCOM': '9',
}


def fetch_json(url, params=None, headers=None, retries=3, delay=2):
    """Fetch JSON from URL with retry logic."""
    hdrs = headers or HEADERS
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=hdrs, params=params, timeout=60)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"  Attempt {attempt+1}/{retries} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
    logger.error(f"  Failed to fetch after {retries} attempts")
    return None


# ---------------------------------------------------------------------------
# Source 1: CafeF (prices + volume via center= parameter)
# ---------------------------------------------------------------------------
def fetch_cafef_prices():
    """
    Fetch price data from CafeF for all three exchanges.
    Uses center=1 (HOSE), center=2 (HNX), center=9 (UPCOM).

    CafeF field meanings:
      a = ticker
      b = close/match price (in x1000 VND, e.g. 7.19 = 7,190 VND)
      e = ref/basic price (x1000 VND)
      v = high price (x1000 VND), w = low price (x1000 VND)
      totalvolume = total matched volume (in shares)
      n = total volume (alternative key)

    Returns dict: ticker -> {price, volume, exchange}
    """
    logger.info("=" * 60)
    logger.info("[Source 1] CafeF stock prices (all exchanges)...")
    result = {}

    cafef_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://banggia.cafef.vn/',
    }

    for exchange, center_code in CAFEF_CENTERS.items():
        url = f'https://banggia.cafef.vn/stockhandler.ashx?center={center_code}'
        try:
            r = requests.get(url, headers=cafef_headers, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logger.warning(f"  CafeF {exchange} (center={center_code}) failed: {e}")
            continue

        if not isinstance(data, list):
            logger.warning(f"  CafeF {exchange}: unexpected type {type(data).__name__}")
            continue

        count = 0
        for rec in data:
            ticker = (rec.get('a') or '').strip()
            if not ticker:
                continue

            # Close/match price in x1000 VND
            price_raw = rec.get('b') or rec.get('e')
            volume_raw = rec.get('totalvolume') or rec.get('n')

            price = None
            volume = None

            try:
                if price_raw is not None:
                    price = float(str(price_raw).replace(',', ''))
                    # CafeF returns prices in x1000 VND (e.g. 23.75 = 23,750 VND)
                    price = price * 1000
            except (ValueError, TypeError):
                price = None

            try:
                if volume_raw is not None:
                    volume = int(float(str(volume_raw).replace(',', '')))
            except (ValueError, TypeError):
                volume = None

            if price and price > 0:
                result[ticker] = {
                    'price': price,
                    'volume': volume if volume and volume > 0 else None,
                    'exchange': exchange,
                }
                count += 1

        logger.info(f"  CafeF {exchange} (center={center_code}): {count} tickers with price")
        time.sleep(0.5)

    logger.info(f"  CafeF total: {len(result)} tickers with price data")
    return result


# ---------------------------------------------------------------------------
# Source 2: VNDirect ratios (market cap, revenue, net income)
# ---------------------------------------------------------------------------
def fetch_vndirect_ratios(tickers, batch_size=50):
    """
    Fetch financial ratios: 51003=market cap, 51006=revenue, 51007=net income.
    Returns dict: ticker -> {market_cap, revenue, net_income}
    """
    logger.info("=" * 60)
    logger.info("[Source 2] VNDirect ratios (market cap, revenue, net income)...")
    result = {}
    total_batches = (len(tickers) + batch_size - 1) // batch_size

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        codes_str = ','.join(batch)
        batch_num = i // batch_size + 1

        data = fetch_json(
            f'{BASE_URL}/ratios/latest',
            params={
                'filter': 'itemCode:51003,51006,51007',
                'where': f'code:{codes_str}',
                'order': 'code',
            }
        )

        if not data:
            time.sleep(1)
            continue

        for rec in data.get('data', []):
            code = rec.get('code', '')
            item_code = str(rec.get('itemCode', ''))
            value = rec.get('value')
            if not code:
                continue
            if code not in result:
                result[code] = {}

            if item_code == '51003':
                result[code]['market_cap'] = value
            elif item_code == '51006':
                result[code]['revenue'] = value
            elif item_code == '51007':
                result[code]['net_income'] = value

        if batch_num % 10 == 0 or batch_num == total_batches:
            logger.info(f"  Batch {batch_num}/{total_batches} done ({len(result)} tickers)")
        time.sleep(0.3)

    logger.info(f"  VNDirect ratios total: {len(result)} tickers")
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    logger.info("Loading company data from %s", DATA_FILE)
    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        companies = json.load(f)
    logger.info(f"Loaded {len(companies)} companies")

    # Clean any previous enrichment fields
    for c in companies:
        for key in ['Stock_Price', 'Volume', 'Market_Cap', 'Market_Tier']:
            c.pop(key, None)

    tickers = [c['Ticker'] for c in companies]

    # Fetch data from sources
    cafef_prices = fetch_cafef_prices()
    vnd_ratios = fetch_vndirect_ratios(tickers, batch_size=50)

    # Merge into company records
    logger.info("=" * 60)
    logger.info("Merging data into company records...")

    price_count = 0
    volume_count = 0
    mcap_count = 0

    for company in companies:
        ticker = company['Ticker']
        exchange = company.get('Exchange', '')

        # Market Tier
        company['Market_Tier'] = MARKET_TIER_MAP.get(exchange, '')

        # Stock Price + Volume from CafeF
        price = None
        volume = None
        if ticker in cafef_prices:
            price = cafef_prices[ticker].get('price')
            volume = cafef_prices[ticker].get('volume')

        if price is not None:
            try:
                company['Stock_Price'] = round(float(price), 2)
                price_count += 1
            except (ValueError, TypeError):
                company['Stock_Price'] = None
        else:
            company['Stock_Price'] = None

        if volume is not None:
            try:
                company['Volume'] = int(float(volume))
                volume_count += 1
            except (ValueError, TypeError):
                company['Volume'] = None
        else:
            company['Volume'] = None

        # Market Cap from VNDirect ratios
        mcap = None
        if ticker in vnd_ratios and vnd_ratios[ticker].get('market_cap'):
            mcap = vnd_ratios[ticker]['market_cap']

        if mcap is not None:
            try:
                company['Market_Cap'] = round(float(mcap), 2)
                mcap_count += 1
            except (ValueError, TypeError):
                company['Market_Cap'] = None
        else:
            company['Market_Cap'] = None

    # Save
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(companies, f, ensure_ascii=False, indent=4)
    logger.info(f"Saved enriched data to {DATA_FILE}")

    # ---- Summary ----
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    exchange_counts = Counter(c.get('Exchange', '?') for c in companies)
    print(f"\nTotal companies: {len(companies)}")
    print("\nBy exchange (Market Tier):")
    for exch in ['HOSE', 'HNX', 'UPCOM']:
        cnt = exchange_counts.get(exch, 0)
        tier = MARKET_TIER_MAP.get(exch, '?')
        print(f"  {exch:6s} ({tier:9s}): {cnt:4d}")

    print(f"\nFinancial data coverage:")
    print(f"  Stock_Price: {price_count:4d}/{len(companies)} ({100*price_count/len(companies):.1f}%)")
    print(f"  Volume:      {volume_count:4d}/{len(companies)} ({100*volume_count/len(companies):.1f}%)")
    print(f"  Market_Cap:  {mcap_count:4d}/{len(companies)} ({100*mcap_count/len(companies):.1f}%)")

    print(f"\nPrice coverage by exchange:")
    for exch in ['HOSE', 'HNX', 'UPCOM']:
        exch_cos = [c for c in companies if c.get('Exchange') == exch]
        wp = sum(1 for c in exch_cos if c.get('Stock_Price'))
        pct = 100 * wp / len(exch_cos) if exch_cos else 0
        print(f"  {exch:6s}: {wp:4d}/{len(exch_cos):4d} ({pct:.1f}%)")

    # Top 10 by market cap
    with_mcap = [c for c in companies if c.get('Market_Cap')]
    if with_mcap:
        with_mcap.sort(key=lambda c: c['Market_Cap'], reverse=True)
        print(f"\nTop 10 by Market Cap:")
        print(f"  {'#':>3s}  {'Ticker':6s}  {'Company':45s}  {'Exch':5s}  "
              f"{'MCap (B VND)':>14s}  {'Price (VND)':>12s}")
        print(f"  {'---':>3s}  {'------':6s}  {'-'*45}  {'-----':5s}  "
              f"{'-'*14:>14s}  {'-'*12:>12s}")
        for i, c in enumerate(with_mcap[:10], 1):
            mcap_b = c['Market_Cap'] / 1e9 if c['Market_Cap'] > 1e6 else c['Market_Cap']
            ps = f"{c['Stock_Price']:,.0f}" if c.get('Stock_Price') else "N/A"
            print(f"  {i:3d}  {c['Ticker']:6s}  {c.get('Full Company Name','')[:45]:45s}  "
                  f"{c['Exchange']:5s}  {mcap_b:>14,.2f}  {ps:>12s}")

    print(f"\nData sources:")
    print(f"  CafeF prices:    {len(cafef_prices):4d} tickers")
    print(f"  VNDirect ratios: {len(vnd_ratios):4d} tickers")
    print()


if __name__ == '__main__':
    main()
