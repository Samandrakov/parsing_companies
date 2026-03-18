"""
Lightweight market cap enrichment using Yahoo Finance Search API.
Fetches market cap for companies by ticker suffix per exchange.
"""

import json
import sys
import time
import requests

sys.stdout.reconfigure(encoding='utf-8')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

# Ticker suffix per exchange for Yahoo Finance
EXCHANGE_SUFFIX = {
    'IDX': '.JK',
    'SET': '.BK',
    'BURSA': '.KL',
    'HOSE': '.VN',
    'HNX': '.VN',
    'PSE': '.PS',
    'SGX': '.SI',
    'YSX': None,  # Not on Yahoo
    'CSX': None,
    'LSX': None,
}


def get_market_cap(ticker_with_suffix):
    """Get market cap from Yahoo Finance quote endpoint."""
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker_with_suffix}'
    params = {'range': '1d', 'interval': '1d'}
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=10)
        if r.status_code == 200:
            data = r.json()
            meta = data.get('chart', {}).get('result', [{}])[0].get('meta', {})
            price = meta.get('regularMarketPrice', 0)
            return {'price': price, 'currency': meta.get('currency', '')}
    except Exception:
        pass
    return None


def search_yahoo(query, exchange_hint=None):
    """Search Yahoo Finance for a company."""
    url = 'https://query1.finance.yahoo.com/v1/finance/search'
    params = {'q': query, 'quotesCount': 5, 'newsCount': 0}
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=10)
        if r.status_code == 200:
            quotes = r.json().get('quotes', [])
            for q in quotes:
                if q.get('quoteType') == 'EQUITY':
                    return {
                        'symbol': q.get('symbol', ''),
                        'marketCap': q.get('marketCap'),
                        'sector': q.get('sectorDisp', ''),
                        'industry': q.get('industryDisp', ''),
                    }
    except Exception:
        pass
    return None


def enrich_file(input_file, output_file=None, max_companies=50, delay=1.5):
    """Enrich companies with market cap data (sample for analysis)."""
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if output_file is None:
        output_file = input_file

    enriched = 0
    errors = 0

    for i, company in enumerate(data):
        if enriched >= max_companies:
            break

        # Skip if already has market cap
        if company.get('Market_Cap'):
            continue

        exchange = company.get('Exchange', '')
        suffix = EXCHANGE_SUFFIX.get(exchange)
        if suffix is None:
            continue

        ticker = company.get('Ticker', company.get('Code', ''))
        if not ticker:
            continue

        # Build Yahoo Finance ticker
        yf_ticker = ticker + suffix

        result = search_yahoo(yf_ticker)
        if result and result.get('marketCap'):
            company['Market_Cap'] = result['marketCap']
            company['YF_Symbol'] = result['symbol']
            if result.get('sector') and not company.get('Sector_YF'):
                company['Sector_YF'] = result['sector']
            if result.get('industry') and not company.get('Industry_YF'):
                company['Industry_YF'] = result['industry']
            enriched += 1
            print(f"  [{enriched}/{max_companies}] {ticker}: MCap={result['marketCap']:,.0f}")
        else:
            errors += 1

        time.sleep(delay)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\nEnriched {enriched} companies, {errors} not found")
    return data


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file')
    parser.add_argument('-n', '--max', type=int, default=50)
    parser.add_argument('-d', '--delay', type=float, default=1.5)
    parser.add_argument('-o', '--output')
    args = parser.parse_args()

    enrich_file(args.input_file, args.output, args.max, args.delay)
