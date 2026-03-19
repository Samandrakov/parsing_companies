"""
Enrich PSE companies with financial data from Yahoo Finance.
Uses strict matching: only accepts PHS exchange results from Yahoo.
"""

import json
import sys
import time
import requests
from difflib import SequenceMatcher

sys.stdout.reconfigure(encoding='utf-8')

INPUT_FILE = 'Philippines/pse_companies.json'
OUTPUT_FILE = 'Philippines/pse_companies.json'

BATCH_SIZE = 20


def name_similarity(a, b):
    """Compare two company names (case-insensitive)."""
    a = a.lower().replace('corporation', '').replace('corp.', '').replace('inc.', '')
    a = a.replace(',', '').replace('.', '').strip()
    b = b.lower().replace('corporation', '').replace('corp.', '').replace('inc.', '')
    b = b.replace(',', '').replace('.', '').strip()
    return SequenceMatcher(None, a, b).ratio()


class YFClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        })
        self.crumb = None

    def authenticate(self):
        try:
            self.session.get('https://fc.yahoo.com', timeout=10, allow_redirects=True)
        except Exception:
            pass
        try:
            r = self.session.get('https://query2.finance.yahoo.com/v1/test/getcrumb', timeout=10)
            if r.status_code == 200 and r.text:
                self.crumb = r.text.strip()
                print(f"Got crumb: {self.crumb[:10]}...")
                return True
        except Exception as e:
            print(f"Crumb failed: {e}")
        return False

    def search_strict(self, query, company_name):
        """Search Yahoo Finance with strict PHS exchange filter + name similarity check."""
        try:
            r = self.session.get(
                'https://query1.finance.yahoo.com/v1/finance/search',
                params={'q': query, 'quotesCount': 10, 'newsCount': 0},
                timeout=10
            )
            if r.status_code != 200:
                return None
            quotes = r.json().get('quotes', [])

            # ONLY accept PHS exchange (Philippine Stock Exchange)
            for q in quotes:
                if q.get('quoteType') == 'EQUITY' and q.get('exchange') == 'PHS':
                    yf_name = q.get('longname', '') or q.get('shortname', '')
                    sim = name_similarity(company_name, yf_name)
                    if sim > 0.3:  # Reasonable threshold
                        return q

            # Also try .PS suffix matches
            for q in quotes:
                if q.get('quoteType') == 'EQUITY' and q.get('symbol', '').endswith('.PS'):
                    yf_name = q.get('longname', '') or q.get('shortname', '')
                    sim = name_similarity(company_name, yf_name)
                    if sim > 0.3:
                        return q

        except Exception:
            pass
        return None

    def batch_quote(self, symbols):
        if not symbols:
            return []
        params = {'symbols': ','.join(symbols)}
        if self.crumb:
            params['crumb'] = self.crumb
        try:
            r = self.session.get(
                'https://query2.finance.yahoo.com/v7/finance/quote',
                params=params, timeout=15
            )
            if r.status_code == 200:
                return r.json().get('quoteResponse', {}).get('result', [])
        except Exception:
            pass
        return []


def main():
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        companies = json.load(f)

    print(f"Loaded {len(companies)} PSE companies")

    # Clear all previous bad YF data
    for c in companies:
        for key in ['YF_Ticker', 'YF_Exchange', 'Market_Cap', 'Stock_Price', 'Volume', 'Currency']:
            c.pop(key, None)

    client = YFClient()
    client.authenticate()

    # Step 1: Search for correct Yahoo tickers
    print(f"Searching Yahoo Finance (strict PHS exchange matching)...")
    found = 0

    for idx, company in enumerate(companies):
        name = company.get('Full Company Name', '')
        ticker = company.get('Ticker', '')

        # Try multiple search strategies
        queries = [
            f"{ticker}.PS",          # Direct ticker
            f"{ticker} PSE",         # Ticker + exchange
            name,                    # Full name
        ]

        for query in queries:
            result = client.search_strict(query, name)
            if result:
                company['YF_Ticker'] = result['symbol']
                found += 1
                break
            time.sleep(0.5)

        if (idx + 1) % 25 == 0:
            print(f"  {idx + 1}/{len(companies)}: found {found} tickers")
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                json.dump(companies, f, ensure_ascii=False, indent=2)

        time.sleep(0.8)

    print(f"\nFound Yahoo tickers for {found}/{len(companies)} companies")
    unique_tickers = len(set(c['YF_Ticker'] for c in companies if c.get('YF_Ticker')))
    print(f"Unique tickers: {unique_tickers}")

    # Save
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(companies, f, ensure_ascii=False, indent=2)

    # Step 2: Batch fetch financial data
    yf_companies = [(c['YF_Ticker'], c) for c in companies if c.get('YF_Ticker')]
    print(f"\nFetching financial data for {len(yf_companies)} companies...")

    enriched = 0
    for i in range(0, len(yf_companies), BATCH_SIZE):
        batch = yf_companies[i:i + BATCH_SIZE]
        symbols = [sym for sym, _ in batch]
        sym_to_co = {sym: c for sym, c in batch}

        results = client.batch_quote(symbols)
        for quote in results:
            sym = quote.get('symbol', '')
            if sym in sym_to_co:
                c = sym_to_co[sym]
                if quote.get('marketCap'):
                    c['Market_Cap'] = quote['marketCap']
                if quote.get('regularMarketPrice'):
                    c['Stock_Price'] = quote['regularMarketPrice']
                if quote.get('regularMarketVolume'):
                    c['Volume'] = quote['regularMarketVolume']
                c['Currency'] = quote.get('currency', '')
                enriched += 1
        time.sleep(0.5)

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(companies, f, ensure_ascii=False, indent=2)

    # Summary
    with_mcap = sum(1 for c in companies if c.get('Market_Cap'))
    with_price = sum(1 for c in companies if c.get('Stock_Price'))
    print(f"\nPSE Final Summary:")
    print(f"  Total: {len(companies)}")
    print(f"  With YF_Ticker: {sum(1 for c in companies if c.get('YF_Ticker'))}")
    print(f"  Unique tickers: {unique_tickers}")
    print(f"  With Market_Cap: {with_mcap}")
    print(f"  With Price: {with_price}")

    top = sorted([c for c in companies if c.get('Market_Cap')],
                 key=lambda c: c['Market_Cap'], reverse=True)
    if top:
        print(f"\nTop 10 by Market Cap:")
        for i, c in enumerate(top[:10], 1):
            mcap_b = c['Market_Cap'] / 1e9
            print(f"  {i:2d}. {c['Ticker']:6s} {c['Full Company Name'][:45]:45s} MCap={mcap_b:,.1f}B")


if __name__ == '__main__':
    main()
