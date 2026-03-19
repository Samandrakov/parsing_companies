"""
Improve SGX market cap coverage by searching Yahoo Finance for companies
that don't have YF_Ticker yet.
"""

import json
import sys
import time
import requests

sys.stdout.reconfigure(encoding='utf-8')

INPUT_FILE = 'Singapore/SGX/output_data_enriched.json'
OUTPUT_FILE = 'Singapore/SGX/output_data_enriched.json'

BATCH_SIZE = 20
SEARCH_DELAY = 1.2


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

    def search(self, query):
        """Search Yahoo Finance, prefer SGX (SES exchange)."""
        try:
            r = self.session.get(
                'https://query1.finance.yahoo.com/v1/finance/search',
                params={'q': query, 'quotesCount': 8, 'newsCount': 0},
                timeout=10
            )
            if r.status_code == 200:
                quotes = r.json().get('quotes', [])
                # Prefer SES (Singapore Exchange Securities)
                for q in quotes:
                    if q.get('quoteType') == 'EQUITY' and q.get('exchange') == 'SES':
                        return q
                # Fallback: any .SI ticker
                for q in quotes:
                    if q.get('quoteType') == 'EQUITY' and q.get('symbol', '').endswith('.SI'):
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

    print(f"Loaded {len(companies)} SGX companies")

    already_have = sum(1 for c in companies if c.get('Market_Cap') and c['Market_Cap'] > 0)
    have_ticker = sum(1 for c in companies if c.get('YF_Ticker'))
    print(f"Already have: {have_ticker} with YF_Ticker, {already_have} with Market_Cap")

    client = YFClient()
    client.authenticate()

    # Step 1: Search Yahoo Finance for companies without YF_Ticker
    to_search = [(i, c) for i, c in enumerate(companies) if not c.get('YF_Ticker')]
    print(f"\nSearching Yahoo Finance for {len(to_search)} companies without ticker...")

    found = 0
    for idx, (i, company) in enumerate(to_search):
        name = company.get('Full Company Name', '')
        if not name:
            continue

        # Clean name for search
        search_name = name.replace(' Ltd', '').replace(' Pte', '').replace(' Limited', '')
        result = client.search(search_name)

        if result:
            company['YF_Ticker'] = result['symbol']
            found += 1

        if (idx + 1) % 50 == 0:
            print(f"  Searched {idx + 1}/{len(to_search)}, found {found} new tickers")
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                json.dump(companies, f, ensure_ascii=False, indent=2)

        time.sleep(SEARCH_DELAY)

    print(f"Found {found} new Yahoo tickers")

    # Save after search
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(companies, f, ensure_ascii=False, indent=2)

    # Step 2: Batch fetch market cap for ALL companies with YF_Ticker but no Market_Cap
    need_mcap = [(c['YF_Ticker'], c) for c in companies
                 if c.get('YF_Ticker') and (not c.get('Market_Cap') or c['Market_Cap'] == 0)]
    print(f"\nFetching market cap for {len(need_mcap)} companies...")

    enriched = 0
    for i in range(0, len(need_mcap), BATCH_SIZE):
        batch = need_mcap[i:i + BATCH_SIZE]
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
                enriched += 1

        time.sleep(0.5)

    # Save final
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(companies, f, ensure_ascii=False, indent=2)

    total_mcap = sum(1 for c in companies if c.get('Market_Cap') and c['Market_Cap'] > 0)
    total_price = sum(1 for c in companies if c.get('Stock_Price'))
    print(f"\nSGX Final Summary:")
    print(f"  Total: {len(companies)}")
    print(f"  With YF_Ticker: {sum(1 for c in companies if c.get('YF_Ticker'))}")
    print(f"  With Market_Cap: {total_mcap}")
    print(f"  With Stock_Price: {total_price}")
    print(f"  New enrichments: {enriched}")


if __name__ == '__main__':
    main()
