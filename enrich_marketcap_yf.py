"""
Fetch market cap data from Yahoo Finance for IDX, SET, SGX companies.
Uses the v7/finance/quote endpoint with cookie/crumb authentication.

Batch processing: up to 20 tickers per request.
"""

import json
import sys
import time
import requests

sys.stdout.reconfigure(encoding='utf-8')

EXCHANGE_SUFFIX = {
    'IDX': '.JK',
    'SET': '.BK',
    'SGX': '.SI',
}

BATCH_SIZE = 20
DELAY = 0.5


class YahooFinanceClient:
    """Yahoo Finance client with cookie/crumb auth."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        })
        self.crumb = None

    def authenticate(self):
        """Get cookies and crumb for Yahoo Finance API."""
        # Step 1: hit fc.yahoo.com to get cookies
        try:
            self.session.get('https://fc.yahoo.com', timeout=10, allow_redirects=True)
        except Exception:
            pass  # Expected to fail, but sets cookies

        # Step 2: get crumb
        try:
            r = self.session.get(
                'https://query2.finance.yahoo.com/v1/test/getcrumb',
                timeout=10
            )
            if r.status_code == 200 and r.text:
                self.crumb = r.text.strip()
                print(f"Got crumb: {self.crumb[:10]}...")
                return True
        except Exception as e:
            print(f"Crumb fetch failed: {e}")

        # Fallback: try without crumb (some endpoints work)
        print("Warning: no crumb, will try without authentication")
        return False

    def batch_quote(self, symbols):
        """Fetch quote data for a batch of symbols."""
        symbols_str = ','.join(symbols)

        # Try v7/finance/quote with crumb
        url = 'https://query2.finance.yahoo.com/v7/finance/quote'
        params = {'symbols': symbols_str}
        if self.crumb:
            params['crumb'] = self.crumb

        try:
            r = self.session.get(url, params=params, timeout=15)
            if r.status_code == 200:
                data = r.json()
                return data.get('quoteResponse', {}).get('result', [])
        except Exception:
            pass

        # Fallback: try v6/finance/quote
        try:
            url = 'https://query2.finance.yahoo.com/v6/finance/quote'
            r = self.session.get(url, params=params, timeout=15)
            if r.status_code == 200:
                data = r.json()
                return data.get('quoteResponse', {}).get('result', [])
        except Exception:
            pass

        return []


def enrich_exchange(client, input_file, exchange, ticker_field='Ticker'):
    """Enrich companies in a file with market cap from Yahoo Finance."""
    with open(input_file, 'r', encoding='utf-8') as f:
        companies = json.load(f)

    suffix = EXCHANGE_SUFFIX[exchange]

    # Build ticker list (skip companies that already have Market_Cap)
    to_fetch = []
    for c in companies:
        ticker = c.get(ticker_field, c.get('Code', '')).strip()
        if not ticker:
            continue
        if c.get('Market_Cap') and c['Market_Cap'] > 0:
            continue
        yf_symbol = ticker + suffix
        to_fetch.append((yf_symbol, ticker))

    print(f"\n{exchange}: {len(companies)} total, {len(to_fetch)} need market cap")

    if not to_fetch:
        return companies

    # Build symbol -> ticker mapping
    symbol_to_ticker = {sym: tk for sym, tk in to_fetch}
    ticker_to_company = {}
    for c in companies:
        tk = c.get(ticker_field, c.get('Code', '')).strip()
        if tk:
            ticker_to_company[tk] = c

    # Fetch in batches
    symbols = [sym for sym, _ in to_fetch]
    enriched = 0
    errors = 0

    for i in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(symbols) + BATCH_SIZE - 1) // BATCH_SIZE

        results = client.batch_quote(batch)

        for quote in results:
            symbol = quote.get('symbol', '')
            mcap = quote.get('marketCap')
            if mcap and symbol in symbol_to_ticker:
                ticker = symbol_to_ticker[symbol]
                if ticker in ticker_to_company:
                    company = ticker_to_company[ticker]
                    company['Market_Cap'] = mcap
                    company['Currency'] = quote.get('currency', '')
                    enriched += 1

        found_in_batch = sum(1 for q in results if q.get('marketCap'))
        if batch_num % 5 == 0 or batch_num == total_batches:
            print(f"  Batch {batch_num}/{total_batches}: {found_in_batch}/{len(batch)} with mcap "
                  f"(total enriched: {enriched})")

        time.sleep(DELAY)

    # Save
    with open(input_file, 'w', encoding='utf-8') as f:
        json.dump(companies, f, ensure_ascii=False, indent=2)

    with_mcap = sum(1 for c in companies if c.get('Market_Cap') and c['Market_Cap'] > 0)
    print(f"  {exchange}: enriched {enriched} new, total with market cap: {with_mcap}/{len(companies)}")
    return companies


def main():
    client = YahooFinanceClient()
    auth_ok = client.authenticate()

    if not auth_ok:
        # Try anyway, some endpoints might work without crumb
        print("Proceeding without crumb...")

    # Indonesia (IDX)
    enrich_exchange(client, 'Indonesia/idx_companies.json', 'IDX')

    # Thailand (SET)
    enrich_exchange(client, 'Thailand/set_companies.json', 'SET')

    # Singapore (SGX) — uses ISIN-based tickers stored in YF_Ticker field
    # Need special handling since SGX tickers aren't simple Ticker+suffix
    with open('Singapore/SGX/output_data_enriched.json', 'r', encoding='utf-8') as f:
        sgx_companies = json.load(f)

    to_fetch_sgx = []
    for c in sgx_companies:
        if c.get('Market_Cap') and c['Market_Cap'] > 0:
            continue
        yf_ticker = c.get('YF_Ticker', '')
        if yf_ticker:
            to_fetch_sgx.append(yf_ticker)

    print(f"\nSGX: {len(sgx_companies)} total, {len(to_fetch_sgx)} need market cap")

    ticker_to_sgx = {}
    for c in sgx_companies:
        yf = c.get('YF_Ticker', '')
        if yf:
            ticker_to_sgx[yf] = c

    enriched_sgx = 0
    for i in range(0, len(to_fetch_sgx), BATCH_SIZE):
        batch = to_fetch_sgx[i:i + BATCH_SIZE]
        results = client.batch_quote(batch)
        for quote in results:
            symbol = quote.get('symbol', '')
            mcap = quote.get('marketCap')
            if mcap and symbol in ticker_to_sgx:
                ticker_to_sgx[symbol]['Market_Cap'] = mcap
                ticker_to_sgx[symbol]['Currency'] = quote.get('currency', '')
                enriched_sgx += 1
        time.sleep(DELAY)

    with open('Singapore/SGX/output_data_enriched.json', 'w', encoding='utf-8') as f:
        json.dump(sgx_companies, f, ensure_ascii=False, indent=2)

    with_mcap_sgx = sum(1 for c in sgx_companies if c.get('Market_Cap') and c['Market_Cap'] > 0)
    print(f"  SGX: enriched {enriched_sgx} new, total with market cap: {with_mcap_sgx}/{len(sgx_companies)}")

    print("\nDone!")


if __name__ == '__main__':
    main()
