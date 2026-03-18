"""
Universal financial data enricher for all ASEAN exchanges.
Uses Yahoo Finance (yfinance) to add Market Cap, Revenue, P/E, ROE, etc.

Yahoo Finance ticker suffixes by exchange:
    IDX (Indonesia):    .JK    (e.g., BBCA.JK)
    SGX (Singapore):    .SI    (e.g., D05.SI)
    SET (Thailand):     .BK    (e.g., PTT.BK)
    Bursa (Malaysia):   .KL    (e.g., 1155.KL)
    HOSE (Vietnam):     .VN    (e.g., VNM.VN)  — limited coverage
    PSE (Philippines):  .PS    (e.g., SM.PS)
    CSX (Cambodia):     N/A    — not on Yahoo Finance
    LSX (Laos):         N/A    — not on Yahoo Finance
    YSX (Myanmar):      N/A    — not on Yahoo Finance

SGX note: SGX data contains ISIN codes, not ticker symbols.
    The enricher will try to look up by ISIN first, then by company name search.
"""

import json
import logging
import time
import re
import requests
import yfinance as yf
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EXCHANGE_SUFFIXES = {
    'IDX': '.JK',
    'SGX': '.SI',
    'SET': '.BK',
    'BURSA': '.KL',
    'HOSE': '.VN',
    'HNX': '.VN',
    'PSE': '.PS',
    'CSX': None,
    'LSX': None,
    'YSX': None,
}

# Well-known SGX tickers for major companies (ISIN -> Yahoo ticker)
SGX_KNOWN_TICKERS = {
    # Banks
    'SG1T75930913': 'D05.SI',   # DBS Group
    'SG1S04926220': 'O39.SI',   # OCBC
    'SG1M31001969': 'U11.SI',   # UOB
    # Telecoms
    'SG1T75931496': 'Z74.SI',   # Singtel
    # REITs & Property
    'SG1T74931364': 'C09.SI',   # City Developments
    'SG2D18969584': 'A17U.SI',  # CapitaLand Ascendas REIT
    'SG1S83002349': 'C38U.SI',  # CapitaLand Integrated Commercial Trust
    # Others
    'SG0F12853582': 'BN4.SI',   # Keppel
    'SG1L01001701': 'S68.SI',   # SGX
    'SG1K66921048': 'F34.SI',   # Wilmar
    'SG1P66918738': 'Y92.SI',   # Thai Beverage
    'SG1N31909426': 'S58.SI',   # SATS
    'SG1J26887955': 'C52.SI',   # ComfortDelGro
    'SG1V61937297': 'V03.SI',   # Venture Corporation
    'SG1J27887951': 'U96.SI',   # Sembcorp Industries
    'SG1P22919927': 'G13.SI',   # Genting Singapore
    'SG2C32962814': 'ME8U.SI',  # Mapletree Industrial Trust
    'SG1T56930848': 'J36.SI',   # Jardine Matheson (delisted but historical)
    'SG1CI9000006': 'BS6.SI',   # YZJ Shipbuilding
    'SG1U76934819': 'S63.SI',   # ST Engineering
}


class UniversalEnricher:
    def __init__(self, exchange: str, input_file: str, output_file: str,
                 ticker_field: str = 'Ticker', delay: float = 2.0):
        self.exchange = exchange.upper()
        self.suffix = EXCHANGE_SUFFIXES.get(self.exchange)
        self.input_file = input_file
        self.output_file = output_file
        self.ticker_field = ticker_field
        self.delay = delay
        self._search_cache = {}

        if not self.suffix:
            logger.warning(f"Exchange {self.exchange} is not supported by Yahoo Finance. "
                           f"Financial enrichment will be skipped.")

    def load_data(self) -> List[Dict]:
        with open(self.input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"Loaded {len(data)} companies from {self.input_file}")
        return data

    def get_yahoo_ticker(self, company: Dict) -> Optional[str]:
        """Build Yahoo Finance ticker from company data."""
        # For SGX: try ISIN lookup first, then name search
        if self.exchange == 'SGX':
            return self._get_sgx_ticker(company)

        ticker = company.get(self.ticker_field, '')
        if not ticker or not isinstance(ticker, str):
            return None

        ticker = ticker.strip().upper()

        # Remove existing suffix if present
        for sfx in EXCHANGE_SUFFIXES.values():
            if sfx and ticker.endswith(sfx):
                ticker = ticker[:-len(sfx)]
                break

        return f"{ticker}{self.suffix}" if self.suffix else None

    def _get_sgx_ticker(self, company: Dict) -> Optional[str]:
        """Resolve SGX ticker from ISIN code or company name."""
        isin = company.get('ISIN Code', '').strip()
        name = company.get('Full Company Name', '').strip()

        # 1. Check known tickers mapping
        if isin in SGX_KNOWN_TICKERS:
            return SGX_KNOWN_TICKERS[isin]

        # 2. Search Yahoo Finance API by company name
        if name:
            result = self._yahoo_search(name, target_exchange='SES')
            if result:
                # Also store sector/industry from search if found
                if result.get('sector'):
                    company['Sector_YF'] = result['sector']
                if result.get('industry'):
                    company['Industry_YF'] = result['industry']
                return result['symbol']

        return None

    def _yahoo_search(self, query: str, target_exchange: str = None) -> Optional[Dict]:
        """Search Yahoo Finance API for a ticker.

        Returns dict with 'symbol', 'sector', 'industry' or None.
        """
        cache_key = f"{query}:{target_exchange}"
        if cache_key in self._search_cache:
            return self._search_cache[cache_key]

        try:
            url = 'https://query1.finance.yahoo.com/v1/finance/search'
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            params = {'q': query, 'quotesCount': 5, 'newsCount': 0}
            r = requests.get(url, params=params, headers=headers, timeout=10)
            data = r.json()
            quotes = data.get('quotes', [])

            # Prefer target exchange
            if target_exchange:
                for q in quotes:
                    if q.get('exchange') == target_exchange and q.get('quoteType') == 'EQUITY':
                        result = {
                            'symbol': q['symbol'],
                            'sector': q.get('sectorDisp', q.get('sector')),
                            'industry': q.get('industryDisp', q.get('industry')),
                        }
                        self._search_cache[cache_key] = result
                        return result

            # Fallback to first equity result
            for q in quotes:
                if q.get('quoteType') == 'EQUITY':
                    result = {
                        'symbol': q['symbol'],
                        'sector': q.get('sectorDisp', q.get('sector')),
                        'industry': q.get('industryDisp', q.get('industry')),
                    }
                    self._search_cache[cache_key] = result
                    return result

        except Exception as e:
            logger.debug(f"Yahoo search failed for '{query}': {e}")

        self._search_cache[cache_key] = None
        return None

    def fetch_financial_data(self, yahoo_ticker: str, retries: int = 3) -> Dict:
        """Fetch financial data from Yahoo Finance with retry on rate limit."""
        for attempt in range(retries):
            try:
                stock = yf.Ticker(yahoo_ticker)
                info = stock.info

                if not info or info.get('regularMarketPrice') is None:
                    return {}

                return {
                    'YF_Ticker': yahoo_ticker,
                    'Market_Cap': info.get('marketCap'),
                    'Stock_Price': info.get('regularMarketPrice') or info.get('currentPrice'),
                    'Volume': info.get('regularMarketVolume') or info.get('averageVolume'),
                    'P/E_Ratio': info.get('trailingPE'),
                    'Forward_P/E': info.get('forwardPE'),
                    'P/B_Ratio': info.get('priceToBook'),
                    'Revenue': info.get('totalRevenue'),
                    'Revenue_Growth': info.get('revenueGrowth'),
                    'Net_Income': info.get('netIncomeToCommon'),
                    'EBITDA': info.get('ebitda'),
                    'Free_Cash_Flow': info.get('freeCashflow'),
                    'ROE': info.get('returnOnEquity'),
                    'ROA': info.get('returnOnAssets'),
                    'Dividend_Yield': info.get('dividendYield'),
                    'Profit_Margin': info.get('profitMargins'),
                    'Operating_Margin': info.get('operatingMargins'),
                    'Debt_to_Equity': info.get('debtToEquity'),
                    'Employees': info.get('fullTimeEmployees'),
                    'Sector_YF': info.get('sector'),
                    'Industry_YF': info.get('industry'),
                    'Currency': info.get('currency'),
                    'Country_YF': info.get('country'),
                }
            except Exception as e:
                if 'Rate' in str(e) or 'Too Many' in str(e):
                    wait = (attempt + 1) * 5
                    logger.info(f"Rate limited, waiting {wait}s (attempt {attempt+1}/{retries})...")
                    time.sleep(wait)
                else:
                    logger.warning(f"Failed to fetch data for {yahoo_ticker}: {e}")
                    return {}
        return {}

    def enrich(self, sample_size: int = 0) -> List[Dict]:
        """Enrich all companies with financial data.

        Args:
            sample_size: If > 0, only enrich the first N companies (for testing).
        """
        if not self.suffix:
            logger.error(f"Cannot enrich: {self.exchange} is not supported by Yahoo Finance.")
            return self.load_data()

        data = self.load_data()
        companies = data[:sample_size] if sample_size > 0 else data
        total = len(companies)

        success_count = 0
        for i, company in enumerate(companies):
            # Skip already enriched
            if company.get('Enrichment_Status') == 'success':
                success_count += 1
                continue

            yahoo_ticker = self.get_yahoo_ticker(company)
            if not yahoo_ticker:
                logger.warning(f"({i+1}/{total}) No ticker for {company.get('Full Company Name', '?')}")
                company['Enrichment_Status'] = 'no_ticker'
                continue

            logger.info(f"({i+1}/{total}) Fetching data for {yahoo_ticker}...")
            financial_data = self.fetch_financial_data(yahoo_ticker)

            if financial_data:
                company.update(financial_data)
                company['Enrichment_Status'] = 'success'
                success_count += 1
            else:
                company['Enrichment_Status'] = 'not_found'

            time.sleep(self.delay)

            # Save progress every 25 companies
            if (i + 1) % 25 == 0:
                self._save(data)
                logger.info(f"Progress saved ({i+1}/{total}, {success_count} enriched).")

        self._save(data)
        logger.info(f"Enrichment complete: {success_count}/{total} companies enriched.")
        logger.info(f"Saved to {self.output_file}")
        return data

    def _save(self, data: List[Dict]):
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)


def enrich_country(exchange: str, input_file: str, output_file: str = None,
                   ticker_field: str = 'Ticker', sample_size: int = 0):
    """Convenience function to enrich company data for a specific exchange."""
    if output_file is None:
        base = input_file.rsplit('.', 1)[0]
        output_file = f"{base}_enriched.json"

    enricher = UniversalEnricher(
        exchange=exchange,
        input_file=input_file,
        output_file=output_file,
        ticker_field=ticker_field,
    )
    return enricher.enrich(sample_size=sample_size)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Enrich ASEAN company data with Yahoo Finance')
    parser.add_argument('exchange', choices=list(EXCHANGE_SUFFIXES.keys()),
                        help='Exchange code (IDX, SGX, SET, BURSA, HOSE, HNX, PSE)')
    parser.add_argument('input_file', help='Input JSON file with company data')
    parser.add_argument('-o', '--output', help='Output JSON file (default: input_enriched.json)')
    parser.add_argument('-t', '--ticker-field', default='Ticker',
                        help='JSON field containing the ticker symbol (default: Ticker)')
    parser.add_argument('-n', '--sample', type=int, default=0,
                        help='Only enrich first N companies (0 = all)')

    args = parser.parse_args()
    enrich_country(
        exchange=args.exchange,
        input_file=args.input_file,
        output_file=args.output,
        ticker_field=args.ticker_field,
        sample_size=args.sample,
    )
