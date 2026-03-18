"""
Consolidate all ASEAN country data into a single dataset.
Merges ISIC classification (from _classified files) with financial data (from _companies files).
Produces:
  - asean_consolidated.json  (unified JSON)
  - asean_consolidated.csv   (for Excel)
  - asean_summary.csv        (country x sector pivot table)
"""

import json
import csv
import sys
import os
from isic_mapper import classify_company, ISIC_SECTIONS

sys.stdout.reconfigure(encoding='utf-8')

# Classified files (ISIC data)
CLASSIFIED_FILES = {
    'Indonesia':   'Indonesia/idx_classified.json',
    'Singapore':   'Singapore/SGX/sgx_classified.json',
    'Thailand':    'Thailand/set_classified.json',
    'Malaysia':    'Malaysia/bursa_classified.json',
    'Vietnam':     'Vietnam/hose_classified.json',
    'Philippines': 'Philippines/pse_classified.json',
    'Myanmar':     'Myanmar/ysx_classified.json',
    'Cambodia':    'Cambodia/csx_classified.json',
    'Laos':        'Laos/lsx_classified.json',
}

# Companies files (financial data) — may not exist for all countries
FINANCIAL_FILES = {
    'Indonesia':   'Indonesia/idx_companies.json',
    'Singapore':   'Singapore/SGX/output_data_enriched.json',
    'Thailand':    'Thailand/set_companies.json',
    'Malaysia':    'Malaysia/bursa_companies.json',
    'Vietnam':     'Vietnam/hose_companies.json',
    'Philippines': 'Philippines/pse_companies.json',
    'Myanmar':     'Myanmar/ysx_companies.json',
    'Cambodia':    'Cambodia/csx_companies.json',
    'Laos':        'Laos/lsx_companies.json',
}

# Standardized output columns
OUTPUT_COLUMNS = [
    'Country', 'Exchange', 'Ticker', 'Full Company Name',
    'Sector', 'Industry', 'ISIC_Section', 'ISIC_Description',
    'Incorporated in', 'Market_Tier',
    'Stock_Price', 'Volume', 'Market_Cap',
]

# Financial fields to pull from _companies files
FINANCIAL_FIELDS = [
    'Stock_Price', 'Volume', 'Market_Cap', 'Market_Cap_MYR_M',
    'Market_Tier', 'PE_Ratio', 'Dividend_Yield', 'Week52_Range',
    'Week52_High', 'Week52_Low', 'Revenue', 'Net_Income',
]


def _load_financial_list(filepath):
    """Load financial data file as a list (same order as classified file)."""
    if not os.path.exists(filepath):
        return []
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def consolidate():
    all_companies = []

    for country, filepath in CLASSIFIED_FILES.items():
        if not os.path.exists(filepath):
            print(f"WARNING: {filepath} not found, skipping {country}")
            continue

        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Load financial data (same order as classified — matched by index)
        fin_file = FINANCIAL_FILES.get(country, '')
        fin_list = _load_financial_list(fin_file) if fin_file else []
        fin_matched = 0

        for i, c in enumerate(data):
            # Pull ticker from financial file if classified file doesn't have it
            fin = fin_list[i] if i < len(fin_list) else {}

            ticker = c.get('Ticker') or c.get('Code') or fin.get('Ticker') or fin.get('Code') or ''
            exchange = c.get('Exchange') or fin.get('Exchange') or ''

            row = {
                'Country': country,
                'Exchange': exchange,
                'Ticker': ticker,
                'Full Company Name': c.get('Full Company Name', c.get('Company Name', '')),
                'Sector': c.get('Sector_YF') or c.get('Sector', '') or fin.get('Sector_YF') or fin.get('Sector', ''),
                'Industry': c.get('Industry_YF') or c.get('Industry', '') or fin.get('Industry_YF') or fin.get('Industry', ''),
                'ISIC_Section': c.get('ISIC_Section', ''),
                'ISIC_Description': c.get('ISIC_Description', ''),
                'Incorporated in': c.get('Incorporated in', '') or fin.get('Incorporated in', country.upper()),
                'Market_Tier': fin.get('Market_Tier', ''),
                'Stock_Price': None,
                'Volume': None,
                'Market_Cap': None,
            }

            # Pull financial data from companies file
            if fin:
                fin_matched += 1
                row['Stock_Price'] = fin.get('Stock_Price')
                row['Volume'] = fin.get('Volume')
                # Market cap: try Market_Cap first, then Market_Cap_MYR_M
                mcap = fin.get('Market_Cap')
                if mcap is None and fin.get('Market_Cap_MYR_M') is not None:
                    # Convert MYR millions to absolute value (MYR)
                    mcap = fin['Market_Cap_MYR_M'] * 1_000_000
                row['Market_Cap'] = mcap

            all_companies.append(row)

        print(f"  {country}: {len(data)} classified, {fin_matched} matched with financial data")

    print(f"Total consolidated companies: {len(all_companies)}")

    # Save JSON
    with open('asean_consolidated.json', 'w', encoding='utf-8') as f:
        json.dump(all_companies, f, ensure_ascii=False, indent=2)
    print("Saved asean_consolidated.json")

    # Save CSV
    with open('asean_consolidated.csv', 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(all_companies)
    print("Saved asean_consolidated.csv")

    # Summary pivot: Country x ISIC Section
    pivot = {}
    country_totals = {}
    for c in all_companies:
        country = c['Country']
        isic = c['ISIC_Section'] or 'Unclassified'
        if country not in pivot:
            pivot[country] = {}
            country_totals[country] = 0
        pivot[country][isic] = pivot[country].get(isic, 0) + 1
        country_totals[country] += 1

    # All ISIC sections found
    all_sections = sorted(set(
        s for sections in pivot.values() for s in sections
    ))

    with open('asean_summary.csv', 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        header = ['Country', 'Total'] + [f"{s} ({ISIC_SECTIONS.get(s, s)[:30]})" for s in all_sections]
        writer.writerow(header)

        for country in CLASSIFIED_FILES:
            if country not in pivot:
                continue
            total = country_totals[country]
            row = [country, total]
            for s in all_sections:
                count = pivot[country].get(s, 0)
                pct = count / total * 100 if total else 0
                row.append(f"{count} ({pct:.1f}%)")
            writer.writerow(row)

        # Total row
        row = ['TOTAL', sum(country_totals.values())]
        for s in all_sections:
            total_s = sum(pivot.get(c, {}).get(s, 0) for c in pivot)
            row.append(total_s)
        writer.writerow(row)

    print("Saved asean_summary.csv")

    # Print summary with financial coverage
    with_price = sum(1 for c in all_companies if c.get('Stock_Price'))
    with_mcap = sum(1 for c in all_companies if c.get('Market_Cap'))
    with_volume = sum(1 for c in all_companies if c.get('Volume'))

    print(f"\n{'Country':<15} {'Companies':>10} {'w/Price':>10} {'w/MCap':>10}")
    print('-' * 47)
    for country in CLASSIFIED_FILES:
        if country in country_totals:
            cos = [c for c in all_companies if c['Country'] == country]
            n_price = sum(1 for c in cos if c.get('Stock_Price'))
            n_mcap = sum(1 for c in cos if c.get('Market_Cap'))
            print(f"{country:<15} {len(cos):>10} {n_price:>10} {n_mcap:>10}")
    print('-' * 47)
    total = sum(country_totals.values())
    print(f"{'TOTAL':<15} {total:>10} {with_price:>10} {with_mcap:>10}")


if __name__ == '__main__':
    consolidate()
