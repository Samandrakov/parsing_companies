"""
Russia-ASEAN trade data analyzer.

Provides trade volume data between Russia and ASEAN countries by sector,
enabling correlation analysis with stock market sector composition.

Data sources:
    1. UN Comtrade API (requires free registration at comtradeplus.un.org)
    2. ITC Trade Map (trademap.org)
    3. Hardcoded baseline data from latest available statistics

Russia country code (ISO): 643
ASEAN country codes: see ASEAN_CODES below
"""

import json
import logging
import os
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ISO 3166 numeric codes
COUNTRY_CODES = {
    'Russia': '643',
    'Indonesia': '360',
    'Singapore': '702',
    'Thailand': '764',
    'Malaysia': '458',
    'Vietnam': '704',
    'Philippines': '608',
    'Cambodia': '116',
    'Laos': '418',
    'Myanmar': '104',
    'Brunei': '096',
}

# HS2 codes -> ISIC mapping (for correlation with market structure)
HS2_TO_ISIC = {
    '01-05': 'A',   # Live animals, animal products
    '06-14': 'A',   # Vegetable products
    '15': 'A',      # Animal/vegetable fats and oils
    '16-24': 'C',   # Prepared foodstuffs, beverages, tobacco
    '25-27': 'B',   # Mineral products (incl. oil, gas, coal)
    '28-38': 'C',   # Chemical products
    '39-40': 'C',   # Plastics, rubber
    '41-43': 'C',   # Leather, fur
    '44-46': 'A',   # Wood products
    '47-49': 'C',   # Paper, printing
    '50-63': 'C',   # Textiles
    '64-67': 'C',   # Footwear, headgear
    '68-70': 'C',   # Stone, ceramic, glass
    '71': 'C',      # Precious metals, jewelry
    '72-83': 'C',   # Base metals
    '84-85': 'C',   # Machinery, electronics
    '86-89': 'H',   # Vehicles, transport equipment
    '90-92': 'C',   # Optical, medical instruments
    '93': 'C',      # Arms and ammunition
    '94-96': 'C',   # Miscellaneous manufactured
    '97': 'R',      # Works of art
}

# Baseline trade data: Russia <-> ASEAN (2023-2024, USD millions)
# Source: UN Comtrade, Russian Customs, ASEAN Secretariat statistics
# Format: {country: {exports_from_russia, imports_to_russia, top_export_sectors, top_import_sectors}}
BASELINE_TRADE_DATA = {
    'Indonesia': {
        'trade_volume_mln_usd': 3800,
        'russia_exports_mln_usd': 1900,
        'russia_imports_mln_usd': 1900,
        'top_russia_exports': [
            {'sector': 'B', 'product': 'Mineral fuels, oil', 'share_pct': 25},
            {'sector': 'C', 'product': 'Fertilizers', 'share_pct': 20},
            {'sector': 'C', 'product': 'Iron and steel', 'share_pct': 15},
            {'sector': 'C', 'product': 'Paper and paperboard', 'share_pct': 10},
        ],
        'top_russia_imports': [
            {'sector': 'A', 'product': 'Palm oil', 'share_pct': 35},
            {'sector': 'C', 'product': 'Rubber products', 'share_pct': 15},
            {'sector': 'C', 'product': 'Textiles and clothing', 'share_pct': 10},
            {'sector': 'C', 'product': 'Coffee, tea, spices', 'share_pct': 8},
        ],
    },
    'Vietnam': {
        'trade_volume_mln_usd': 7200,
        'russia_exports_mln_usd': 2000,
        'russia_imports_mln_usd': 5200,
        'top_russia_exports': [
            {'sector': 'B', 'product': 'Mineral fuels, coal', 'share_pct': 20},
            {'sector': 'C', 'product': 'Iron and steel', 'share_pct': 18},
            {'sector': 'C', 'product': 'Fertilizers', 'share_pct': 15},
            {'sector': 'C', 'product': 'Machinery and equipment', 'share_pct': 10},
        ],
        'top_russia_imports': [
            {'sector': 'C', 'product': 'Electronics, phones', 'share_pct': 40},
            {'sector': 'C', 'product': 'Textiles and footwear', 'share_pct': 20},
            {'sector': 'A', 'product': 'Coffee, pepper, cashew', 'share_pct': 10},
            {'sector': 'A', 'product': 'Seafood', 'share_pct': 8},
        ],
    },
    'Thailand': {
        'trade_volume_mln_usd': 2800,
        'russia_exports_mln_usd': 1200,
        'russia_imports_mln_usd': 1600,
        'top_russia_exports': [
            {'sector': 'B', 'product': 'Mineral fuels, oil', 'share_pct': 25},
            {'sector': 'C', 'product': 'Iron and steel', 'share_pct': 20},
            {'sector': 'C', 'product': 'Fertilizers', 'share_pct': 15},
        ],
        'top_russia_imports': [
            {'sector': 'C', 'product': 'Rubber and products', 'share_pct': 20},
            {'sector': 'C', 'product': 'Vehicles and parts', 'share_pct': 15},
            {'sector': 'C', 'product': 'Electronics', 'share_pct': 12},
            {'sector': 'A', 'product': 'Rice, food products', 'share_pct': 10},
        ],
    },
    'Singapore': {
        'trade_volume_mln_usd': 4500,
        'russia_exports_mln_usd': 2500,
        'russia_imports_mln_usd': 2000,
        'top_russia_exports': [
            {'sector': 'B', 'product': 'Mineral fuels, oil products', 'share_pct': 60},
            {'sector': 'C', 'product': 'Precious metals', 'share_pct': 10},
        ],
        'top_russia_imports': [
            {'sector': 'C', 'product': 'Electronics, machinery', 'share_pct': 35},
            {'sector': 'C', 'product': 'Pharmaceutical products', 'share_pct': 15},
            {'sector': 'K', 'product': 'Financial services (re-export hub)', 'share_pct': 10},
        ],
    },
    'Malaysia': {
        'trade_volume_mln_usd': 3200,
        'russia_exports_mln_usd': 1400,
        'russia_imports_mln_usd': 1800,
        'top_russia_exports': [
            {'sector': 'B', 'product': 'Mineral fuels', 'share_pct': 30},
            {'sector': 'C', 'product': 'Fertilizers', 'share_pct': 15},
            {'sector': 'C', 'product': 'Iron and steel', 'share_pct': 12},
        ],
        'top_russia_imports': [
            {'sector': 'A', 'product': 'Palm oil', 'share_pct': 30},
            {'sector': 'C', 'product': 'Electronics, semiconductors', 'share_pct': 25},
            {'sector': 'C', 'product': 'Rubber products', 'share_pct': 10},
        ],
    },
    'Philippines': {
        'trade_volume_mln_usd': 1200,
        'russia_exports_mln_usd': 600,
        'russia_imports_mln_usd': 600,
        'top_russia_exports': [
            {'sector': 'B', 'product': 'Mineral fuels', 'share_pct': 30},
            {'sector': 'C', 'product': 'Fertilizers', 'share_pct': 20},
            {'sector': 'C', 'product': 'Iron and steel', 'share_pct': 15},
        ],
        'top_russia_imports': [
            {'sector': 'C', 'product': 'Electronics, semiconductors', 'share_pct': 30},
            {'sector': 'A', 'product': 'Tropical fruits, coconut oil', 'share_pct': 20},
        ],
    },
    'Cambodia': {
        'trade_volume_mln_usd': 80,
        'russia_exports_mln_usd': 50,
        'russia_imports_mln_usd': 30,
        'top_russia_exports': [
            {'sector': 'C', 'product': 'Fertilizers', 'share_pct': 25},
            {'sector': 'C', 'product': 'Iron and steel', 'share_pct': 20},
        ],
        'top_russia_imports': [
            {'sector': 'C', 'product': 'Textiles, garments', 'share_pct': 40},
            {'sector': 'A', 'product': 'Rice', 'share_pct': 15},
        ],
    },
    'Myanmar': {
        'trade_volume_mln_usd': 150,
        'russia_exports_mln_usd': 100,
        'russia_imports_mln_usd': 50,
        'top_russia_exports': [
            {'sector': 'C', 'product': 'Machinery, military equipment', 'share_pct': 35},
            {'sector': 'C', 'product': 'Iron and steel', 'share_pct': 20},
        ],
        'top_russia_imports': [
            {'sector': 'A', 'product': 'Agricultural products', 'share_pct': 30},
            {'sector': 'C', 'product': 'Textiles', 'share_pct': 20},
        ],
    },
    'Laos': {
        'trade_volume_mln_usd': 30,
        'russia_exports_mln_usd': 20,
        'russia_imports_mln_usd': 10,
        'top_russia_exports': [
            {'sector': 'C', 'product': 'Machinery and equipment', 'share_pct': 30},
            {'sector': 'C', 'product': 'Iron and steel', 'share_pct': 20},
        ],
        'top_russia_imports': [
            {'sector': 'A', 'product': 'Coffee, agricultural products', 'share_pct': 40},
        ],
    },
    'Brunei': {
        'trade_volume_mln_usd': 15,
        'russia_exports_mln_usd': 10,
        'russia_imports_mln_usd': 5,
        'top_russia_exports': [
            {'sector': 'C', 'product': 'Iron and steel products', 'share_pct': 30},
        ],
        'top_russia_imports': [
            {'sector': 'B', 'product': 'Oil and gas', 'share_pct': 60},
        ],
    },
}


def get_trade_summary():
    """Print summary of Russia-ASEAN trade by country."""
    print("=" * 80)
    print("RUSSIA - ASEAN TRADE SUMMARY")
    print("=" * 80)
    print(f"{'Country':<15} {'Volume ($M)':>12} {'RU Export':>12} {'RU Import':>12} {'Balance':>12}")
    print("-" * 63)

    total_vol = 0
    total_exp = 0
    total_imp = 0

    sorted_countries = sorted(
        BASELINE_TRADE_DATA.items(),
        key=lambda x: x[1]['trade_volume_mln_usd'],
        reverse=True
    )

    for country, data in sorted_countries:
        vol = data['trade_volume_mln_usd']
        exp = data['russia_exports_mln_usd']
        imp = data['russia_imports_mln_usd']
        balance = exp - imp
        total_vol += vol
        total_exp += exp
        total_imp += imp

        sign = '+' if balance >= 0 else ''
        print(f"{country:<15} {vol:>12,} {exp:>12,} {imp:>12,} {sign}{balance:>11,}")

    print("-" * 63)
    total_balance = total_exp - total_imp
    sign = '+' if total_balance >= 0 else ''
    print(f"{'TOTAL':<15} {total_vol:>12,} {total_exp:>12,} {total_imp:>12,} {sign}{total_balance:>11,}")


def get_sector_trade_matrix():
    """Show trade by ISIC sector across all ASEAN countries."""
    from isic_mapper import ISIC_SECTIONS

    print("\n" + "=" * 80)
    print("RUSSIA-ASEAN TRADE BY ISIC SECTOR")
    print("=" * 80)

    # Aggregate by ISIC sector
    sector_totals = {}
    for country, data in BASELINE_TRADE_DATA.items():
        for item in data.get('top_russia_exports', []) + data.get('top_russia_imports', []):
            sector = item['sector']
            if sector not in sector_totals:
                sector_totals[sector] = {'countries': set(), 'mentions': 0}
            sector_totals[sector]['countries'].add(country)
            sector_totals[sector]['mentions'] += 1

    print(f"\n{'ISIC':<5} {'Sector Description':<45} {'Countries':>10} {'Mentions':>10}")
    print("-" * 70)
    for code in sorted(sector_totals.keys()):
        desc = ISIC_SECTIONS.get(code, 'Unknown')
        countries = len(sector_totals[code]['countries'])
        mentions = sector_totals[code]['mentions']
        print(f"{code:<5} {desc:<45} {countries:>10} {mentions:>10}")


def correlate_with_market(classified_file: str, country: str):
    """Correlate market structure (from ISIC classification) with trade data.

    Shows which sectors have high market cap AND high trade volume with Russia,
    indicating cooperation potential.
    """
    with open(classified_file, 'r', encoding='utf-8') as f:
        companies = json.load(f)

    trade = BASELINE_TRADE_DATA.get(country)
    if not trade:
        logger.error(f"No trade data for {country}")
        return

    # Count companies by ISIC sector
    sector_counts = {}
    for c in companies:
        section = c.get('ISIC_Section')
        if section:
            sector_counts[section] = sector_counts.get(section, 0) + 1

    # Get trade sectors
    trade_sectors = set()
    for item in trade.get('top_russia_exports', []) + trade.get('top_russia_imports', []):
        trade_sectors.add(item['sector'])

    from isic_mapper import ISIC_SECTIONS

    print(f"\n{'='*80}")
    print(f"MARKET STRUCTURE vs RUSSIA TRADE: {country.upper()}")
    print(f"{'='*80}")
    print(f"Trade volume with Russia: ${trade['trade_volume_mln_usd']:,}M")
    print(f"Listed companies analyzed: {len(companies)}")
    print(f"\n{'ISIC':<5} {'Sector':<40} {'Companies':>10} {'% Market':>10} {'RU Trade':>10}")
    print("-" * 75)

    total = len(companies)
    for code in sorted(set(list(sector_counts.keys()) + list(trade_sectors))):
        desc = ISIC_SECTIONS.get(code, 'Unknown')[:38]
        count = sector_counts.get(code, 0)
        pct = count / total * 100 if total else 0
        has_trade = 'YES' if code in trade_sectors else '-'
        print(f"{code:<5} {desc:<40} {count:>10} {pct:>9.1f}% {has_trade:>10}")

    # Identify cooperation opportunities
    print(f"\n--- COOPERATION POTENTIAL ---")
    for code in sorted(trade_sectors):
        if code in sector_counts:
            desc = ISIC_SECTIONS.get(code, '?')
            count = sector_counts[code]
            print(f"  {code} ({desc}): {count} listed companies + active Russia trade")
        else:
            desc = ISIC_SECTIONS.get(code, '?')
            print(f"  {code} ({desc}): Russia trades but no/few listed companies — potential gap")


def export_trade_data(output_file: str = 'russia_asean_trade.json'):
    """Export baseline trade data to JSON."""
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(BASELINE_TRADE_DATA, f, ensure_ascii=False, indent=2)
    logger.info(f"Trade data exported to {output_file}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Russia-ASEAN trade analysis')
    subparsers = parser.add_subparsers(dest='command')

    subparsers.add_parser('summary', help='Print trade summary table')
    subparsers.add_parser('sectors', help='Show trade by ISIC sector')

    corr_parser = subparsers.add_parser('correlate', help='Correlate market structure with trade')
    corr_parser.add_argument('classified_file', help='ISIC-classified JSON file')
    corr_parser.add_argument('country', help='Country name (e.g., Indonesia)')

    subparsers.add_parser('export', help='Export trade data to JSON')

    args = parser.parse_args()

    if args.command == 'summary':
        get_trade_summary()
    elif args.command == 'sectors':
        get_sector_trade_matrix()
    elif args.command == 'correlate':
        correlate_with_market(args.classified_file, args.country)
    elif args.command == 'export':
        export_trade_data()
    else:
        get_trade_summary()
