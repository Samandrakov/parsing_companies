"""
ISIC Rev.4 sector/industry classifier for ASEAN company data.

Maps local exchange sector/industry names to standardized ISIC codes.
This enables cross-country comparison of market structures in the dissertation.

ISIC = International Standard Industrial Classification (UN)
"""

import json
import re
import logging
from typing import Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ISIC Rev.4 Section codes (top level)
ISIC_SECTIONS = {
    'A': 'Agriculture, forestry and fishing',
    'B': 'Mining and quarrying',
    'C': 'Manufacturing',
    'D': 'Electricity, gas, steam and air conditioning supply',
    'E': 'Water supply; sewerage, waste management',
    'F': 'Construction',
    'G': 'Wholesale and retail trade',
    'H': 'Transportation and storage',
    'I': 'Accommodation and food service activities',
    'J': 'Information and communication',
    'K': 'Financial and insurance activities',
    'L': 'Real estate activities',
    'M': 'Professional, scientific and technical activities',
    'N': 'Administrative and support service activities',
    'O': 'Public administration and defence',
    'P': 'Education',
    'Q': 'Human health and social work activities',
    'R': 'Arts, entertainment and recreation',
    'S': 'Other service activities',
}

# Mapping keywords -> ISIC section code
# Covers: English (SGX, PSE, SET, Bursa), Indonesian (IDX), and common variations
KEYWORD_TO_ISIC = {
    # A - Agriculture
    'agriculture': 'A', 'farming': 'A', 'plantation': 'A', 'palm oil': 'A',
    'rubber': 'A', 'crop': 'A', 'livestock': 'A', 'fishing': 'A', 'forestry': 'A',
    'aquaculture': 'A', 'timber': 'A', 'wood': 'A',
    # Indonesian
    'pertanian': 'A', 'perkebunan': 'A', 'peternakan': 'A', 'perikanan': 'A',
    'kelapa sawit': 'A', 'tanaman pangan': 'A',
    'produk makanan pertanian': 'A', 'perkebunan & tanaman pangan': 'A',
    'ikan, daging & produk unggas': 'A', 'barang kimia pertanian': 'A',
    'perhutanan': 'A',
    # Thai
    'agro': 'A', 'agribusiness': 'A',

    # B - Mining
    'mining': 'B', 'quarrying': 'B', 'coal': 'B', 'oil': 'B', 'gas': 'B',
    'petroleum': 'B', 'mineral': 'B', 'ore': 'B', 'gold': 'B', 'nickel': 'B',
    'tin': 'B', 'copper': 'B', 'bauxite': 'B',
    # Indonesian
    'pertambangan': 'B', 'batu bara': 'B', 'minyak': 'B',
    'minyak & gas': 'B', 'logam & mineral': 'B', 'bahan tambang': 'B',

    # C - Manufacturing
    'manufacturing': 'C', 'industrial': 'C', 'factory': 'C', 'production': 'C',
    'automotive': 'C', 'automobile': 'C', 'vehicle': 'C', 'car': 'C',
    'electronics': 'C', 'semiconductor': 'C', 'chip': 'C',
    'chemical': 'C', 'pharmaceutical': 'C', 'drug': 'C', 'medicine': 'C',
    'food processing': 'C', 'beverage': 'C', 'tobacco': 'C',
    'textile': 'C', 'garment': 'C', 'apparel': 'C', 'clothing': 'C',
    'steel': 'C', 'metal': 'C', 'cement': 'C', 'ceramic': 'C', 'glass': 'C',
    'plastic': 'C', 'packaging': 'C', 'paper': 'C', 'printing': 'C',
    'consumer goods': 'C', 'consumer products': 'C', 'fmcg': 'C',
    # Indonesian
    'manufaktur': 'C', 'industri': 'C', 'otomotif': 'C', 'farmasi': 'C',
    'makanan': 'C', 'minuman': 'C', 'kimia': 'C', 'semen': 'C', 'tekstil': 'C',
    'perindustrian': 'C', 'barang konsumen': 'C',

    # D - Electricity/Energy
    'electricity': 'D', 'power': 'D', 'energy': 'D', 'utility': 'D', 'utilities': 'D',
    'solar': 'D', 'renewable': 'D', 'wind energy': 'D', 'hydropower': 'D',
    # Indonesian
    'energi': 'D', 'listrik': 'D',

    # E - Water/Waste
    'water supply': 'E', 'sewerage': 'E', 'waste': 'E', 'recycling': 'E',
    'environmental': 'E',

    # F - Construction
    'construction': 'F', 'building': 'F', 'infrastructure': 'F',
    'engineering': 'F', 'contractor': 'F',
    # Indonesian
    'konstruksi': 'F',

    # G - Trade
    'wholesale': 'G', 'retail': 'G', 'trade': 'G', 'trading': 'G',
    'distribution': 'G', 'supermarket': 'G', 'department store': 'G',
    'e-commerce': 'G', 'ecommerce': 'G',
    # Indonesian
    'perdagangan': 'G', 'ritel': 'G', 'distributor': 'G',

    # H - Transportation
    'transportation': 'H', 'transport': 'H', 'shipping': 'H', 'logistics': 'H',
    'airline': 'H', 'aviation': 'H', 'port': 'H', 'warehouse': 'H',
    'freight': 'H', 'courier': 'H', 'postal': 'H',
    # Indonesian
    'transportasi': 'H', 'logistik': 'H', 'pelayaran': 'H', 'penerbangan': 'H',

    # I - Accommodation/Food
    'hotel': 'I', 'hospitality': 'I', 'resort': 'I', 'restaurant': 'I',
    'food service': 'I', 'catering': 'I', 'tourism': 'I',
    # Indonesian
    'perhotelan': 'I', 'pariwisata': 'I', 'restoran': 'I',
    'barang konsumen non-primer': 'I',  # IDX: hotels, restaurants, leisure

    # J - Information/Communication
    'technology': 'J', 'tech': 'J', 'software': 'J', 'it ': 'J',
    'telecom': 'J', 'telecommunications': 'J', 'media': 'J',
    'publishing': 'J', 'broadcasting': 'J', 'internet': 'J', 'digital': 'J',
    'data center': 'J', 'cloud': 'J',
    # Indonesian
    'teknologi': 'J', 'telekomunikasi': 'J', 'penerbitan': 'J',

    # K - Financial
    'bank': 'K', 'banking': 'K', 'finance': 'K', 'financial': 'K',
    'insurance': 'K', 'investment': 'K', 'securities': 'K',
    'asset management': 'K', 'fund': 'K', 'leasing': 'K', 'fintech': 'K',
    'multi-finance': 'K', 'capital': 'K',
    # Indonesian
    'keuangan': 'K', 'perbankan': 'K', 'asuransi': 'K', 'pembiayaan': 'K',

    # L - Real Estate
    'real estate': 'L', 'property': 'L', 'reit': 'L', 'realty': 'L',
    'land': 'L', 'housing': 'L', 'residential': 'L', 'commercial property': 'L',
    # Indonesian
    'properti': 'L', 'real estat': 'L',

    # M - Professional Services
    'consulting': 'M', 'advisory': 'M', 'legal': 'M', 'accounting': 'M',
    'research': 'M', 'scientific': 'M', 'design': 'M', 'architectural': 'M',
    'engineering services': 'M',
    # Indonesian
    'konsultasi': 'M', 'jasa profesional': 'M',

    # N - Administrative
    'staffing': 'N', 'security services': 'N', 'cleaning': 'N',
    'rental': 'N', 'travel agency': 'N', 'outsourcing': 'N',

    # P - Education
    'education': 'P', 'school': 'P', 'university': 'P', 'training': 'P',
    # Indonesian
    'pendidikan': 'P',

    # Q - Health
    'health': 'Q', 'healthcare': 'Q', 'hospital': 'Q', 'clinic': 'Q',
    'medical': 'Q',
    # Indonesian
    'kesehatan': 'Q', 'rumah sakit': 'Q',

    # R - Arts/Entertainment
    'entertainment': 'R', 'gaming': 'R', 'casino': 'R', 'sport': 'R',
    'leisure': 'R', 'amusement': 'R',
    # Indonesian
    'hiburan': 'R',

    # S - Other Services
    'services': 'S', 'conglomerate': 'S', 'diversified': 'S', 'holding': 'S',
    'multi-sector': 'S',

    # SET (Thailand) sector codes
    'agro': 'A', 'food': 'C', 'fashion': 'C', 'home': 'C',
    'auto': 'C', 'imm': 'C', 'steel': 'C', 'petro': 'C', 'pkg': 'C',
    'paper': 'C',
    'energ': 'D',
    'cons': 'F', 'conmat': 'F',
    'comm': 'G', 'fince': 'G',
    'trans': 'H',
    'tourism': 'I', 'hosp': 'I',
    'ict': 'J', 'media': 'J', 'etron': 'J',
    'fin': 'K', 'bank': 'K', 'insur': 'K',
    'prop': 'L', 'pf&reit': 'L',
    'prof': 'M',
    'helth': 'Q',

    # PSE (Philippines) sector codes
    'holding firms': 'S', 'mining and oil': 'B',

    # Bursa (Malaysia) common terms
    'plantation': 'A', 'consumer products': 'C', 'industrial products': 'C',

    # Company name keywords (for fallback classification by name)
    'trust': 'K', 'reit': 'L', 'biotech': 'Q', 'bio-tech': 'Q',
    'pharma': 'Q', 'shipping': 'H', 'marine': 'H', 'offshore': 'B',
    'telecom': 'J', 'semiconductor': 'C', 'steel': 'C', 'cement': 'C',
    'food': 'C', 'agri': 'A',
}


def classify_company(company: Dict, field_priority: List[str] = None) -> Dict:
    """Classify a company into ISIC section based on its sector/industry fields.

    Args:
        company: Company data dict
        field_priority: Fields to check, in order of priority.
                        Default: Sector_YF, Industry_YF, Sector, Industry, etc.

    Returns:
        Dict with ISIC_Section, ISIC_Code, ISIC_Description, Classification_Source
    """
    if field_priority is None:
        field_priority = [
            'Sector_YF', 'Industry_YF',  # Yahoo Finance (English, standardized)
            'Sub-industry', 'Sub_Industry',  # Most specific local field first
            'Industry',                       # Specific local field
            'Subsector',                      # Semi-specific local field
            'Sector',                         # Broad local field — checked AFTER specific ones
            'Sector_CSV', 'Industry_CSV',
            'Sector_JSON', 'Industry_JSON',
            'Main Business Fields', 'Main_Business_Fields',
            'DESCRIPTION', 'Description',
            'Full Company Name',  # Last resort: infer from company name
        ]

    for field in field_priority:
        value = company.get(field, '')
        if not value or not isinstance(value, str):
            continue

        isic_code = _match_isic(value)
        if isic_code:
            return {
                'ISIC_Section': isic_code,
                'ISIC_Description': ISIC_SECTIONS.get(isic_code, 'Unknown'),
                'Classification_Source': field,
                'Classification_Text': value[:100],
            }

    return {
        'ISIC_Section': None,
        'ISIC_Description': 'Unclassified',
        'Classification_Source': None,
        'Classification_Text': None,
    }


def _match_isic(text: str) -> Optional[str]:
    """Match text to an ISIC section code using keyword matching."""
    text_lower = text.lower()

    # Try exact/longest match first
    best_match = None
    best_length = 0

    for keyword, code in KEYWORD_TO_ISIC.items():
        if keyword in text_lower and len(keyword) > best_length:
            best_match = code
            best_length = len(keyword)

    return best_match


def classify_all(input_file: str, output_file: str = None) -> Tuple[List[Dict], Dict]:
    """Classify all companies in a JSON file and add ISIC codes.

    Returns:
        Tuple of (enriched_data, statistics)
    """
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    logger.info(f"Classifying {len(data)} companies...")

    stats = {'total': len(data), 'classified': 0, 'unclassified': 0, 'sections': {}}

    for company in data:
        classification = classify_company(company)
        company.update(classification)

        section = classification['ISIC_Section']
        if section:
            stats['classified'] += 1
            stats['sections'][section] = stats['sections'].get(section, 0) + 1
        else:
            stats['unclassified'] += 1

    if output_file is None:
        base = input_file.rsplit('.', 1)[0]
        output_file = f"{base}_classified.json"

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # Print statistics
    logger.info(f"Classification complete: {stats['classified']}/{stats['total']} classified")
    logger.info(f"Unclassified: {stats['unclassified']}")
    print(f"\nISIC Section Distribution:")
    for code in sorted(stats['sections'].keys()):
        count = stats['sections'][code]
        desc = ISIC_SECTIONS.get(code, '?')
        pct = count / stats['total'] * 100
        print(f"  {code} - {desc}: {count} ({pct:.1f}%)")

    if stats['unclassified'] > 0:
        print(f"\n  Unclassified: {stats['unclassified']} ({stats['unclassified']/stats['total']*100:.1f}%)")

    logger.info(f"Output saved to {output_file}")
    return data, stats


def compare_market_structures(files: Dict[str, str]) -> Dict:
    """Compare ISIC market structures across multiple countries.

    Args:
        files: Dict of country_name -> json_file_path

    Returns:
        Comparison matrix: {country: {isic_section: percentage}}
    """
    comparison = {}

    for country, filepath in files.items():
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)

        total = len(data)
        sections = {}
        for company in data:
            section = company.get('ISIC_Section')
            if section:
                sections[section] = sections.get(section, 0) + 1

        comparison[country] = {
            'total_companies': total,
            'classified': sum(sections.values()),
            'sections': {k: v / total * 100 for k, v in sections.items()},
        }

    # Print comparison table
    all_sections = sorted(set(
        s for c in comparison.values() for s in c['sections'].keys()
    ))

    header = f"{'ISIC Section':<50}" + "".join(f"{c:>12}" for c in comparison.keys())
    print(header)
    print("-" * len(header))

    for section in all_sections:
        desc = ISIC_SECTIONS.get(section, '?')
        row = f"{section} - {desc:<47}"
        for country in comparison.keys():
            pct = comparison[country]['sections'].get(section, 0)
            row += f"{pct:>11.1f}%"
        print(row)

    # Totals
    print("-" * len(header))
    row = f"{'Total companies':<50}"
    for country in comparison.keys():
        row += f"{comparison[country]['total_companies']:>12}"
    print(row)

    return comparison


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Classify ASEAN companies by ISIC Rev.4')
    subparsers = parser.add_subparsers(dest='command')

    # Classify command
    classify_parser = subparsers.add_parser('classify', help='Classify companies in a JSON file')
    classify_parser.add_argument('input_file', help='Input JSON file')
    classify_parser.add_argument('-o', '--output', help='Output file')

    # Compare command
    compare_parser = subparsers.add_parser('compare', help='Compare market structures across countries')
    compare_parser.add_argument('files', nargs='+', help='country:file pairs (e.g., Indonesia:idx.json)')

    args = parser.parse_args()

    if args.command == 'classify':
        classify_all(args.input_file, args.output)

    elif args.command == 'compare':
        file_dict = {}
        for pair in args.files:
            country, path = pair.split(':', 1)
            file_dict[country] = path
        compare_market_structures(file_dict)

    else:
        parser.print_help()
