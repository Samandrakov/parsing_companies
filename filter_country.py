"""
Country filter utility for ASEAN company data.

Filters out companies that are not truly domestic based on:
1. "Incorporated in" field (SGX data has this)
2. Phone number country code analysis
3. Website domain analysis
4. Registered office address analysis

Also detects and flags offshore jurisdictions (Bermuda, Cayman Islands, BVI, etc.)
and tries to determine the real country of origin.
"""

import json
import re
import logging
from typing import Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Offshore jurisdictions — companies registered here are usually from elsewhere
OFFSHORE_JURISDICTIONS = {
    'BERMUDA', 'CAYMAN ISLANDS', 'BRITISH VIRGIN ISLANDS', 'BVI',
    'JERSEY', 'GUERNSEY', 'ISLE OF MAN', 'MAURITIUS', 'PANAMA',
    'BAHAMAS', 'LIECHTENSTEIN', 'LUXEMBOURG', 'SEYCHELLES',
}

# ASEAN countries
ASEAN_COUNTRIES = {
    'INDONESIA', 'SINGAPORE', 'THAILAND', 'MALAYSIA', 'VIETNAM',
    'PHILIPPINES', 'CAMBODIA', 'LAOS', 'MYANMAR', 'BRUNEI',
}

# Phone code to country mapping (ASEAN + common)
PHONE_CODES = {
    '+62': 'INDONESIA', '62': 'INDONESIA',
    '+65': 'SINGAPORE', '65': 'SINGAPORE',
    '+66': 'THAILAND', '66': 'THAILAND',
    '+60': 'MALAYSIA', '60': 'MALAYSIA',
    '+84': 'VIETNAM', '84': 'VIETNAM',
    '+63': 'PHILIPPINES', '63': 'PHILIPPINES',
    '+855': 'CAMBODIA', '855': 'CAMBODIA',
    '+856': 'LAOS', '856': 'LAOS',
    '+95': 'MYANMAR', '95': 'MYANMAR',
    '+673': 'BRUNEI', '673': 'BRUNEI',
    '+86': 'CHINA', '86': 'CHINA',
    '+852': 'HONG KONG', '852': 'HONG KONG',
    '+81': 'JAPAN', '81': 'JAPAN',
    '+82': 'SOUTH KOREA', '82': 'SOUTH KOREA',
    '+91': 'INDIA', '91': 'INDIA',
    '+61': 'AUSTRALIA', '61': 'AUSTRALIA',
    '+44': 'UNITED KINGDOM', '44': 'UNITED KINGDOM',
    '+1': 'USA/CANADA', '1': 'USA/CANADA',
}

# Domain TLD to country mapping
DOMAIN_TLDS = {
    '.id': 'INDONESIA', '.co.id': 'INDONESIA',
    '.sg': 'SINGAPORE', '.com.sg': 'SINGAPORE',
    '.th': 'THAILAND', '.co.th': 'THAILAND',
    '.my': 'MALAYSIA', '.com.my': 'MALAYSIA',
    '.vn': 'VIETNAM', '.com.vn': 'VIETNAM',
    '.ph': 'PHILIPPINES', '.com.ph': 'PHILIPPINES',
    '.kh': 'CAMBODIA', '.com.kh': 'CAMBODIA',
    '.la': 'LAOS',
    '.mm': 'MYANMAR',
    '.bn': 'BRUNEI', '.com.bn': 'BRUNEI',
    '.cn': 'CHINA', '.com.cn': 'CHINA',
    '.hk': 'HONG KONG', '.com.hk': 'HONG KONG',
    '.jp': 'JAPAN', '.co.jp': 'JAPAN',
    '.kr': 'SOUTH KOREA', '.co.kr': 'SOUTH KOREA',
    '.au': 'AUSTRALIA', '.com.au': 'AUSTRALIA',
    '.uk': 'UNITED KINGDOM', '.co.uk': 'UNITED KINGDOM',
}

# Address keywords to country
ADDRESS_KEYWORDS = {
    'jakarta': 'INDONESIA', 'indonesia': 'INDONESIA', 'surabaya': 'INDONESIA',
    'singapore': 'SINGAPORE',
    'bangkok': 'THAILAND', 'thailand': 'THAILAND',
    'kuala lumpur': 'MALAYSIA', 'malaysia': 'MALAYSIA', 'selangor': 'MALAYSIA',
    'ho chi minh': 'VIETNAM', 'hanoi': 'VIETNAM', 'vietnam': 'VIETNAM',
    'manila': 'PHILIPPINES', 'philippines': 'PHILIPPINES', 'makati': 'PHILIPPINES',
    'phnom penh': 'CAMBODIA', 'cambodia': 'CAMBODIA',
    'vientiane': 'LAOS', 'laos': 'LAOS',
    'yangon': 'MYANMAR', 'myanmar': 'MYANMAR',
    'hong kong': 'HONG KONG',
    'beijing': 'CHINA', 'shanghai': 'CHINA', 'shenzhen': 'CHINA', 'china': 'CHINA',
    'tokyo': 'JAPAN', 'japan': 'JAPAN',
}


def detect_country_from_phone(phone: str) -> Optional[str]:
    """Detect country from phone number prefix."""
    if not phone:
        return None
    phone = re.sub(r'[^\d+]', '', str(phone))
    # Try longest codes first
    for code_len in [4, 3, 2]:
        for code, country in PHONE_CODES.items():
            if phone.startswith(code) and len(code) >= code_len:
                return country
    return None


def detect_country_from_website(website: str) -> Optional[str]:
    """Detect country from website domain TLD."""
    if not website:
        return None
    website = str(website).lower().rstrip('/')
    # Check longest TLDs first (e.g., .co.id before .id)
    for tld in sorted(DOMAIN_TLDS.keys(), key=len, reverse=True):
        if website.endswith(tld) or f'{tld}/' in website:
            return DOMAIN_TLDS[tld]
    return None


def detect_country_from_address(address: str) -> Optional[str]:
    """Detect country from registered office address."""
    if not address:
        return None
    address_lower = str(address).lower()
    for keyword, country in ADDRESS_KEYWORDS.items():
        if keyword in address_lower:
            return country
    return None


def analyze_company(company: Dict) -> Dict:
    """Analyze a company and determine its likely real country."""
    signals = {}

    # Signal 1: Incorporated in (most authoritative)
    incorporated = (company.get('Incorporated in') or '').upper().strip()
    if incorporated:
        signals['incorporated'] = incorporated

    # Signal 2: Phone number
    phone = company.get('Telephone', '') or company.get('Phone', '')
    phone_country = detect_country_from_phone(phone)
    if phone_country:
        signals['phone'] = phone_country

    # Signal 3: Website domain
    website = company.get('Link to Internet Website', '') or company.get('Website', '')
    web_country = detect_country_from_website(website)
    if web_country:
        signals['website'] = web_country

    # Signal 4: Address
    address = company.get('Registered Office', '') or company.get('Address', '')
    addr_country = detect_country_from_address(address)
    if addr_country:
        signals['address'] = addr_country

    # Determine real country
    real_country = _resolve_country(signals)
    is_offshore = incorporated in OFFSHORE_JURISDICTIONS
    is_asean = real_country in ASEAN_COUNTRIES if real_country else None

    return {
        'signals': signals,
        'real_country': real_country,
        'is_offshore_registered': is_offshore,
        'is_asean': is_asean,
    }


def _resolve_country(signals: Dict) -> Optional[str]:
    """Resolve the most likely real country from multiple signals."""
    incorporated = signals.get('incorporated')

    # If incorporated in a non-offshore country, trust it
    if incorporated and incorporated not in OFFSHORE_JURISDICTIONS:
        return incorporated

    # If offshore, use other signals to find real country
    other_countries = [
        signals.get('address'),
        signals.get('phone'),
        signals.get('website'),
    ]
    other_countries = [c for c in other_countries if c]

    if other_countries:
        # Return most common signal
        from collections import Counter
        counts = Counter(other_countries)
        return counts.most_common(1)[0][0]

    # Fall back to incorporated even if offshore
    return incorporated


def filter_companies(input_file: str, output_file: str, target_country: str = None,
                     asean_only: bool = False, exclude_offshore: bool = False) -> Tuple[List[Dict], Dict]:
    """Filter and annotate companies with country analysis.

    Args:
        input_file: Input JSON file
        output_file: Output JSON file
        target_country: Only keep companies from this country (e.g., 'SINGAPORE')
        asean_only: Only keep companies from ASEAN countries
        exclude_offshore: Exclude companies registered in offshore jurisdictions

    Returns:
        Tuple of (filtered_data, statistics)
    """
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    logger.info(f"Loaded {len(data)} companies from {input_file}")

    stats = {
        'total': len(data),
        'analyzed': 0,
        'filtered_out': 0,
        'offshore': 0,
        'asean': 0,
        'countries': {},
    }

    filtered = []
    for company in data:
        analysis = analyze_company(company)
        stats['analyzed'] += 1

        # Add analysis results to company data
        company['_country_analysis'] = analysis
        company['Real_Country'] = analysis['real_country']
        company['Is_Offshore'] = analysis['is_offshore_registered']
        company['Is_ASEAN'] = analysis['is_asean']

        if analysis['is_offshore_registered']:
            stats['offshore'] += 1
        if analysis['is_asean']:
            stats['asean'] += 1

        real = analysis['real_country'] or 'UNKNOWN'
        stats['countries'][real] = stats['countries'].get(real, 0) + 1

        # Apply filters
        keep = True
        if target_country and analysis['real_country'] != target_country.upper():
            keep = False
        if asean_only and not analysis['is_asean']:
            keep = False
        if exclude_offshore and analysis['is_offshore_registered']:
            keep = False

        if keep:
            filtered.append(company)
        else:
            stats['filtered_out'] += 1

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(filtered, f, ensure_ascii=False, indent=2)

    logger.info(f"Filtered: {len(filtered)}/{len(data)} companies kept")
    logger.info(f"Countries found: {stats['countries']}")
    logger.info(f"Offshore registered: {stats['offshore']}")
    logger.info(f"ASEAN companies: {stats['asean']}")
    logger.info(f"Output saved to {output_file}")

    return filtered, stats


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Filter ASEAN companies by real country of origin')
    parser.add_argument('input_file', help='Input JSON file')
    parser.add_argument('-o', '--output', help='Output file (default: input_filtered.json)')
    parser.add_argument('-c', '--country', help='Keep only companies from this country')
    parser.add_argument('--asean-only', action='store_true', help='Keep only ASEAN companies')
    parser.add_argument('--exclude-offshore', action='store_true', help='Exclude offshore-registered companies')
    parser.add_argument('--stats-only', action='store_true', help='Only print statistics, do not filter')

    args = parser.parse_args()

    output = args.output or args.input_file.rsplit('.', 1)[0] + '_filtered.json'

    if args.stats_only:
        # Just analyze and print stats
        with open(args.input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        countries = {}
        for c in data:
            analysis = analyze_company(c)
            real = analysis['real_country'] or 'UNKNOWN'
            countries[real] = countries.get(real, 0) + 1

        print(f"\nTotal companies: {len(data)}")
        print(f"\nCountry distribution:")
        for country, count in sorted(countries.items(), key=lambda x: -x[1]):
            pct = count / len(data) * 100
            print(f"  {country}: {count} ({pct:.1f}%)")
    else:
        filtered, stats = filter_companies(
            input_file=args.input_file,
            output_file=output,
            target_country=args.country,
            asean_only=args.asean_only,
            exclude_offshore=args.exclude_offshore,
        )
