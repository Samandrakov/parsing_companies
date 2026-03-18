"""
Fetch full company names from KLSE Screener and update bursa_companies.json.
Strategy:
1. Fetch the screener page with per_page=2000 to get all stocks
2. Parse <td title="FULL COMPANY NAME"> attributes (ALL CAPS names)
3. Map stock codes to full names using the href in the same <td>
4. Convert ALL CAPS to proper Title Case using smart heuristics
5. Fall back to Yahoo Finance API for any codes not found
"""

import requests
import re
import json
import time
from pathlib import Path

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# Explicit acronyms that should ALWAYS stay uppercase
FORCE_UPPER = {
    # Financial
    "CIMB", "RHB", "AMMB", "BIMB", "AFG", "MNRB", "MBSB", "UOB",
    # Major corps
    "IHH", "IOI", "YTL", "UMW", "FGV", "IJM", "PPB", "KPJ", "MISC",
    "KLCC", "DRB", "MBM", "TDM", "CCB", "MMC", "UEM", "MKH",
    # Telcos/Tech
    "TM", "MSC", "CTOS",
    # Industry codes
    "REIT", "ETF", "FTSE", "MSCI", "KLCI",
    # Specific company-name acronyms (longer than 3 chars)
    "TXCD", "SCIB", "MMIS", "VSTECS", "MRCB", "MAHB",
    "LTKM", "APFT", "HPMT", "JAKS", "NLEX",
    "SAPNRG", "VITROX", "WELLCAL", "YINSON",
    "CEKD", "PCCS", "PMCK", "BJFOOD",
    # D.I.Y. special case handled separately
}

# Common 2-3 letter English words that should NOT stay uppercase.
# Everything NOT in this set and 2-3 letters long will be kept uppercase
# (since most 2-3 letter combos in company names are abbreviations).
COMMON_SHORT_WORDS = {
    # Articles/prepositions (handled separately as lowercase)
    "A", "AN", "OF", "AND", "THE", "FOR", "IN", "OR", "TO", "AT", "BY",
    "ON", "IS", "IT", "AS", "IF", "SO", "NO", "UP", "DO", "MY", "WE",
    "HE", "ME", "BE", "AM",
    # Common 3-letter English words that appear in company names as regular words
    "AIR", "OIL", "GAS", "SEA", "SUN", "TOP", "BIG", "RED", "NEW", "OLD",
    "ONE", "TWO", "TEN", "SIX", "NET", "BUS", "CAR", "VAN", "TIN", "ARM",
    "BOX", "CUT", "DRY", "EAR", "FAR", "FIT", "HOT", "KEY", "LAW", "LET",
    "LOW", "MAP", "OWN", "PAY", "PUT", "RAW", "SET", "SIT", "WAR", "WAX",
    "WET", "AGE", "AID", "ATE", "AWE", "AXE", "BAD", "BAN", "BAR", "BAY",
    "BED", "BID", "BIT", "BOW", "BUY", "CUP", "DAY", "DIG", "DOG", "DOT",
    "DUE", "EAT", "END", "ERA", "EVE", "EYE", "FAN", "FAT", "FEW", "FLY",
    "FOG", "FOX", "FUN", "GAP", "GEM", "GET", "GOD", "GUN", "GUT", "GUY",
    "HAT", "HER", "HIM", "HIS", "HOP", "HUG", "ICE", "ILL", "INK", "INN",
    "JAM", "JAR", "JAW", "JOY", "ALL", "ADD", "ACE", "ACT", "APP",
    "ART", "ASK", "BAG", "BAT", "CAN", "CAT", "COW", "CRY", "CUB",
    "DAD", "DIM", "DIP", "DOE", "DUG", "DYE", "EEL", "EGG", "ELF",
    "ELM", "EMU", "FIG", "FIN", "FIR", "FIX", "FUR", "GAG", "GEL",
    "GIG", "GNU", "GOT", "GUM", "HAD", "HAM", "HAS", "HAY", "HEN",
    "HEW", "HID", "HIT", "HOG", "HOW", "HUB", "HUE", "JAB", "JOB",
    "JOG", "JOT", "JUG", "KIT", "LAB", "LAD", "LAP", "LAY", "LED",
    "LEG", "LID", "LIE", "LIP", "LIT", "LOG", "LOT", "MAD", "MAN",
    "MAT", "MAY", "MEN", "MET", "MIX", "MOB", "MOM", "MOP", "MUD",
    "MUG", "NAP", "NIT", "NOD", "NOR", "NOT", "NOW", "NUN", "NUT",
    "OAK", "OAR", "OAT", "ODD", "OPT", "ORB", "ORE", "OUR", "OUT",
    "OWE", "OWL", "PAD", "PAL", "PAN", "PAT", "PAW", "PEA", "PEG",
    "PEN", "PET", "PIE", "PIG", "PIN", "PIT", "PLY", "POD", "POP",
    "POT", "PRY", "PUB", "PUG", "PUN", "PUS", "RAG", "RAN", "RAP",
    "RAT", "RAY", "RIB", "RID", "RIG", "RIM", "RIP", "ROB", "ROD",
    "ROT", "ROW", "RUB", "RUG", "RUN", "RUT", "RYE", "SAG", "SAP",
    "SAT", "SAW", "SAY", "SHE", "SHY", "SIN", "SIP", "SKI", "SKY",
    "SLY", "SOB", "SOD", "SON", "SOP", "SOT", "SOW", "SOY", "SPA",
    "SPY", "STY", "SUB", "SUM", "TAB", "TAG", "TAN", "TAP", "TAR",
    "TAT", "TAX", "TEA", "THE", "TIE", "TIP", "TOE", "TON", "TOO",
    "TOW", "TOY", "TUB", "TUG", "URN", "USE", "VAN", "VAT", "VET",
    "VIA", "VOW", "WAD", "WAG", "WAS", "WAY", "WEB", "WED", "WHO",
    "WIG", "WIN", "WIT", "WOE", "WOK", "WON", "WOO", "WOW", "YAK",
    "YAM", "YAP", "YAW", "YEA", "YES", "YET", "YEW", "YOU", "ZAP",
    "ZEN", "ZIP", "ZOO",
    # Malay/Chinese/place name words that are NOT acronyms
    "ANG", "HIN", "HUP", "HWA", "KIM", "LEE", "LIM", "POH", "TEO",
    "WAH", "ANN", "HON", "TAI", "JOO", "YEW", "HAP", "WAI",
    "HOO", "YAP", "ONG", "GOH", "SIM", "CHE", "SOH", "HOE",
    "FOO", "HUI", "TAN", "LOH", "YEO", "MOH", "WAN", "MAH",
    "KOH", "SOO", "LAI", "KAM", "YEE", "HAI", "UNI", "ECO",
    "BIO", "GEO", "NEO", "SIK", "PAC",
}


def _has_normal_vowel_pattern(word: str) -> bool:
    """Check if a word has a normal vowel pattern (i.e., looks like a real word, not an acronym)."""
    vowels = set("AEIOU")
    w = word.upper()
    vowel_count = sum(1 for c in w if c in vowels)
    # No vowels at all -> acronym (e.g., LTKM, MRCB)
    if vowel_count == 0:
        return False
    # Very low vowel ratio for word length -> likely acronym
    ratio = vowel_count / len(w)
    if ratio < 0.2 and len(w) >= 4:
        return False
    return True


def smart_title_case(name: str) -> str:
    """
    Convert ALL CAPS company name to proper Title Case.
    - Explicit acronyms in FORCE_UPPER always stay UPPERCASE
    - 2-3 letter words that are NOT common English words stay UPPERCASE (likely acronyms)
    - 'BERHAD' -> 'Berhad', 'BHD' -> 'Bhd', 'SDN' -> 'Sdn'
    - Prepositions lowercase (unless first word)
    - Single letters stay uppercase
    - Everything else gets title-cased
    - Special handling for D.I.Y., F&N, etc.
    """
    lowercase_words = {"OF", "AND", "THE", "FOR", "IN", "E", "DE", "DU", "OR"}

    # Handle special patterns before splitting
    # D.I.Y. -> keep as D.I.Y.
    name = re.sub(r'\bD\.I\.Y\.?', 'D.I.Y.', name, flags=re.IGNORECASE)
    # F&N -> keep as F&N
    name = re.sub(r'\bF&N\b', 'F&N', name, flags=re.IGNORECASE)

    words = name.split()
    result = []
    for i, w in enumerate(words):
        # Separate prefix/suffix punctuation: e.g. "(MALAYSIA)" -> "(" + "MALAYSIA" + ")"
        prefix_match = re.match(r'^([(\[]*)', w)
        suffix_match = re.search(r'([)\].,;:!?]*)$', w)
        prefix = prefix_match.group(1) if prefix_match else ""
        suffix = suffix_match.group(1) if suffix_match else ""
        core = w[len(prefix):len(w) - len(suffix)] if suffix else w[len(prefix):]

        if not core:
            result.append(w)
            continue

        core_upper = core.upper()
        core_alpha = re.sub(r'[^A-Za-z]', '', core).upper()

        # Special patterns: D.I.Y., D&O, F&N, etc.
        if re.match(r'^[A-Z]\.([A-Z]\.)+[A-Z]?\.?$', core, re.IGNORECASE):
            result.append(prefix + core.upper() + suffix)
            continue
        if re.match(r'^[A-Z]&[A-Z]$', core, re.IGNORECASE):
            result.append(prefix + core.upper() + suffix)
            continue

        # Explicit acronyms
        if core_upper in FORCE_UPPER:
            result.append(prefix + core_upper + suffix)
        elif core.lower() == "berhad":
            result.append(prefix + "Berhad" + suffix)
        elif core.lower() in ("bhd", "bhd."):
            result.append(prefix + "Bhd" + suffix)
        elif core.lower() == "sdn":
            result.append(prefix + "Sdn" + suffix)
        elif core_upper in lowercase_words and i > 0:
            result.append(prefix + core.lower() + suffix)
        elif len(core) == 1:
            # Single letter -> uppercase
            result.append(prefix + core.upper() + suffix)
        elif len(core_alpha) <= 3 and core_alpha.isalpha() and core_upper not in COMMON_SHORT_WORDS:
            # 2-3 letter word not in common English words -> likely acronym, keep uppercase
            result.append(prefix + core_upper + suffix)
        elif len(core_alpha) >= 4 and core_alpha.isalpha() and not _has_normal_vowel_pattern(core_alpha):
            # 4+ letter word with no vowels or unusual pattern -> likely acronym
            result.append(prefix + core_upper + suffix)
        else:
            # Regular word -> title case
            # Handle hyphenated words: "HARBOUR-LINK" -> "Harbour-Link", "DRB-HICOM" -> "DRB-Hicom"
            if "-" in core:
                parts = core.split("-")
                titled_parts = []
                for p in parts:
                    p_upper = p.upper()
                    p_alpha = re.sub(r'[^A-Za-z]', '', p).upper()
                    if p_upper in FORCE_UPPER:
                        titled_parts.append(p_upper)
                    elif len(p_alpha) <= 3 and p_alpha.isalpha() and p_upper not in COMMON_SHORT_WORDS:
                        titled_parts.append(p_upper)
                    else:
                        titled_parts.append(p.capitalize())
                result.append(prefix + "-".join(titled_parts) + suffix)
            else:
                result.append(prefix + core.capitalize() + suffix)

    return " ".join(result)


def fetch_klse_screener() -> dict:
    """Fetch all stocks from KLSE Screener and return code->full_name mapping."""
    url = "https://www.klsescreener.com/v2/screener/quote_results"
    params = {
        "board": "all",
        "sector": "all",
        "market_cap_min": "0",
        "per_page": "2000",
    }

    print("Fetching KLSE Screener page...")
    resp = requests.get(url, headers=HEADERS, params=params, timeout=60)
    resp.raise_for_status()
    html = resp.text
    print(f"  Got {len(html)} bytes of HTML")

    # Parse: <td title="FULL COMPANY NAME"><a href="/v2/stocks/view/CODE/slug">
    pattern = r'<td\s+title="([^"]+)">\s*<a\s+href="/v2/stocks/view/(\d+)/'
    matches = re.findall(pattern, html)

    code_to_name = {}
    for raw_name, code in matches:
        raw_name = raw_name.strip()
        if not raw_name:
            continue
        proper_name = smart_title_case(raw_name)
        if code not in code_to_name:
            code_to_name[code] = proper_name

    print(f"  Extracted {len(code_to_name)} company names")
    return code_to_name


def fetch_yahoo_name(ticker: str, code: str) -> str | None:
    """Try Yahoo Finance search API to get full company name."""
    for search_term in [f"{code}.KL", f"{ticker}.KL"]:
        url = "https://query1.finance.yahoo.com/v1/finance/search"
        params = {"q": search_term, "quotesCount": "3", "newsCount": "0"}
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                for q in data.get("quotes", []):
                    if q.get("exchange") == "KLS" or ".KL" in q.get("symbol", ""):
                        name = q.get("longname") or q.get("shortname")
                        if name:
                            return name
        except Exception as e:
            print(f"  Yahoo error for {ticker}/{code}: {e}")
        time.sleep(0.5)
    return None


def main():
    json_path = Path(__file__).parent / "bursa_companies.json"

    with open(json_path, "r", encoding="utf-8") as f:
        companies = json.load(f)

    print(f"Loaded {len(companies)} companies")

    # Step 1: Fetch from KLSE Screener
    klse_map = fetch_klse_screener()

    # Step 2: Match companies
    updated = 0
    not_found = []

    for comp in companies:
        code = comp["Code"]
        ticker = comp["Ticker"]
        old_name = comp.get("Full Company Name", "")

        matched = False
        for try_code in [code, code.lstrip("0")]:
            if try_code in klse_map:
                new_name = klse_map[try_code]
                comp["Full Company Name"] = new_name
                if new_name != old_name:
                    updated += 1
                matched = True
                break

        if not matched:
            not_found.append((ticker, code))

    print(f"\nUpdated {updated} companies from KLSE Screener")
    print(f"Not found: {len(not_found)}")
    for t, c in not_found:
        print(f"  {t} ({c})")

    # Step 3: Try Yahoo Finance for remaining
    if not_found:
        print(f"\nTrying Yahoo Finance for {len(not_found)} remaining...")
        yahoo_found = 0
        yahoo_failed = []

        for ticker, code in not_found:
            name = fetch_yahoo_name(ticker, code)
            if name:
                for comp in companies:
                    if comp["Code"] == code:
                        comp["Full Company Name"] = name
                        yahoo_found += 1
                        print(f"  Found: {ticker} -> {name}")
                        break
            else:
                yahoo_failed.append((ticker, code))

        print(f"  Found {yahoo_found} via Yahoo Finance")
        if yahoo_failed:
            print(f"  Still missing {len(yahoo_failed)}:")
            for t, c in yahoo_failed:
                print(f"    {t} ({c})")

    # Save
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(companies, f, ensure_ascii=False, indent=2)

    print(f"\nSaved to {json_path}")

    # Verification
    checks = {
        "MAYBANK": "Malayan Banking Berhad",
        "CIMB": "CIMB Group Holdings Berhad",
        "TENAGA": "Tenaga Nasional Bhd",
        "TOPGLOV": "Top Glove Corporation Bhd",
        "PCHEM": "Petronas Chemicals Group Berhad",
        "IHH": "IHH Healthcare Berhad",
        "AXIATA": "Axiata Group Berhad",
        "GENM": "Genting Malaysia Berhad",
        "PBBANK": "Public Bank Berhad",
        "BAT": "British American Tobacco (Malaysia) Berhad",
        "NESTLE": "Nestle (Malaysia) Berhad",
        "CAPITALA": "Capital A Berhad",
        "IOI": "IOI Corporation Berhad",
        "YTL": "YTL Corporation Berhad",
        "MISC": "MISC Berhad",
        "KLCC": "KLCC Stapled Group",
    }
    print("\nVerification:")
    all_ok = True
    for comp in companies:
        if comp["Ticker"] in checks:
            actual = comp["Full Company Name"]
            expected = checks[comp["Ticker"]]
            ok = actual == expected
            if not ok:
                all_ok = False
            mark = "OK" if ok else "MISMATCH"
            print(f"  [{mark}] {comp['Ticker']:12s} -> {actual}")
            if not ok:
                print(f"  {'':12s}    expected: {expected}")

    if all_ok:
        print("\n  All checks passed!")


if __name__ == "__main__":
    main()
