"""
Universal JSON to CSV converter for all ASEAN exchange data.
Works with output from any country parser.
"""

import json
import pandas as pd
import re
import argparse
from typing import Any


def clean_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return re.sub(r'\s+', ' ', value.strip())
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def json_to_csv(input_file: str, output_file: str, priority_columns: list = None):
    """Convert JSON company data to CSV with optional column ordering."""

    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if not data:
        print("Empty JSON file")
        return

    # Flatten nested structures
    flat_data = []
    for company in data:
        flat = {}
        for key, value in company.items():
            if key.startswith('_'):
                continue  # Skip internal fields
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    flat[f"{key}_{sub_key}"] = clean_value(sub_value)
            elif isinstance(value, list):
                flat[key] = "; ".join(str(item) for item in value)
            else:
                flat[key] = clean_value(value)
        flat_data.append(flat)

    df = pd.DataFrame(flat_data)

    # Reorder columns: priority first, then the rest
    if priority_columns is None:
        priority_columns = [
            'Ticker', 'Full Company Name', 'Company_name',
            'ISIN Code', 'ISIN_Code',
            'Incorporated in', 'Real_Country', 'Is_ASEAN',
            'Sector', 'Sector_YF', 'Sector_CSV', 'Sector_JSON',
            'Industry', 'Industry_YF', 'Industry_CSV', 'Industry_JSON',
            'Market_Cap', 'Stock_Price', 'Stock_price',
            'Revenue', 'Net_Income', 'Free_Cash_Flow',
            'P/E_Ratio', 'P/B_Ratio', 'ROE', 'ROA',
            'Dividend_Yield', 'Profit_Margin',
            'Revenue_Growth', 'Volume',
            'Registered Office', 'Registered_Office',
            'Telephone', 'Fax', 'Email',
            'Link to Internet Website', 'Website',
            'Exchange',
        ]

    existing_priority = [c for c in priority_columns if c in df.columns]
    other_cols = [c for c in df.columns if c not in existing_priority]
    df = df[existing_priority + other_cols]

    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"Saved {len(df)} companies to {output_file}")
    print(f"Columns ({len(df.columns)}): {list(df.columns[:10])}...")

    # Print completeness stats
    print(f"\nData completeness:")
    for col in df.columns[:15]:
        filled = df[col].notna().sum()
        filled = sum(1 for v in df[col] if str(v).strip() not in ['', 'None', 'nan', 'N/A'])
        pct = filled / len(df) * 100
        print(f"  {col}: {filled}/{len(df)} ({pct:.0f}%)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert JSON company data to CSV')
    parser.add_argument('input_file', help='Input JSON file')
    parser.add_argument('-o', '--output', help='Output CSV file (default: input.csv)')
    args = parser.parse_args()

    output = args.output or args.input_file.rsplit('.', 1)[0] + '.csv'
    json_to_csv(args.input_file, output)
