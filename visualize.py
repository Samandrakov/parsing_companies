"""
Visualization module for ASEAN market structure analysis.
Generates charts for dissertation on Russia-ASEAN cooperation.
"""

import json
import sys
import os
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

sys.stdout.reconfigure(encoding='utf-8')

from isic_mapper import ISIC_SECTIONS
from trade_russia import BASELINE_TRADE_DATA

# Color scheme
COLORS = {
    'A': '#2d6a4f', 'B': '#6b4226', 'C': '#1d3557', 'D': '#e9c46a',
    'F': '#e76f51', 'G': '#264653', 'H': '#2a9d8f', 'I': '#f4a261',
    'J': '#457b9d', 'K': '#6a0572', 'L': '#bc4749', 'M': '#a8dadc',
    'P': '#606c38', 'Q': '#d62828', 'R': '#f77f00', 'S': '#adb5bd',
}

COUNTRY_ORDER = ['Indonesia', 'Malaysia', 'Thailand', 'Vietnam', 'Singapore',
                 'Philippines', 'Myanmar', 'Cambodia', 'Laos']

OUTPUT_DIR = 'charts'


def load_consolidated():
    with open('asean_consolidated.json', 'r', encoding='utf-8') as f:
        return json.load(f)


def chart_companies_per_country(data):
    """Bar chart: number of listed companies per ASEAN country."""
    counts = {}
    for c in data:
        country = c['Country']
        counts[country] = counts.get(country, 0) + 1

    countries = [c for c in COUNTRY_ORDER if c in counts]
    values = [counts[c] for c in countries]

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.bar(countries, values, color='#1d3557', edgecolor='white', linewidth=0.5)

    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 15,
                str(val), ha='center', va='bottom', fontsize=10, fontweight='bold')

    ax.set_ylabel('Number of Listed Companies', fontsize=11)
    ax.set_title('Listed Companies on ASEAN Stock Exchanges', fontsize=14, fontweight='bold')
    ax.set_ylim(0, max(values) * 1.15)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.xticks(rotation=30, ha='right')
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/01_companies_per_country.png', dpi=150)
    plt.close()
    print("Saved 01_companies_per_country.png")


def chart_market_structure_stacked(data):
    """Stacked bar chart: ISIC sector composition per country (%)."""
    # Build country -> section -> count
    matrix = {}
    totals = {}
    for c in data:
        country = c['Country']
        isic = c.get('ISIC_Section', '')
        if not isic:
            isic = 'Other'
        if country not in matrix:
            matrix[country] = {}
            totals[country] = 0
        matrix[country][isic] = matrix[country].get(isic, 0) + 1
        totals[country] += 1

    countries = [c for c in COUNTRY_ORDER if c in matrix]

    # Top sections by total count
    all_sections = {}
    for c in matrix.values():
        for s, cnt in c.items():
            all_sections[s] = all_sections.get(s, 0) + cnt
    top_sections = sorted(all_sections, key=lambda x: -all_sections[x])[:10]

    fig, ax = plt.subplots(figsize=(12, 6))
    x = np.arange(len(countries))
    width = 0.7
    bottom = np.zeros(len(countries))

    for section in top_sections:
        values = []
        for c in countries:
            cnt = matrix.get(c, {}).get(section, 0)
            total = totals.get(c, 1)
            values.append(cnt / total * 100)
        label = f"{section} - {ISIC_SECTIONS.get(section, section)[:35]}"
        color = COLORS.get(section, '#cccccc')
        ax.bar(x, values, width, bottom=bottom, label=label, color=color, edgecolor='white', linewidth=0.3)
        bottom += np.array(values)

    ax.set_ylabel('Share of Listed Companies (%)', fontsize=11)
    ax.set_title('ASEAN Market Structure by ISIC Sector', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(countries, rotation=30, ha='right')
    ax.set_ylim(0, 105)
    ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), fontsize=8, frameon=False)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/02_market_structure_stacked.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("Saved 02_market_structure_stacked.png")


def chart_trade_volume(data):
    """Horizontal bar chart: Russia-ASEAN trade volume by country."""
    countries = []
    exports = []
    imports = []

    for country in reversed(COUNTRY_ORDER):
        trade = BASELINE_TRADE_DATA.get(country)
        if trade:
            countries.append(country)
            exports.append(trade['russia_exports_mln_usd'])
            imports.append(trade['russia_imports_mln_usd'])

    fig, ax = plt.subplots(figsize=(10, 5))
    y = np.arange(len(countries))
    height = 0.35

    ax.barh(y + height / 2, exports, height, label='Russia Exports', color='#e63946')
    ax.barh(y - height / 2, imports, height, label='Russia Imports', color='#457b9d')

    ax.set_xlabel('Trade Volume ($ million)', fontsize=11)
    ax.set_title('Russia-ASEAN Bilateral Trade', fontsize=14, fontweight='bold')
    ax.set_yticks(y)
    ax.set_yticklabels(countries)
    ax.legend(loc='lower right')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/03_russia_asean_trade.png', dpi=150)
    plt.close()
    print("Saved 03_russia_asean_trade.png")


def chart_cooperation_potential(data):
    """Bubble chart: market size (companies) vs trade volume, by country."""
    fig, ax = plt.subplots(figsize=(10, 7))

    company_counts = {}
    for c in data:
        country = c['Country']
        company_counts[country] = company_counts.get(country, 0) + 1

    for country in COUNTRY_ORDER:
        trade = BASELINE_TRADE_DATA.get(country)
        if not trade or country not in company_counts:
            continue
        x = company_counts[country]
        y = trade['trade_volume_mln_usd']
        size = y / 10  # Scale bubble size

        ax.scatter(x, y, s=size, alpha=0.7, edgecolors='black', linewidth=0.5)
        ax.annotate(country, (x, y), textcoords="offset points",
                    xytext=(8, 5), fontsize=10, fontweight='bold')

    ax.set_xlabel('Listed Companies on Stock Exchange', fontsize=11)
    ax.set_ylabel('Trade Volume with Russia ($ million)', fontsize=11)
    ax.set_title('Cooperation Potential: Market Depth vs Trade Volume',
                 fontsize=14, fontweight='bold')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/04_cooperation_potential.png', dpi=150)
    plt.close()
    print("Saved 04_cooperation_potential.png")


def chart_sector_trade_heatmap(data):
    """Heatmap: ISIC sectors that overlap between market structure and Russia trade."""
    countries = [c for c in COUNTRY_ORDER if c in BASELINE_TRADE_DATA]

    # Market structure: % of companies per ISIC section
    market = {}
    totals = {}
    for c in data:
        country = c['Country']
        isic = c.get('ISIC_Section', '')
        if country not in market:
            market[country] = {}
            totals[country] = 0
        if isic:
            market[country][isic] = market[country].get(isic, 0) + 1
        totals[country] += 1

    # Trade sectors per country
    trade_sectors = {}
    for country, tdata in BASELINE_TRADE_DATA.items():
        trade_sectors[country] = set()
        for item in tdata.get('top_russia_exports', []) + tdata.get('top_russia_imports', []):
            trade_sectors[country].add(item['sector'])

    # Key sections
    sections = ['A', 'B', 'C', 'D', 'F', 'H', 'J', 'K', 'L', 'Q']

    # Build matrix: 2 = both market + trade, 1 = market only, 0.5 = trade only, 0 = neither
    matrix = []
    for country in countries:
        row = []
        for s in sections:
            has_market = market.get(country, {}).get(s, 0) > 0
            has_trade = s in trade_sectors.get(country, set())
            if has_market and has_trade:
                row.append(2)
            elif has_market:
                row.append(1)
            elif has_trade:
                row.append(0.5)
            else:
                row.append(0)
        matrix.append(row)

    matrix = np.array(matrix)

    fig, ax = plt.subplots(figsize=(12, 6))
    from matplotlib.colors import ListedColormap
    cmap = ListedColormap(['#f8f9fa', '#adb5bd', '#fca311', '#2d6a4f'])
    im = ax.imshow(matrix, cmap=cmap, aspect='auto', vmin=0, vmax=2)

    section_labels = [f"{s}\n{ISIC_SECTIONS.get(s, '?')[:20]}" for s in sections]
    ax.set_xticks(np.arange(len(sections)))
    ax.set_xticklabels(section_labels, fontsize=8, rotation=45, ha='right')
    ax.set_yticks(np.arange(len(countries)))
    ax.set_yticklabels(countries, fontsize=10)
    ax.set_title('Market Structure × Russia Trade Overlap by ISIC Sector',
                 fontsize=13, fontweight='bold')

    # Add text annotations
    for i in range(len(countries)):
        for j in range(len(sections)):
            val = matrix[i, j]
            if val == 2:
                text = 'Both'
                color = 'white'
            elif val == 1:
                text = 'Mkt'
                color = 'black'
            elif val == 0.5:
                text = 'Trade'
                color = 'black'
            else:
                text = ''
                color = 'black'
            ax.text(j, i, text, ha='center', va='center', fontsize=7, color=color)

    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/05_sector_trade_heatmap.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("Saved 05_sector_trade_heatmap.png")


def chart_sgx_country_pie():
    """Pie chart: SGX companies by real country of origin (hypothesis test)."""
    from filter_country import analyze_company

    with open('Singapore/SGX/output_data_enriched.json', 'r', encoding='utf-8') as f:
        data = json.load(f)

    countries = {}
    for c in data:
        analysis = analyze_company(c)
        real = analysis['real_country'] or 'UNKNOWN'
        if real not in ('SINGAPORE', 'UNKNOWN'):
            # Group small non-SG countries
            if countries.get(real, 0) < 5:
                real_label = real
            else:
                real_label = real
        else:
            real_label = real
        countries[real_label] = countries.get(real_label, 0) + 1

    # Group very small slices as "Others"
    threshold = 5
    grouped = {}
    others = 0
    for c, cnt in countries.items():
        if cnt < threshold and c != 'SINGAPORE':
            others += cnt
        else:
            grouped[c] = cnt
    if others > 0:
        grouped['Others'] = others

    labels = sorted(grouped.keys(), key=lambda x: -grouped[x])
    sizes = [grouped[l] for l in labels]
    colors_list = ['#1d3557', '#457b9d', '#e63946', '#f4a261', '#2a9d8f',
                   '#e9c46a', '#264653', '#bc4749', '#adb5bd']

    fig, ax = plt.subplots(figsize=(8, 8))
    wedges, texts, autotexts = ax.pie(
        sizes, labels=labels, autopct='%1.1f%%', startangle=90,
        colors=colors_list[:len(labels)], textprops={'fontsize': 9}
    )
    ax.set_title('SGX: Companies by Country of Origin\n(Testing the "domestic listing" hypothesis)',
                 fontsize=13, fontweight='bold')
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/06_sgx_country_origin.png', dpi=150)
    plt.close()
    print("Saved 06_sgx_country_origin.png")


def generate_all():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    data = load_consolidated()
    print(f"Loaded {len(data)} companies\n")

    chart_companies_per_country(data)
    chart_market_structure_stacked(data)
    chart_trade_volume(data)
    chart_cooperation_potential(data)
    chart_sector_trade_heatmap(data)
    chart_sgx_country_pie()

    print(f"\nAll charts saved to {OUTPUT_DIR}/")


if __name__ == '__main__':
    generate_all()
