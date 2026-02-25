"""
Extract TOP 20 VENDORS BY CLASS data from NABCA PDFs
"""

import pdfplumber
import csv
import os
import re
from pathlib import Path

# Known classes
CLASSES = [
    'DOM WHSKY', 'SCOTCH', 'CANADIAN', 'IRISH', 'OTH IMP WHSKY',
    'GIN', 'VODKA', 'RUM', 'BRANDY/COGNAC', 'COCKTAILS',
    'CORDIALS', 'TEQUILA', 'MEZCAL'
]

# PDF files with their report periods
PDF_FILES = [
    ('631_9L_0125.PDF', 2025, 1),
    ('631_9L_0225.PDF', 2025, 2),
    ('631_9L_0325.PDF', 2025, 3),
    ('631_9L_0425.PDF', 2025, 4),
    ('631_9L_0525.PDF', 2025, 5),
    ('631_9L_0625.PDF', 2025, 6),
    ('631_9L_0724.PDF', 2024, 7),
    ('631_9L_0725.PDF', 2025, 7),
    ('631_9L_0824.PDF', 2024, 8),
    ('631_9L_0825.PDF', 2025, 8),
    ('631_9L_0924.PDF', 2024, 9),
    ('631_9L_0925.PDF', 2025, 9),
    ('631_9L_1024.PDF', 2024, 10),
    ('631_9L_1025.PDF', 2025, 10),
    ('631_9L_1124.PDF', 2024, 11),
    ('631_9L_1125.PDF', 2025, 11),
    ('631_9L_1224.PDF', 2024, 12),
    ('631_9L_1225.PDF', 2025, 12),
]

def parse_number(val):
    """Parse a number, handling commas and negatives"""
    if not val:
        return None
    val = str(val).strip().replace(',', '')
    if not val or val == '-':
        return None
    try:
        # Handle negative percentages like -.11
        if val.startswith('-.'):
            val = '-0.' + val[2:]
        elif val.startswith('.'):
            val = '0' + val
        return float(val)
    except:
        return None

def find_top20_pages(pdf):
    """Find pages containing TOP 20 VENDORS BY CLASS"""
    pages = []
    for i, page in enumerate(pdf.pages):
        if i < 300 or i > 450:  # Limit search range
            continue
        text = page.extract_text() or ''
        if 'TOP 20' in text.upper() and 'BY CLASS' in text.upper():
            pages.append(i)
    return pages

def extract_from_pdf(pdf_path, report_year, report_month):
    """Extract TOP 20 VENDORS BY CLASS data from a PDF"""
    records = []

    with pdfplumber.open(pdf_path) as pdf:
        # Find relevant pages
        start_pages = find_top20_pages(pdf)
        if not start_pages:
            print(f"  Warning: Could not find TOP 20 VENDORS BY CLASS in {pdf_path}")
            return records

        start_page = start_pages[0]
        print(f"  Found TOP 20 VENDORS BY CLASS starting at page {start_page + 1}")

        current_class = None

        # Process pages (usually spans about 7-8 pages)
        for page_num in range(start_page, min(start_page + 10, len(pdf.pages))):
            page = pdf.pages[page_num]
            tables = page.extract_tables()

            if not tables:
                continue

            for table in tables:
                for row in table:
                    if not row or len(row) < 4:
                        continue

                    # Get first two columns
                    col0 = str(row[0]).strip() if row[0] else ''
                    col1 = str(row[1]).strip() if row[1] else ''

                    # Skip header rows
                    if 'Class / Vendor' in col0 or 'Rank' in col1:
                        continue

                    # Check if this is bundled data (contains newlines)
                    if '\n' in col0:
                        # Split all columns by newline
                        names = col0.split('\n')
                        ranks = col1.split('\n') if col1 else [''] * len(names)

                        # Get other columns
                        shares = str(row[2]).split('\n') if row[2] else [''] * len(names)
                        l12m_this = str(row[3]).split('\n') if row[3] else [''] * len(names)
                        l12m_prior = str(row[4]).split('\n') if len(row) > 4 and row[4] else [''] * len(names)
                        l12m_chg = str(row[5]).split('\n') if len(row) > 5 and row[5] else [''] * len(names)
                        ytd_this = str(row[6]).split('\n') if len(row) > 6 and row[6] else [''] * len(names)
                        ytd_last = str(row[7]).split('\n') if len(row) > 7 and row[7] else [''] * len(names)
                        ytd_chg = str(row[8]).split('\n') if len(row) > 8 and row[8] else [''] * len(names)
                        cm_this = str(row[9]).split('\n') if len(row) > 9 and row[9] else [''] * len(names)
                        cm_last = str(row[10]).split('\n') if len(row) > 10 and row[10] else [''] * len(names)
                        cm_chg = str(row[11]).split('\n') if len(row) > 11 and row[11] else [''] * len(names)

                        # Process each item
                        for i, name in enumerate(names):
                            name = name.strip()
                            if not name:
                                continue

                            rank = ranks[i].strip() if i < len(ranks) else ''

                            # Check if this is a class row (no rank)
                            if name in CLASSES and not rank:
                                current_class = name
                                continue

                            # This is a vendor row
                            if current_class and rank:
                                try:
                                    record = {
                                        'class': current_class,
                                        'vendor_name': name,
                                        'rank': int(rank),
                                        'market_share': parse_number(shares[i] if i < len(shares) else ''),
                                        'l12m_cases_this_year': int(parse_number(l12m_this[i]) or 0) if i < len(l12m_this) else None,
                                        'l12m_cases_prior_year': int(parse_number(l12m_prior[i]) or 0) if i < len(l12m_prior) else None,
                                        'l12m_change_pct': parse_number(l12m_chg[i] if i < len(l12m_chg) else ''),
                                        'ytd_cases_this_year': int(parse_number(ytd_this[i]) or 0) if i < len(ytd_this) else None,
                                        'ytd_cases_last_year': int(parse_number(ytd_last[i]) or 0) if i < len(ytd_last) else None,
                                        'ytd_change_pct': parse_number(ytd_chg[i] if i < len(ytd_chg) else ''),
                                        'curr_month_this_year': int(parse_number(cm_this[i]) or 0) if i < len(cm_this) else None,
                                        'curr_month_last_year': int(parse_number(cm_last[i]) or 0) if i < len(cm_last) else None,
                                        'curr_month_change_pct': parse_number(cm_chg[i] if i < len(cm_chg) else ''),
                                        'report_year': report_year,
                                        'report_month': report_month
                                    }
                                    records.append(record)
                                except Exception as e:
                                    print(f"    Error parsing vendor {name}: {e}")
                    else:
                        # Single row - check if class
                        if col0 in CLASSES and not col1:
                            current_class = col0
                        elif current_class and col1:
                            # Vendor row
                            try:
                                record = {
                                    'class': current_class,
                                    'vendor_name': col0,
                                    'rank': int(col1),
                                    'market_share': parse_number(row[2] if len(row) > 2 else ''),
                                    'l12m_cases_this_year': int(parse_number(row[3]) or 0) if len(row) > 3 else None,
                                    'l12m_cases_prior_year': int(parse_number(row[4]) or 0) if len(row) > 4 else None,
                                    'l12m_change_pct': parse_number(row[5] if len(row) > 5 else ''),
                                    'ytd_cases_this_year': int(parse_number(row[6]) or 0) if len(row) > 6 else None,
                                    'ytd_cases_last_year': int(parse_number(row[7]) or 0) if len(row) > 7 else None,
                                    'ytd_change_pct': parse_number(row[8] if len(row) > 8 else ''),
                                    'curr_month_this_year': int(parse_number(row[9]) or 0) if len(row) > 9 else None,
                                    'curr_month_last_year': int(parse_number(row[10]) or 0) if len(row) > 10 else None,
                                    'curr_month_change_pct': parse_number(row[11] if len(row) > 11 else ''),
                                    'report_year': report_year,
                                    'report_month': report_month
                                }
                                records.append(record)
                            except Exception as e:
                                print(f"    Error parsing: {e}")

    return records

def main():
    # Setup paths
    base_dir = Path(__file__).parent.parent
    pdf_dir = base_dir
    output_dir = Path(__file__).parent / 'output'
    output_dir.mkdir(exist_ok=True)

    all_records = []

    print("Extracting TOP 20 VENDORS BY CLASS data...")
    print("=" * 60)

    for pdf_name, year, month in PDF_FILES:
        pdf_path = pdf_dir / pdf_name
        if not pdf_path.exists():
            print(f"Skipping {pdf_name} - file not found")
            continue

        print(f"\nProcessing {pdf_name} ({year}-{month:02d})...")
        records = extract_from_pdf(pdf_path, year, month)
        print(f"  Extracted {len(records)} vendor records")

        # Count by class
        class_counts = {}
        for r in records:
            c = r['class']
            class_counts[c] = class_counts.get(c, 0) + 1
        for c, cnt in sorted(class_counts.items()):
            print(f"    {c}: {cnt} vendors")

        all_records.extend(records)

    print("\n" + "=" * 60)
    print(f"Total records extracted: {len(all_records)}")

    # Save to CSV
    csv_path = output_dir / 'top20_vendors_by_class.csv'
    if all_records:
        fieldnames = list(all_records[0].keys())
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_records)
        print(f"\nSaved to {csv_path}")

    # Generate SQL bulk inserts
    sql_dir = output_dir / 'top20_vendors_bulk'
    sql_dir.mkdir(exist_ok=True)

    # Split into files by report period
    records_by_period = {}
    for r in all_records:
        key = f"{r['report_year']}_{r['report_month']:02d}"
        if key not in records_by_period:
            records_by_period[key] = []
        records_by_period[key].append(r)

    for period, recs in sorted(records_by_period.items()):
        sql_path = sql_dir / f"bulk_{period}.sql"
        with open(sql_path, 'w', encoding='utf-8') as f:
            f.write(f"-- Bulk insert for {period} ({len(recs)} records)\n")
            f.write("INSERT INTO new_nabca.raw_top20_vendors_by_class (\n")
            f.write("  class, vendor_name, rank, market_share,\n")
            f.write("  l12m_cases_this_year, l12m_cases_prior_year, l12m_change_pct,\n")
            f.write("  ytd_cases_this_year, ytd_cases_last_year, ytd_change_pct,\n")
            f.write("  curr_month_this_year, curr_month_last_year, curr_month_change_pct,\n")
            f.write("  report_year, report_month\n")
            f.write(") VALUES\n")

            values = []
            for r in recs:
                vendor = r['vendor_name'].replace("'", "''")
                val = f"('{r['class']}', '{vendor}', {r['rank']}, "
                val += f"{r['market_share'] if r['market_share'] is not None else 'NULL'}, "
                val += f"{r['l12m_cases_this_year'] if r['l12m_cases_this_year'] else 'NULL'}, "
                val += f"{r['l12m_cases_prior_year'] if r['l12m_cases_prior_year'] else 'NULL'}, "
                val += f"{r['l12m_change_pct'] if r['l12m_change_pct'] is not None else 'NULL'}, "
                val += f"{r['ytd_cases_this_year'] if r['ytd_cases_this_year'] else 'NULL'}, "
                val += f"{r['ytd_cases_last_year'] if r['ytd_cases_last_year'] else 'NULL'}, "
                val += f"{r['ytd_change_pct'] if r['ytd_change_pct'] is not None else 'NULL'}, "
                val += f"{r['curr_month_this_year'] if r['curr_month_this_year'] else 'NULL'}, "
                val += f"{r['curr_month_last_year'] if r['curr_month_last_year'] else 'NULL'}, "
                val += f"{r['curr_month_change_pct'] if r['curr_month_change_pct'] is not None else 'NULL'}, "
                val += f"{r['report_year']}, {r['report_month']})"
                values.append(val)

            f.write(',\n'.join(values))
            f.write(';\n')
        print(f"Generated {sql_path}")

if __name__ == '__main__':
    main()
