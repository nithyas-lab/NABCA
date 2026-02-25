"""
Extract By Class - YEAR TO DATE
================================
Downloads PDFs and extracts YTD section using pdfplumber.
Pages 7-8 for most months, pages 5-6 for July 2025.

Usage:
    python ytd.py                    # Extract all months to CSV only
    python ytd.py --upload           # Extract all months and upload to Supabase
    python ytd.py 2025-12            # Extract specific month(s)
    python ytd.py 2025-12 --upload   # Extract and upload specific month
"""

import boto3
import pdfplumber
import io
import re
import os
import csv
import sys

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, will use system environment variables

# AWS Configuration - Load from environment variables
# Set these in your environment or create a .env file (see .env.example)
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("AWS_BUCKET", "nabca-data")

if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
    raise ValueError("AWS_ACCESS_KEY and AWS_SECRET_KEY must be set in environment variables")

s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION
)

# PDF to date mapping - ALL MONTHS
PDF_DATE_MAP = {
    "631_9L_0724.PDF": (2024, 7),
    "631_9L_0824.PDF": (2024, 8),
    "631_9L_0924.PDF": (2024, 9),
    "631_9L_1024.PDF": (2024, 10),
    "631_9L_1124.PDF": (2024, 11),
    "631_9L_1224.PDF": (2024, 12),
    "631_9L_0125.pdf": (2025, 1),
    "631_9L_0225.PDF": (2025, 2),
    "631_9L_0325.PDF": (2025, 3),
    "631_9L_0425.PDF": (2025, 4),
    "631_9L_0525.PDF": (2025, 5),
    "631_9L_0625.PDF": (2025, 6),
    "631_9L_0725.PDF": (2025, 7),
    "631_9L_0825.PDF": (2025, 8),
    "631_9L_0925.PDF": (2025, 9),
    "631_9L_1025.PDF": (2025, 10),
    "631_9L_1125.PDF": (2025, 11),
    "631_9L_1225.PDF": (2025, 12),
}

# Known class names for validation
KNOWN_CLASSES = [
    "DOM WHSKY-BLND", "DOM WHSKY-SNGL MALT", "DOM WHSKY-STRT-BRBN/TN",
    "DOM WHSKY-STRT-OTH", "DOM WHSKY-STRT-RYE", "DOM WHSKY-STRT-SM BTCH",
    "TOTAL DOM WHSKY", "SCOTCH-BLND-FRGN BTLD", "SCOTCH-BLND-US BTLD",
    "SCOTCH-SNGL MALT", "TOTAL SCOTCH", "CAN-FRGN BLND-FRGN BTLD",
    "CAN-US BLND-US BTLD", "CAN-US BTLD", "TOTAL CAN",
    "IRISH", "IRISH-BLND", "IRISH-SNGL MALT", "TOTAL IRISH",
    "OTH IMP WHSKY", "OTH IMP WHSKY-BLND", "OTH IMP WHSKY-SNGL MALT",
    "TOTAL OTH IMP WHSKY", "GIN-CLASSIC-DOM", "GIN-CLASSIC-IMP",
    "GIN-CONTEMP-DOM", "GIN-CONTEMP-IMP", "TOTAL GIN",
    "NEUTRAL GRAIN SPIRIT", "VODKA-CLASSIC-DOM", "VODKA-CLASSIC-IMP",
    "VODKA-FLVRD-DOM", "VODKA-FLVRD-IMP", "TOTAL VODKA",
    "CACHACA", "RUM-AGED/DARK", "RUM-FLVRD", "RUM-WHITE", "RUM-LIGHT",
    "TOTAL RUM", "BRANDY-APPLE/FRUIT", "BRANDY-GRAPE-DOM",
    "BRANDY-GRAPE-IMP", "TOTAL BRANDY", "COGNAC-VS", "COGNAC-VSOP",
    "COGNAC-XO", "TOTAL COGNAC", "TEQUILA-ANEJO", "TEQUILA-BLANCO",
    "TEQUILA-CRISTALINO", "TEQUILA-GOLD", "TEQUILA-MEZCAL",
    "TEQUILA-REPOSADO", "TOTAL TEQUILA", "COCKTAILS", "TOTAL COCKTAILS",
    "CRDL-COFFEE LQR", "CRDL-CRM LQR", "CRDL-CRM LQR-IRISH STYLE",
    "CRDL-CRM LQR-OTH STYLE", "CRDL-FLVRD BRNDES-DOM",
    "CRDL-FLVRD BRNDES-OTH", "CRDL-FLVRD BRNDES-APRCT",
    "CRDL-FLVRD BRNDES-BLKBRY", "CRDL-FLVRD BRNDES-COF",
    "CRDL-FLVRD BRNDES-PEACH", "CRDL-FLVRD-DOM", "CRDL-FLVRD-IMP",
    "CRDL-LQR&SPC-AMARETTO", "CRDL-LQR&SPC-ANISE", "CRDL-LQR&SPC-BITTERS",
    "CRDL-LQR&SPC-CRM", "CRDL-LQR&SPC-FRUIT", "CRDL-LQR&SPC-HERBAL",
    "CRDL-LQR&SPC-OTH", "CRDL-LQR&SPC-TRIPLE SEC/CURACAO",
    "CRDL-LQR&SPC-WHSKY", "CRDL-LQR&SPC-AMRT", "CRDL-LQR&SPC-ANSE FLVRD",
    "SPCTY", "CRDL-ROCK & RYE", "CRDL-SNPS-CINNAMON", "CRDL-SNPS-OTH",
    "CRDL-SNPS-PEACH", "CRDL-SNPS-ROOTBEER", "TOTAL CRDL", "TOTAL SPIRITS"
]

# Parent class mapping
def get_parent_class(class_name):
    """Determine parent class based on class name"""
    cn = class_name.upper()

    if cn.startswith('DOM WHSKY') or cn == 'TOTAL DOM WHSKY':
        return 'DOM WHSKY'
    elif cn.startswith('SCOTCH') or cn == 'TOTAL SCOTCH':
        return 'SCOTCH'
    elif cn.startswith('CAN-') or cn == 'TOTAL CAN' or cn == 'TOTAL CANADIAN':
        return 'CANADIAN'
    elif cn.startswith('IRISH') or cn == 'TOTAL IRISH':
        return 'IRISH'
    elif cn.startswith('OTH IMP WHSKY') or cn == 'TOTAL OTH IMP WHSKY':
        return 'OTH IMP WHSKY'
    elif cn.startswith('GIN-') or cn == 'TOTAL GIN':
        return 'GIN'
    elif cn.startswith('VODKA') or cn == 'TOTAL VODKA':
        return 'VODKA'
    elif cn == 'NEUTRAL GRAIN SPIRIT':
        return 'VODKA'
    elif cn.startswith('RUM-') or cn == 'TOTAL RUM':
        return 'RUM'
    elif cn == 'CACHACA':
        return 'RUM'
    elif cn.startswith('BRNDY/CGNC') or 'BRANDY/COGNAC' in cn or cn == 'TOTAL BRANDY':
        return 'BRANDY/COGNAC'
    elif cn.startswith('COGNAC') or cn == 'TOTAL COGNAC':
        return 'BRANDY/COGNAC'
    elif cn.startswith('TEQUILA') or cn == 'TOTAL TEQUILA':
        return 'TEQUILA'
    elif cn.startswith('MEZCAL') or cn == 'TOTAL MEZCAL':
        return 'MEZCAL'
    elif cn.startswith('CRDL-') or cn == 'TOTAL CRDL' or cn == 'TOTAL CORDIALS':
        return 'CORDIALS'
    elif cn == 'COCKTAILS' or cn == 'TOTAL COCKTAILS':
        return 'COCKTAILS'
    elif cn == 'SPCTY':
        return 'CORDIALS'
    else:
        return None

def parse_class_line(line, year, month):
    """Parse a line of class data using pattern matching"""
    line = line.strip()
    if not line or len(line) < 10:
        return None

    # Skip header/footer lines
    skip_patterns = ['CLASS', 'NABCA', 'BY CLASS', '%Total', 'Bottle',
                     'PAGE', 'ACBAN', 'thgirypoC', '1.75 L', '% of',
                     'YEAR TO DATE', 'TOTAL CASE', 'ROLLING', 'CURRENT']
    for pattern in skip_patterns:
        if pattern in line:
            return None

    # Try to match known class names
    best_match = None
    best_match_len = 0

    for class_name in KNOWN_CLASSES:
        if line.upper().startswith(class_name):
            if len(class_name) > best_match_len:
                best_match = class_name
                best_match_len = len(class_name)

    if not best_match:
        # Try to find class name by looking for number pattern start
        parts = line.split()
        if len(parts) < 3:
            return None

        # Find where numbers start
        class_parts = []
        num_start = -1
        for i, part in enumerate(parts):
            clean = part.replace(',', '').replace('.', '').replace('-', '')
            if clean.isdigit():
                num_start = i
                break
            class_parts.append(part)

        if num_start < 1:
            return None

        best_match = ' '.join(class_parts)
        rest = ' '.join(parts[num_start:])
    else:
        # Extract the rest after class name
        rest = line[best_match_len:].strip()

    # Parse numbers from rest of line
    # Pattern matches: "123", "123.45", ".45", "1,234", "1,234.56"
    numbers = re.findall(r'\d[\d,]*\.?\d*|\.\d+', rest)
    if len(numbers) < 2:
        return None

    # Clean and convert numbers
    cleaned = []
    for n in numbers:
        try:
            n_clean = n.replace(',', '')
            if '.' in n_clean:
                cleaned.append(float(n_clean))
            else:
                cleaned.append(int(n_clean))
        except:
            pass

    if len(cleaned) < 2:
        return None

    # Build record
    record = {
        'class_name': best_match,
        'parent_class': get_parent_class(best_match),
        'pct_total_dist_spirits': None,  # Will be populated for TOTAL rows
        'report_year': year,
        'report_month': month
    }

    # Check if this is a TOTAL row
    is_total = best_match.upper().startswith('TOTAL')

    # Assign numbers
    idx = 0

    if is_total and len(cleaned) >= 2:
        # TOTAL rows: pct_total_dist_spirits pct_of_class total_cases bottle_sizes...
        # Example: "TOTAL DOM WHSKY 18.60 100.00 10667672..."
        if isinstance(cleaned[0], float) and cleaned[0] < 100:
            record['pct_total_dist_spirits'] = float(cleaned[0])
            idx += 1
        if idx < len(cleaned):
            val = cleaned[idx]
            if isinstance(val, float) or val < 200:
                record['pct_of_class'] = float(val)
                idx += 1
            else:
                record['pct_of_class'] = None
    else:
        # Regular subcategory rows: pct total_cases bottle_sizes...
        if idx < len(cleaned):
            val = cleaned[idx]
            if isinstance(val, float) or val < 100:  # Likely percentage
                record['pct_of_class'] = float(val)
                idx += 1
            else:
                record['pct_of_class'] = None

    # Parse case counts (same for both TOTAL and regular rows)
    if idx < len(cleaned):
        record['ytd_total_cases'] = int(cleaned[idx]) if cleaned[idx] else None
        idx += 1
    if idx < len(cleaned):
        record['ytd_cases_1_75l'] = int(cleaned[idx]) if cleaned[idx] else None
        idx += 1
    if idx < len(cleaned):
        record['ytd_cases_1_0l'] = int(cleaned[idx]) if cleaned[idx] else None
        idx += 1
    if idx < len(cleaned):
        record['ytd_cases_750ml'] = int(cleaned[idx]) if cleaned[idx] else None
        idx += 1
    if idx < len(cleaned):
        record['ytd_cases_750ml_traveler'] = int(cleaned[idx]) if cleaned[idx] else None
        idx += 1
    if idx < len(cleaned):
        record['ytd_cases_375ml'] = int(cleaned[idx]) if cleaned[idx] else None
        idx += 1
    if idx < len(cleaned):
        record['ytd_cases_200ml'] = int(cleaned[idx]) if cleaned[idx] else None
        idx += 1
    if idx < len(cleaned):
        record['ytd_cases_100ml'] = int(cleaned[idx]) if cleaned[idx] else None
        idx += 1
    if idx < len(cleaned):
        record['ytd_cases_50ml'] = int(cleaned[idx]) if cleaned[idx] else None
        idx += 1

    return record

def extract_from_pdf(pdf_key, year, month):
    """Extract By Class YEAR TO DATE data

    Page locations:
    - July 2025: pages 5-6
    - Other months: pages 7-8
    """
    # July 2025 special case
    if year == 2025 and month == 7:
        page_range = range(4, 7)  # Pages 5-7 (0-indexed: 4-6)
        print(f"  Downloading pages 5-6 from {pdf_key}...")
    else:
        page_range = range(6, 10)  # Pages 7-10 (0-indexed: 6-9)
        print(f"  Downloading pages 7-8 from {pdf_key}...")

    # Download PDF
    response = s3.get_object(Bucket=BUCKET, Key=pdf_key)
    pdf_bytes = response['Body'].read()

    records = []
    in_ytd_section = False

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        # Process appropriate page range based on month
        for page_num in page_range:
            if page_num >= len(pdf.pages):
                continue

            page = pdf.pages[page_num]
            text = page.extract_text()

            if not text:
                continue

            lines = text.split('\n')

            for line in lines:
                # Check section markers
                if "YEAR TO DATE" in line and "TOTAL CASE SALES" in line:
                    in_ytd_section = True
                    continue
                if "ROLLING 12 MONTH" in line and "CASE SALES" in line:
                    in_ytd_section = False
                    break  # Stop processing this PDF

                if not in_ytd_section:
                    continue

                # Try to parse line
                record = parse_class_line(line, year, month)
                if record:
                    # Filter out summary rows
                    class_name = record['class_name']
                    if class_name in ['LAST YEAR', 'PERCENT BY SIZE',
                                      'PERCENT OF INCREASE OR DECREASE',
                                      'TWO YEAR SPIRITS COMPARISON--THIS YEAR']:
                        continue

                    records.append(record)

    return records

def upload_to_supabase(records, year, month):
    """Upload records to Supabase database"""
    import psycopg2

    conn = psycopg2.connect(
        host='db.tnricrwvrnsnfbvrvoor.supabase.co',
        port=5432,
        database='postgres',
        user='postgres',
        password='pkWEbDa5HkTSGV9j'
    )
    cur = conn.cursor()

    # Check if data already exists
    cur.execute('''
        SELECT COUNT(*) FROM new_nabca.raw_ytd
        WHERE report_year = %s AND report_month = %s
    ''', (year, month))

    existing_count = cur.fetchone()[0]

    if existing_count > 0:
        print(f"    Deleting {existing_count} existing records...")
        cur.execute('''
            DELETE FROM new_nabca.raw_ytd
            WHERE report_year = %s AND report_month = %s
        ''', (year, month))
        conn.commit()

    # Insert new records
    print(f"    Inserting {len(records)} records to Supabase...")

    for r in records:
        cur.execute('''
            INSERT INTO new_nabca.raw_ytd (
                class_name, parent_class, pct_of_class, pct_total_dist_spirits,
                ytd_total_cases, ytd_cases_1_75l, ytd_cases_1_0l, ytd_cases_750ml,
                ytd_cases_750ml_traveler, ytd_cases_375ml, ytd_cases_200ml,
                ytd_cases_100ml, ytd_cases_50ml, report_year, report_month
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            r['class_name'], r.get('parent_class'), r.get('pct_of_class'),
            r.get('pct_total_dist_spirits'), r.get('ytd_total_cases'),
            r.get('ytd_cases_1_75l'), r.get('ytd_cases_1_0l'),
            r.get('ytd_cases_750ml'), r.get('ytd_cases_750ml_traveler'),
            r.get('ytd_cases_375ml'), r.get('ytd_cases_200ml'),
            r.get('ytd_cases_100ml'), r.get('ytd_cases_50ml'),
            r['report_year'], r['report_month']
        ))

    conn.commit()

    # Verify
    cur.execute('''
        SELECT COUNT(*) FROM new_nabca.raw_ytd
        WHERE report_year = %s AND report_month = %s
    ''', (year, month))

    final_count = cur.fetchone()[0]
    print(f"    Verified: {final_count} records in database")

    cur.close()
    conn.close()

def extract_all_pdfs():
    """Extract from all PDFs"""
    all_records = []

    # Get PDF list
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix='raw-pdfs/')
    pdfs = [obj['Key'] for obj in response.get('Contents', [])
            if obj['Key'].lower().endswith('.pdf')]

    print(f"Found {len(pdfs)} PDFs")

    for pdf_key in sorted(pdfs):
        filename = pdf_key.split('/')[-1]

        # Get date mapping (case insensitive)
        date_info = None
        for key, value in PDF_DATE_MAP.items():
            if key.lower() == filename.lower():
                date_info = value
                break

        if not date_info:
            print(f"  SKIP: No date mapping for {filename}")
            continue

        year, month = date_info
        print(f"Processing {filename} ({year}-{month:02d})...")

        try:
            records = extract_from_pdf(pdf_key, year, month)
            print(f"  Extracted {len(records)} records")
            all_records.extend(records)
        except Exception as e:
            print(f"  ERROR: {e}")

    return all_records

if __name__ == "__main__":
    # Parse command line arguments
    upload_to_db = '--upload' in sys.argv
    target_months = set()

    for arg in sys.argv[1:]:
        if arg == '--upload':
            continue
        if '-' in arg:  # Format: YYYY-MM
            parts = arg.split('-')
            if len(parts) == 2:
                try:
                    target_months.add((int(parts[0]), int(parts[1])))
                except:
                    pass

    print("=" * 70)
    print("EXTRACT BY CLASS - YEAR TO DATE")
    print("=" * 70)

    if target_months:
        print(f"Processing specific months: {sorted(target_months)}")
    else:
        print(f"Processing all {len(PDF_DATE_MAP)} months")

    if upload_to_db:
        print("Will upload to Supabase after extraction")

    os.makedirs('output', exist_ok=True)
    all_records = []

    # Filter months to process
    months_to_process = PDF_DATE_MAP.items()
    if target_months:
        months_to_process = [(pdf, date) for pdf, date in PDF_DATE_MAP.items()
                            if date in target_months]

    for pdf_name, (year, month) in sorted(months_to_process, key=lambda x: (x[1][0], x[1][1])):
        print(f"\nProcessing {pdf_name} ({year}-{month:02d})...")

        # Download and extract
        s3_pdf_key = f"raw-pdfs/{pdf_name}"
        records = extract_from_pdf(s3_pdf_key, year, month)
        print(f"  Extracted {len(records)} records")
        all_records.extend(records)

        # Save individual month CSV
        output_file = f"output/ytd_{year}_{month:02d}.csv"
        fieldnames = [
            'class_name', 'parent_class', 'pct_of_class', 'pct_total_dist_spirits',
            'ytd_total_cases', 'ytd_cases_1_75l', 'ytd_cases_1_0l', 'ytd_cases_750ml',
            'ytd_cases_750ml_traveler', 'ytd_cases_375ml', 'ytd_cases_200ml',
            'ytd_cases_100ml', 'ytd_cases_50ml', 'report_year', 'report_month'
        ]

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(records)

        print(f"  Saved CSV: {output_file}")

        # Upload to Supabase if requested
        if upload_to_db:
            upload_to_supabase(records, year, month)

    # Save combined CSV
    if all_records:
        combined_file = "output/by_class_year_to_date.csv"
        fieldnames = [
            'class_name', 'parent_class', 'pct_of_class', 'pct_total_dist_spirits',
            'ytd_total_cases', 'ytd_cases_1_75l', 'ytd_cases_1_0l', 'ytd_cases_750ml',
            'ytd_cases_750ml_traveler', 'ytd_cases_375ml', 'ytd_cases_200ml',
            'ytd_cases_100ml', 'ytd_cases_50ml', 'report_year', 'report_month'
        ]

        with open(combined_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(all_records)

        print(f"\n{'='*70}")
        print(f"COMPLETED: {len(all_records)} total records")
        print(f"CSV saved: {combined_file}")
        print(f"Months processed: {len(months_to_process)}")
        if upload_to_db:
            print(f"Uploaded to Supabase: raw_ytd table")
        print("Done!")
