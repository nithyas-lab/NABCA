"""
Extract TOP 100 - VENDORS table from NABCA PDFs
Pages ~383-384 (search range 370-400)
"""

import boto3
import pdfplumber
import io
import re
import os
import csv
import psycopg2

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

# PDF to date mapping
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

def parse_vendor_line(line, year, month):
    """Parse a line of vendor data"""
    line = line.strip()
    if not line or len(line) < 20:
        return None

    # Skip header/footer lines
    skip_patterns = ['NABCA', 'TOP 100', 'Share', 'Vendor', 'Rank', 'Market',
                     'ACBAN', 'thgirypoC', 'ALL CONTROL', 'PAGE', 'to Date',
                     'Last 12', 'Current', 'Months', 'This Year', 'Prior Year']
    for pattern in skip_patterns:
        if pattern in line:
            return None

    # Pattern: VENDOR_NAME RANK SHARE L12M_TY L12M_PY L12M_CHG YTD_TY YTD_LY YTD_CHG CM_TY CM_LY CM_CHG
    # Example: SAZERAC COMPANY 1 14.50 8810256 8505861 3.58 6500998 6277342 3.56 762214 692759 10.03

    # Find where numbers start (rank should be 1-100)
    parts = line.split()
    if len(parts) < 12:
        return None

    # Find the rank (1-100) - it's the first pure integer after vendor name
    vendor_parts = []
    num_start = -1

    for i, part in enumerate(parts):
        # Check if this is the rank (1-100)
        if part.isdigit() and 1 <= int(part) <= 100:
            num_start = i
            break
        vendor_parts.append(part)

    if num_start < 1 or num_start >= len(parts) - 10:
        return None

    vendor_name = ' '.join(vendor_parts)
    # Fix character encoding issues (Unicode apostrophes to ASCII)
    vendor_name = vendor_name.replace('\u2019', "'").replace('\u2018', "'").replace('\ufffd', "'")
    numbers = parts[num_start:]

    if len(numbers) < 11:
        return None

    try:
        # Parse numbers
        def parse_num(s):
            s = s.replace(',', '')
            if s == '' or s == '-':
                return None
            try:
                if '.' in s:
                    return float(s)
                return int(s)
            except:
                return None

        def parse_pct(s):
            s = s.replace(',', '')
            if s == '' or s == '-':
                return None
            try:
                return float(s)
            except:
                return None

        rank = int(numbers[0])
        market_share = parse_pct(numbers[1])
        l12m_this_year = parse_num(numbers[2])
        l12m_prior_year = parse_num(numbers[3])
        l12m_change = parse_pct(numbers[4])
        ytd_this_year = parse_num(numbers[5])
        ytd_last_year = parse_num(numbers[6])
        ytd_change = parse_pct(numbers[7])
        curr_month_this = parse_num(numbers[8])
        curr_month_last = parse_num(numbers[9])
        curr_month_change = parse_pct(numbers[10])

        return {
            'vendor_name': vendor_name,
            'rank': rank,
            'market_share': market_share,
            'l12m_cases_this_year': l12m_this_year,
            'l12m_cases_prior_year': l12m_prior_year,
            'l12m_change_pct': l12m_change,
            'ytd_cases_this_year': ytd_this_year,
            'ytd_cases_last_year': ytd_last_year,
            'ytd_change_pct': ytd_change,
            'curr_month_this_year': curr_month_this,
            'curr_month_last_year': curr_month_last,
            'curr_month_change_pct': curr_month_change,
            'report_year': year,
            'report_month': month
        }
    except Exception as e:
        return None

def extract_from_pdf(pdf_key, year, month):
    """Extract TOP 100 VENDORS data"""
    print(f"  Downloading {pdf_key}...")

    response = s3.get_object(Bucket=BUCKET, Key=pdf_key)
    pdf_bytes = response['Body'].read()

    records = []
    in_top100_section = False
    found_ranks = set()

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        total_pages = len(pdf.pages)

        # Search pages 300-450 for TOP 100 - VENDORS (page numbers vary between PDFs)
        start_page = max(0, 299)
        end_page = min(450, total_pages)

        for page_num in range(start_page, end_page):
            page = pdf.pages[page_num]
            text = page.extract_text()

            if not text:
                continue

            # Check if this is TOP 100 - VENDORS page
            if "TOP 100 - VENDORS" in text or "TOP 100" in text and "VENDORS" in text:
                in_top100_section = True

            # Check if we've moved to a different section
            if in_top100_section and "TOP 20 - VENDORS" in text:
                # This is TOP 20 BY CLASS, not TOP 100
                break

            if not in_top100_section:
                continue

            lines = text.split('\n')

            for line in lines:
                record = parse_vendor_line(line, year, month)
                if record and record['rank'] not in found_ranks:
                    records.append(record)
                    found_ranks.add(record['rank'])

            # Stop if we have all 100
            if len(found_ranks) >= 100:
                break

    return records

def extract_all_pdfs():
    """Extract from all PDFs"""
    all_records = []

    response = s3.list_objects_v2(Bucket=BUCKET, Prefix='raw-pdfs/')
    pdfs = [obj['Key'] for obj in response.get('Contents', [])
            if obj['Key'].lower().endswith('.pdf')]

    print(f"Found {len(pdfs)} PDFs")

    for pdf_key in sorted(pdfs):
        filename = pdf_key.split('/')[-1]

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
            print(f"  Extracted {len(records)} vendors")
            all_records.extend(records)
        except Exception as e:
            print(f"  ERROR: {e}")

    return all_records

def upload_to_supabase(records):
    """Upload records to Supabase"""
    print("\n" + "=" * 60)
    print("UPLOADING TO SUPABASE")
    print("=" * 60)

    conn = psycopg2.connect(
        host='db.tnricrwvrnsnfbvrvoor.supabase.co',
        port=5432,
        database='postgres',
        user='postgres',
        password='pkWEbDa5HkTSGV9j'
    )
    cur = conn.cursor()

    # Group by month
    by_month = {}
    for r in records:
        key = (r['report_year'], r['report_month'])
        if key not in by_month:
            by_month[key] = []
        by_month[key].append(r)

    for (year, month), month_records in sorted(by_month.items()):
        print(f"\n{year}-{month:02d}: {len(month_records)} records")

        # Delete existing
        cur.execute('''
            DELETE FROM new_nabca.raw_top100_vendors
            WHERE report_year = %s AND report_month = %s
        ''', (year, month))
        deleted = cur.rowcount
        print(f"  Deleted {deleted} existing records")

        # Insert new
        for r in month_records:
            cur.execute('''
                INSERT INTO new_nabca.raw_top100_vendors (
                    report_year, report_month, vendor_name, rank, market_share,
                    l12m_cases_this_year, l12m_cases_prior_year, l12m_change_pct,
                    ytd_cases_this_year, ytd_cases_last_year, ytd_change_pct,
                    curr_month_this_year, curr_month_last_year, curr_month_change_pct
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            ''', (
                r['report_year'], r['report_month'], r['vendor_name'],
                r['rank'], r['market_share'],
                r['l12m_cases_this_year'], r['l12m_cases_prior_year'], r['l12m_change_pct'],
                r['ytd_cases_this_year'], r['ytd_cases_last_year'], r['ytd_change_pct'],
                r['curr_month_this_year'], r['curr_month_last_year'], r['curr_month_change_pct']
            ))

        print(f"  Inserted {len(month_records)} new records")

    conn.commit()
    cur.close()
    conn.close()
    print("\nUpload complete!")

if __name__ == "__main__":
    print("=" * 60)
    print("EXTRACT TOP 100 - VENDORS")
    print("Pages ~383-384")
    print("=" * 60)

    records = extract_all_pdfs()
    print(f"\nTotal records: {len(records)}")

    # Save CSV
    output_dir = os.path.join(os.path.dirname(__file__), 'output')
    os.makedirs(output_dir, exist_ok=True)

    csv_path = os.path.join(output_dir, 'top100_vendors.csv')
    fieldnames = ['vendor_name', 'rank', 'market_share',
                  'l12m_cases_this_year', 'l12m_cases_prior_year', 'l12m_change_pct',
                  'ytd_cases_this_year', 'ytd_cases_last_year', 'ytd_change_pct',
                  'curr_month_this_year', 'curr_month_last_year', 'curr_month_change_pct',
                  'report_year', 'report_month']

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(records)
    print(f"\nSaved CSV: {csv_path}")

    # Show sample
    print("\nSample records (Top 5):")
    for r in records[:5]:
        print(f"  {r['rank']:3}. {r['vendor_name']}: {r.get('l12m_cases_this_year', 'N/A'):,} cases (L12M)")

    # Upload to Supabase
    upload_to_supabase(records)

    print("\nDone!")
