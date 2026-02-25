"""
Extract By Class - Current Month - Total Case Sales
====================================================
Extracts the "By Class" table from pages 5-6 of each NABCA PDF.
Uses pdfplumber for fast text-based extraction.

Usage:
    python current_month.py                    # Extract all months to CSV only
    python current_month.py --upload           # Extract all months and upload to Supabase
    python current_month.py 2025-12            # Extract specific month(s)
    python current_month.py 2025-12 --upload   # Extract and upload specific month
"""

import boto3
import pdfplumber
import io
import re
import os
import sys
import csv
from datetime import datetime

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

# Initialize S3 client
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

# Known CLASS names (to help with parsing)
CLASS_PREFIXES = [
    "DOM WHSKY", "SCOTCH", "CAN-", "BLND-FRGN", "IRISH", "OTH IMP WHSKY",
    "GIN-", "NEUTRAL GRAIN", "VODKA", "CACHACA", "RUM-", "BRANDY",
    "COGNAC", "TEQUILA", "MEZCAL", "COCKTAILS", "TOTAL COCKTAILS",
    "CRDL-", "SPCTY", "TOTAL SPIRITS", "TOTAL CRDL"
]

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
    elif cn.startswith('BRNDY/CGNC') or cn == 'TOTAL BRANDY' or 'BRANDY/COGNAC' in cn:
        return 'BRANDY/COGNAC'
    elif cn.startswith('TEQUILA') or cn == 'TOTAL TEQUILA':
        return 'TEQUILA'
    elif cn.startswith('MEZCAL') or cn == 'TOTAL MEZCAL':
        return 'MEZCAL'
    elif cn.startswith('CRDL-') or cn == 'TOTAL CORDIALS':
        return 'CORDIALS'
    elif cn == 'COCKTAILS' or cn == 'TOTAL COCKTAILS':
        return 'COCKTAILS'
    else:
        return None

def parse_class_line(line):
    """Parse a line of class data into components

    Handles multiple formats:
    1. Regular: CLASS_NAME pct total_cases bottle_sizes...
    2. TOTAL: TOTAL CLASS pct_spirits 100.00 total_cases bottle_sizes...
    3. TOTAL ALL SPIRITS: TOTAL ALL SPIRITS 100.00 total_cases bottle_sizes...
    4. NULL rows: CLASS_NAME .00 (no case counts)
    5. Summary: TWO YEAR SPIRITS... or PERCENT...
    """
    # Skip header lines and empty lines
    if not line or len(line) < 5:
        return None
    if "CLASS" in line or "% of" in line or "Dist. Spirits" in line:
        return None
    if "PAGE" in line or "NABCA" in line or "BY CLASS" in line:
        return None
    if "Bottle Sizes" in line or "1.75 L" in line:
        return None
    if "ACBAN" in line or "thgirypoC" in line:
        return None

    # Split into parts
    parts = line.split()
    if len(parts) < 2:
        return None

    # Find where numbers start
    num_start = -1
    class_parts = []

    for i, part in enumerate(parts):
        # Check if this looks like a number
        clean_part = part.replace(',', '').replace('.', '').replace('-', '')
        if clean_part.isdigit() or (part.count('.') <= 1 and part.replace('.', '').replace('-', '').replace(',', '').isdigit()):
            num_start = i
            break
        class_parts.append(part)

    if num_start < 1:
        return None

    class_name = ' '.join(class_parts)
    numbers = parts[num_start:]

    # Clean up class name
    class_name = class_name.strip()
    if not class_name or class_name in ['', ' ']:
        return None

    # Parse numbers
    parsed_numbers = []
    for num in numbers:
        try:
            cleaned = num.replace(',', '')
            if '.' in cleaned or '-' in cleaned:
                parsed_numbers.append(float(cleaned))
            else:
                parsed_numbers.append(int(cleaned))
        except:
            pass

    # Special case: NULL value rows have only one number (.00)
    # e.g., "CRDL-FLVRD BRNDES-APRCT .00"
    if len(parsed_numbers) == 1 and parsed_numbers[0] == 0.0:
        return {
            'class_name': class_name,
            'numbers': parsed_numbers,
            'is_null_row': True
        }

    # Need at least 2 numbers for valid data rows
    if len(parsed_numbers) < 2:
        return None

    return {
        'class_name': class_name,
        'numbers': parsed_numbers,
        'is_null_row': False
    }

def extract_current_month_by_class(pdf_bytes, year, month):
    """Extract By Class - Current Month data from PDF"""
    records = []

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        in_current_month_section = False

        # Scan pages 4-7 (0-indexed: 3-6) looking for CURRENT MONTH section
        for page_num in range(3, min(10, len(pdf.pages))):
            page = pdf.pages[page_num]
            text = page.extract_text()

            if not text:
                continue

            lines = text.split('\n')

            for line in lines:
                # Check if we're entering CURRENT MONTH section
                if "CURRENT MONTH" in line and "TOTAL CASE SALES" in line:
                    in_current_month_section = True
                    continue

                # Check if we're leaving CURRENT MONTH section (entering YTD)
                if "YEAR TO DATE" in line and "TOTAL CASE SALES" in line:
                    in_current_month_section = False
                    continue

                # Only process lines in CURRENT MONTH section
                if not in_current_month_section:
                    continue

                # Try to parse class data
                parsed = parse_class_line(line)
                if parsed:
                    class_name = parsed['class_name']
                    nums = parsed['numbers']
                    is_null_row = parsed.get('is_null_row', False)

                    # Skip LAST YEAR row (not in Supabase)
                    if class_name == 'LAST YEAR':
                        continue

                    # Map full names to shortened Supabase names
                    if class_name == 'TWO YEAR SPIRITS COMPARISON--THIS YEAR':
                        class_name = 'TWO YEAR SPIRITS'
                    elif class_name == 'PERCENT OF INCREASE OR DECREASE':
                        class_name = 'PERCENT OF INCREASE OR'

                    record = {
                        'class_name': class_name,
                        'parent_class': get_parent_class(class_name),
                        'pct_total_dist_spirits': None,  # Will be populated for TOTAL rows
                        'report_year': year,
                        'report_month': month
                    }

                    # Handle NULL value rows (e.g., "CRDL-FLVRD BRNDES-APRCT .00")
                    if is_null_row:
                        record['pct_of_class'] = 0.0
                        record['total_cases'] = 0  # Store as 0 to match Supabase
                        # All bottle size fields default to None which becomes NULL
                        records.append(record)
                        continue

                    # Determine row type
                    is_total = class_name.startswith('TOTAL')
                    is_total_all_spirits = (class_name == 'TOTAL ALL SPIRITS')
                    is_two_year = 'TWO YEAR SPIRITS' in class_name
                    is_percent = ('PERCENT' in class_name and class_name != 'TOTAL ALL SPIRITS')

                    # Handle different row formats
                    if is_total_all_spirits:
                        # TOTAL ALL SPIRITS: 100.00 total_cases bottle_sizes...
                        # Format: nums = [100.00, total_cases, 1.75L, 1.0L, ...]
                        record['pct_of_class'] = 100.0
                        if len(nums) >= 2:
                            record['total_cases'] = int(nums[1]) if nums[1] else 0
                        if len(nums) >= 3:
                            record['cases_1_75l'] = int(nums[2]) if nums[2] else 0
                        if len(nums) >= 4:
                            record['cases_1_0l'] = int(nums[3]) if nums[3] else 0
                        if len(nums) >= 5:
                            record['cases_750ml'] = int(nums[4]) if nums[4] else 0
                        if len(nums) >= 6:
                            record['cases_750ml_traveler'] = int(nums[5]) if nums[5] else 0
                        if len(nums) >= 7:
                            record['cases_375ml'] = int(nums[6]) if nums[6] else 0
                        if len(nums) >= 8:
                            record['cases_200ml'] = int(nums[7]) if nums[7] else 0
                        if len(nums) >= 9:
                            record['cases_100ml'] = int(nums[8]) if nums[8] else 0
                        if len(nums) >= 10:
                            record['cases_50ml'] = int(nums[9]) if nums[9] else 0

                    elif is_two_year:
                        # TWO YEAR SPIRITS: total_cases bottle_sizes... (no percentage)
                        # Format: nums = [total_cases, 1.75L, 1.0L, ...]
                        record['pct_of_class'] = None
                        if len(nums) >= 1:
                            record['total_cases'] = int(nums[0]) if nums[0] else 0
                        if len(nums) >= 2:
                            record['cases_1_75l'] = int(nums[1]) if nums[1] else 0
                        if len(nums) >= 3:
                            record['cases_1_0l'] = int(nums[2]) if nums[2] else 0
                        if len(nums) >= 4:
                            record['cases_750ml'] = int(nums[3]) if nums[3] else 0
                        if len(nums) >= 5:
                            record['cases_750ml_traveler'] = int(nums[4]) if nums[4] else 0
                        if len(nums) >= 6:
                            record['cases_375ml'] = int(nums[5]) if nums[5] else 0
                        if len(nums) >= 7:
                            record['cases_200ml'] = int(nums[6]) if nums[6] else 0
                        if len(nums) >= 8:
                            record['cases_100ml'] = int(nums[7]) if nums[7] else 0
                        if len(nums) >= 9:
                            record['cases_50ml'] = int(nums[8]) if nums[8] else 0

                    elif is_percent:
                        # PERCENT rows: all percentages, store as pct_of_class and total_cases
                        # For "PERCENT OF INCREASE OR DECREASE": negative percentages
                        # For "PERCENT BY SIZE": size distribution percentages
                        record['pct_of_class'] = nums[0] if len(nums) >= 1 else None
                        record['total_cases'] = int(nums[1]) if len(nums) >= 2 and isinstance(nums[1], (int, float)) else None
                        # Don't parse bottle sizes for PERCENT rows

                    elif is_total:
                        # Regular TOTAL rows: pct_total_dist_spirits pct_of_class total_cases bottle_sizes...
                        # Format: nums = [pct_total_dist_spirits, 100.00, total_cases, 1.75L, ...]
                        # Example: "TOTAL DOM WHSKY 18.60 100.00 925548..."
                        if len(nums) >= 1:
                            record['pct_total_dist_spirits'] = float(nums[0]) if nums[0] else None
                        if len(nums) >= 2:
                            record['pct_of_class'] = float(nums[1]) if nums[1] else 100.0
                        if len(nums) >= 3:
                            record['total_cases'] = int(nums[2]) if nums[2] else 0
                        if len(nums) >= 4:
                            record['cases_1_75l'] = int(nums[3]) if nums[3] else 0
                        if len(nums) >= 5:
                            record['cases_1_0l'] = int(nums[4]) if nums[4] else 0
                        if len(nums) >= 6:
                            record['cases_750ml'] = int(nums[5]) if nums[5] else 0
                        if len(nums) >= 7:
                            record['cases_750ml_traveler'] = int(nums[6]) if nums[6] else 0
                        if len(nums) >= 8:
                            record['cases_375ml'] = int(nums[7]) if nums[7] else 0
                        if len(nums) >= 9:
                            record['cases_200ml'] = int(nums[8]) if nums[8] else 0
                        if len(nums) >= 10:
                            record['cases_100ml'] = int(nums[9]) if nums[9] else 0
                        if len(nums) >= 11:
                            record['cases_50ml'] = int(nums[10]) if nums[10] else 0

                    else:
                        # Regular subcategory rows: pct total_cases bottle_sizes...
                        # Format: nums = [pct, total_cases, 1.75L, 1.0L, ...]
                        if len(nums) >= 1:
                            record['pct_of_class'] = nums[0] if isinstance(nums[0], float) else None
                        if len(nums) >= 2:
                            record['total_cases'] = int(nums[1]) if nums[1] else 0
                        if len(nums) >= 3:
                            record['cases_1_75l'] = int(nums[2]) if nums[2] else 0
                        if len(nums) >= 4:
                            record['cases_1_0l'] = int(nums[3]) if nums[3] else 0
                        if len(nums) >= 5:
                            record['cases_750ml'] = int(nums[4]) if nums[4] else 0
                        if len(nums) >= 6:
                            record['cases_750ml_traveler'] = int(nums[5]) if nums[5] else 0
                        if len(nums) >= 7:
                            record['cases_375ml'] = int(nums[6]) if nums[6] else 0
                        if len(nums) >= 8:
                            record['cases_200ml'] = int(nums[7]) if nums[7] else 0
                        if len(nums) >= 9:
                            record['cases_100ml'] = int(nums[8]) if nums[8] else 0
                        if len(nums) >= 10:
                            record['cases_50ml'] = int(nums[9]) if nums[9] else 0

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
        SELECT COUNT(*) FROM new_nabca.raw_current_month
        WHERE report_year = %s AND report_month = %s
    ''', (year, month))

    existing_count = cur.fetchone()[0]

    if existing_count > 0:
        print(f"    Deleting {existing_count} existing records...")
        cur.execute('''
            DELETE FROM new_nabca.raw_current_month
            WHERE report_year = %s AND report_month = %s
        ''', (year, month))
        conn.commit()

    # Insert new records
    print(f"    Inserting {len(records)} records to Supabase...")

    for r in records:
        cur.execute('''
            INSERT INTO new_nabca.raw_current_month (
                class_name, parent_class, pct_of_class, pct_total_dist_spirits, total_cases,
                cases_1_75l, cases_1_0l, cases_750ml, cases_750ml_traveler,
                cases_375ml, cases_200ml, cases_100ml, cases_50ml,
                report_year, report_month
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            r['class_name'], r.get('parent_class'), r.get('pct_of_class'),
            r.get('pct_total_dist_spirits'), r.get('total_cases', 0),
            r.get('cases_1_75l', 0), r.get('cases_1_0l', 0),
            r.get('cases_750ml', 0), r.get('cases_750ml_traveler', 0),
            r.get('cases_375ml', 0), r.get('cases_200ml', 0),
            r.get('cases_100ml', 0), r.get('cases_50ml', 0),
            r['report_year'], r['report_month']
        ))

    conn.commit()

    # Verify
    cur.execute('''
        SELECT COUNT(*) FROM new_nabca.raw_current_month
        WHERE report_year = %s AND report_month = %s
    ''', (year, month))

    final_count = cur.fetchone()[0]
    print(f"    Verified: {final_count} records in database")

    cur.close()
    conn.close()

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
    print("CURRENT MONTH BY CLASS - TOTAL CASE SALES")
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

        # Download PDF from S3
        s3_pdf_key = f"raw-pdfs/{pdf_name}"
        response = s3.get_object(Bucket=BUCKET, Key=s3_pdf_key)
        pdf_bytes = response['Body'].read()

        # Extract records
        records = extract_current_month_by_class(pdf_bytes, year, month)
        print(f"  Extracted {len(records)} records")
        all_records.extend(records)

        # Save individual month CSV
        output_file = f"output/current_month_{year}_{month:02d}.csv"
        fieldnames = [
            'class_name', 'parent_class', 'pct_of_class', 'pct_total_dist_spirits', 'total_cases',
            'cases_1_75l', 'cases_1_0l', 'cases_750ml', 'cases_750ml_traveler',
            'cases_375ml', 'cases_200ml', 'cases_100ml', 'cases_50ml',
            'report_year', 'report_month'
        ]

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

        print(f"  Saved CSV: {output_file}")

        # Upload to Supabase if requested
        if upload_to_db:
            upload_to_supabase(records, year, month)

    # Save combined CSV
    if all_records:
        combined_file = "output/current_month_all_months.csv"
        fieldnames = [
            'class_name', 'parent_class', 'pct_of_class', 'pct_total_dist_spirits', 'total_cases',
            'cases_1_75l', 'cases_1_0l', 'cases_750ml', 'cases_750ml_traveler',
            'cases_375ml', 'cases_200ml', 'cases_100ml', 'cases_50ml',
            'report_year', 'report_month'
        ]

        with open(combined_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_records)

        print(f"\n{'='*70}")
        print(f"COMPLETED: {len(all_records)} total records")
        print(f"CSV saved: {combined_file}")
        print(f"Months processed: {len(months_to_process)}")
        if upload_to_db:
            print(f"Uploaded to Supabase: raw_current_month table")
        print("Done!")
