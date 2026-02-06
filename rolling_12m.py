"""
Extract By Class - ROLLING 12 MONTH
====================================
Uses AWS Textract (asynchronous) for table extraction from pages 9-10.
Pages 7-8 for July 2025.

Usage:
    python rolling_12m.py                    # Extract all months to CSV only
    python rolling_12m.py --upload           # Extract all months and upload to Supabase
    python rolling_12m.py 2025-12            # Extract specific month(s)
    python rolling_12m.py 2025-12 --upload   # Extract and upload specific month
"""

import boto3
import io
import csv
import os
import sys
import time
from PyPDF2 import PdfReader, PdfWriter

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

textract = boto3.client(
    'textract',
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
    elif cn.startswith('TEQUILA') or cn == 'TOTAL TEQUILA':
        return 'TEQUILA'
    elif cn.startswith('MEZCAL') or cn == 'TOTAL MEZCAL':
        return 'MEZCAL'
    elif cn.startswith('CRDL-') or cn == 'TOTAL CRDL' or cn == 'TOTAL CORDIALS':
        return 'CORDIALS'
    elif cn == 'COCKTAILS' or cn == 'TOTAL COCKTAILS':
        return 'COCKTAILS'
    else:
        return None

def extract_pages(pdf_bytes, page_start, page_end):
    """Extract specific pages from PDF bytes (0-indexed)"""
    reader = PdfReader(io.BytesIO(pdf_bytes))
    writer = PdfWriter()

    for page_num in range(page_start, min(page_end, len(reader.pages))):
        if page_num < len(reader.pages):
            writer.add_page(reader.pages[page_num])

    output = io.BytesIO()
    writer.write(output)
    output.seek(0)
    return output.read()

def call_textract_async(temp_s3_key):
    """Call Textract asynchronously on S3 PDF"""
    print("  Starting Textract analysis (async)...")

    response = textract.start_document_analysis(
        DocumentLocation={
            'S3Object': {
                'Bucket': BUCKET,
                'Name': temp_s3_key
            }
        },
        FeatureTypes=['TABLES']
    )

    job_id = response['JobId']
    print(f"  Textract Job ID: {job_id}")

    # Poll for completion
    while True:
        response = textract.get_document_analysis(JobId=job_id)
        status = response['JobStatus']

        if status == 'SUCCEEDED':
            print("  Textract completed!")
            break
        elif status == 'FAILED':
            raise Exception(f"Textract failed: {response.get('StatusMessage')}")

        print(f"  Status: {status}...")
        time.sleep(3)

    # Get all blocks
    blocks = response.get('Blocks', [])
    next_token = response.get('NextToken')

    while next_token:
        response = textract.get_document_analysis(JobId=job_id, NextToken=next_token)
        blocks.extend(response.get('Blocks', []))
        next_token = response.get('NextToken')

    return blocks

def parse_textract_tables(blocks, year, month):
    """Parse Textract blocks to extract rolling 12M data"""
    print(f"  Found {len(blocks)} blocks from Textract")
    print("  Parsing tables...")

    # Organize blocks
    tables = {}
    cells = {}

    for block in blocks:
        if block['BlockType'] == 'TABLE':
            tables[block['Id']] = block
        elif block['BlockType'] == 'CELL':
            cells[block['Id']] = block

    records = []

    # Process each table
    for table_id, table in tables.items():
        # Get cells for this table
        table_cells = []
        if 'Relationships' in table:
            for relationship in table['Relationships']:
                if relationship['Type'] == 'CHILD':
                    for cell_id in relationship['Ids']:
                        if cell_id in cells:
                            table_cells.append(cells[cell_id])

        # Organize cells by row
        rows = {}
        for cell in table_cells:
            row_index = cell.get('RowIndex', 0)
            col_index = cell.get('ColumnIndex', 0)

            if row_index not in rows:
                rows[row_index] = {}

            # Get cell text
            cell_text = ''
            if 'Relationships' in cell:
                for relationship in cell['Relationships']:
                    if relationship['Type'] == 'CHILD':
                        for child_id in relationship['Ids']:
                            child = next((b for b in blocks if b['Id'] == child_id), None)
                            if child and child['BlockType'] == 'WORD':
                                cell_text += child.get('Text', '') + ' '

            rows[row_index][col_index] = cell_text.strip()

        # Skip header row, parse data rows
        for row_idx in sorted(rows.keys()):
            if row_idx == 1:  # Skip header
                continue

            row = rows[row_idx]
            if len(row) < 3:
                continue

            class_name = row.get(1, '').strip()

            # Skip empty or header-like rows
            if not class_name or class_name.upper() in ['CLASS', 'NABCA', '']:
                continue

            # Check if this is a TOTAL row
            is_total = class_name.upper().startswith('TOTAL')

            record = {
                'class_name': class_name,
                'parent_class': get_parent_class(class_name),
                'pct_total_dist_spirits': None,
                'report_year': year,
                'report_month': month
            }

            # Parse numbers based on row type
            if is_total:
                # TOTAL rows: pct_total_dist_spirits pct_of_class total_cases bottle_sizes...
                # Column 2: (empty or pct_total_dist_spirits)
                # Column 3: pct_of_class (usually 100.00)
                # Column 4: total_cases
                # Column 5+: bottle sizes

                # Try column 2 for pct_total_dist_spirits, but it might be in column 3
                pct_spirits_str = row.get(2, '').replace(',', '').replace('%', '').strip()
                pct_class_str = row.get(3, '').replace(',', '').replace('%', '').strip()

                # If col 2 is empty and col 3 has a value, check if col 3 is pct_total or pct_class
                if not pct_spirits_str and pct_class_str:
                    # For TOTAL rows, if only one percentage, it's likely pct_total_dist_spirits
                    # followed by 100.0 as pct_of_class
                    col_4_str = row.get(4, '').replace(',', '').replace('%', '').strip()
                    try:
                        col_4_val = float(col_4_str) if col_4_str else None
                        if col_4_val and col_4_val == 100.0:
                            # Col 3 is pct_total_dist_spirits, col 4 is pct_of_class
                            record['pct_total_dist_spirits'] = float(pct_class_str)
                            record['pct_of_class'] = 100.0
                            col_idx = 5  # Case counts start at column 5
                        else:
                            # Col 3 is pct_of_class (100.0), col 4 is total_cases
                            record['pct_of_class'] = float(pct_class_str)
                            col_idx = 4  # Case counts start at column 4
                    except:
                        record['pct_of_class'] = float(pct_class_str)
                        col_idx = 4
                else:
                    # Standard case: col 2 has pct_total, col 3 has pct_class
                    if pct_spirits_str:
                        try:
                            record['pct_total_dist_spirits'] = float(pct_spirits_str)
                        except:
                            pass
                    if pct_class_str:
                        try:
                            record['pct_of_class'] = float(pct_class_str)
                        except:
                            record['pct_of_class'] = 100.0
                    col_idx = 4  # Case counts start at column 4
            else:
                # Regular subcategory rows: (empty) pct_of_class total_cases bottle_sizes...
                # Column 2: (empty)
                # Column 3: pct_of_class
                # Column 4: total_cases
                # Column 5+: bottle sizes
                try:
                    pct_str = row.get(3, '').replace(',', '').replace('%', '').strip()
                    if pct_str:
                        record['pct_of_class'] = float(pct_str)
                except:
                    pass

                # Case counts start at column 4
                col_idx = 4

            # Parse case counts (same order for both TOTAL and regular rows)
            try:
                total_cases_str = row.get(col_idx, '').replace(',', '').strip()
                if total_cases_str:
                    record['r12m_total_cases'] = int(total_cases_str)
            except:
                pass

            try:
                cases_1_75l_str = row.get(col_idx + 1, '').replace(',', '').strip()
                if cases_1_75l_str:
                    record['r12m_cases_1_75l'] = int(cases_1_75l_str)
            except:
                pass

            try:
                cases_1_0l_str = row.get(col_idx + 2, '').replace(',', '').strip()
                if cases_1_0l_str:
                    record['r12m_cases_1_0l'] = int(cases_1_0l_str)
            except:
                pass

            try:
                cases_750ml_str = row.get(col_idx + 3, '').replace(',', '').strip()
                if cases_750ml_str:
                    record['r12m_cases_750ml'] = int(cases_750ml_str)
            except:
                pass

            try:
                cases_750ml_traveler_str = row.get(col_idx + 4, '').replace(',', '').strip()
                if cases_750ml_traveler_str:
                    record['r12m_cases_750ml_traveler'] = int(cases_750ml_traveler_str)
            except:
                pass

            try:
                cases_375ml_str = row.get(col_idx + 5, '').replace(',', '').strip()
                if cases_375ml_str:
                    record['r12m_cases_375ml'] = int(cases_375ml_str)
            except:
                pass

            try:
                cases_200ml_str = row.get(col_idx + 6, '').replace(',', '').strip()
                if cases_200ml_str:
                    record['r12m_cases_200ml'] = int(cases_200ml_str)
            except:
                pass

            try:
                cases_100ml_str = row.get(col_idx + 7, '').replace(',', '').strip()
                if cases_100ml_str:
                    record['r12m_cases_100ml'] = int(cases_100ml_str)
            except:
                pass

            try:
                cases_50ml_str = row.get(col_idx + 8, '').replace(',', '').strip()
                if cases_50ml_str:
                    record['r12m_cases_50ml'] = int(cases_50ml_str)
            except:
                pass

            records.append(record)

    return records

def extract_rolling_12m(pdf_key, year, month):
    """Extract Rolling 12 Month data from PDF

    Page locations:
    - July 2025: pages 7-8 (0-indexed: 6-7)
    - Other months: pages 9-10 (0-indexed: 8-9)
    """
    # Determine page range
    if year == 2025 and month == 7:
        page_start, page_end = 6, 8
        print(f"  Extracting pages 7-8 from {pdf_key}...")
    else:
        page_start, page_end = 8, 10
        print(f"  Extracting pages 9-10 from {pdf_key}...")

    # Download PDF
    print(f"  Downloading from S3...")
    response = s3.get_object(Bucket=BUCKET, Key=pdf_key)
    pdf_bytes = response['Body'].read()

    # Extract specific pages
    pages_bytes = extract_pages(pdf_bytes, page_start, page_end)

    # Upload subset to S3 for Textract
    temp_s3_key = f"temp/rolling_12m_pages_{year}_{month:02d}.pdf"
    print(f"  Uploading subset to S3...")
    s3.put_object(Bucket=BUCKET, Key=temp_s3_key, Body=pages_bytes)

    # Call Textract asynchronously
    blocks = call_textract_async(temp_s3_key)

    # Parse results
    records = parse_textract_tables(blocks, year, month)

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
        SELECT COUNT(*) FROM new_nabca.raw_rolling_12m
        WHERE report_year = %s AND report_month = %s
    ''', (year, month))

    existing_count = cur.fetchone()[0]

    if existing_count > 0:
        print(f"    Deleting {existing_count} existing records...")
        cur.execute('''
            DELETE FROM new_nabca.raw_rolling_12m
            WHERE report_year = %s AND report_month = %s
        ''', (year, month))
        conn.commit()

    # Insert new records
    print(f"    Inserting {len(records)} records to Supabase...")

    for r in records:
        cur.execute('''
            INSERT INTO new_nabca.raw_rolling_12m (
                class_name, parent_class, pct_of_class, pct_total_dist_spirits,
                r12m_total_cases, r12m_cases_1_75l, r12m_cases_1_0l, r12m_cases_750ml,
                r12m_cases_750ml_traveler, r12m_cases_375ml, r12m_cases_200ml,
                r12m_cases_100ml, r12m_cases_50ml, report_year, report_month
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            r['class_name'], r.get('parent_class'), r.get('pct_of_class'),
            r.get('pct_total_dist_spirits'), r.get('r12m_total_cases'),
            r.get('r12m_cases_1_75l'), r.get('r12m_cases_1_0l'),
            r.get('r12m_cases_750ml'), r.get('r12m_cases_750ml_traveler'),
            r.get('r12m_cases_375ml'), r.get('r12m_cases_200ml'),
            r.get('r12m_cases_100ml'), r.get('r12m_cases_50ml'),
            r['report_year'], r['report_month']
        ))

    conn.commit()

    # Verify
    cur.execute('''
        SELECT COUNT(*) FROM new_nabca.raw_rolling_12m
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
    print("EXTRACT BY CLASS - ROLLING 12 MONTH")
    print("Using Textract (asynchronous)")
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

        try:
            # Extract data
            s3_pdf_key = f"raw-pdfs/{pdf_name}"
            records = extract_rolling_12m(s3_pdf_key, year, month)
            print(f"  Extracted {len(records)} records")
            all_records.extend(records)

            # Save individual month CSV
            output_file = f"output/rolling_12m_{year}_{month:02d}.csv"
            fieldnames = [
                'class_name', 'parent_class', 'pct_of_class', 'pct_total_dist_spirits',
                'r12m_total_cases', 'r12m_cases_1_75l', 'r12m_cases_1_0l', 'r12m_cases_750ml',
                'r12m_cases_750ml_traveler', 'r12m_cases_375ml', 'r12m_cases_200ml',
                'r12m_cases_100ml', 'r12m_cases_50ml', 'report_year', 'report_month'
            ]

            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(records)

            print(f"  Saved CSV: {output_file}")

            # Upload to Supabase if requested
            if upload_to_db:
                upload_to_supabase(records, year, month)

        except Exception as e:
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()

    # Save combined CSV
    if all_records:
        combined_file = "output/rolling_12m.csv"
        fieldnames = [
            'class_name', 'parent_class', 'pct_of_class', 'pct_total_dist_spirits',
            'r12m_total_cases', 'r12m_cases_1_75l', 'r12m_cases_1_0l', 'r12m_cases_750ml',
            'r12m_cases_750ml_traveler', 'r12m_cases_375ml', 'r12m_cases_200ml',
            'r12m_cases_100ml', 'r12m_cases_50ml', 'report_year', 'report_month'
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
            print(f"Uploaded to Supabase: raw_rolling_12m table")
        print("Done!")
