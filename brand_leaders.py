"""
Extract BRAND LEADERS from NABCA PDFs (Pages 3-4 only)
========================================================
Extracts top 100 brands by class from NABCA monthly reports.
Saves to CSV and optionally uploads to Supabase.

Usage:
    python brand_leaders.py                    # Extract all months to CSV only
    python brand_leaders.py --upload           # Extract all months and upload to Supabase
    python brand_leaders.py 2025-12            # Extract specific month(s)
    python brand_leaders.py 2025-12 --upload   # Extract and upload specific month
"""
import boto3
import json
import csv
import os
import sys
from PyPDF2 import PdfReader, PdfWriter
import io

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

def extract_pages_3_4(pdf_bytes):
    """Extract just pages 3-4 from PDF bytes"""
    reader = PdfReader(io.BytesIO(pdf_bytes))
    writer = PdfWriter()

    # Pages 3-4 (0-indexed: pages 2-3)
    if len(reader.pages) >= 4:
        writer.add_page(reader.pages[2])
        writer.add_page(reader.pages[3])

    output = io.BytesIO()
    writer.write(output)
    output.seek(0)
    return output.read()

def call_textract_s3(s3_key):
    """Call Textract on S3 PDF"""
    import time

    print("  Starting Textract analysis...")
    response = textract.start_document_analysis(
        DocumentLocation={
            'S3Object': {
                'Bucket': BUCKET,
                'Name': s3_key
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

def extract_brand_leaders(blocks, year, month):
    """Extract brand leaders data from Textract blocks"""
    print(f"  Found {len(blocks)} blocks from Textract")
    print("  Parsing brand leaders table...")

    # Find tables in blocks
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
            if len(row) >= 9:  # Expect at least 9 columns
                brand = row.get(1, '').strip()
                rank = row.get(3, '').strip()

                # Skip if this looks like a header row (contains "BRAND" or "Rank")
                if brand.upper() == 'BRAND' or rank.upper() == 'RANK':
                    continue

                # Skip empty rows
                if not brand:
                    continue

                record = {
                    'brand': brand,
                    'type': row.get(2, '').strip(),
                    'rank': rank,
                    'pct_total': row.get(4, '').strip(),
                    'ytd_case_sales': row.get(5, '').strip(),
                    'ytd_change_vs_ly': row.get(6, '').strip(),
                    'current_month_case_sales': row.get(7, '').strip(),
                    'month_change_vs_ly': row.get(8, '').strip(),
                    'l12m_case_sales': row.get(9, '').strip(),
                    'report_year': year,
                    'report_month': month
                }

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
        SELECT COUNT(*) FROM new_nabca.raw_brand_leaders
        WHERE report_year = %s AND report_month = %s
    ''', (year, month))

    existing_count = cur.fetchone()[0]

    if existing_count > 0:
        print(f"    Deleting {existing_count} existing records...")
        cur.execute('''
            DELETE FROM new_nabca.raw_brand_leaders
            WHERE report_year = %s AND report_month = %s
        ''', (year, month))
        conn.commit()

    # Insert new records
    print(f"    Inserting {len(records)} records to Supabase...")

    for r in records:
        cur.execute('''
            INSERT INTO new_nabca.raw_brand_leaders (
                brand, type, rank, pct_total, ytd_case_sales,
                ytd_change_vs_ly, current_month_case_sales,
                month_change_vs_ly, l12m_case_sales,
                report_year, report_month
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            r['brand'], r['type'], r['rank'], r['pct_total'],
            r['ytd_case_sales'], r['ytd_change_vs_ly'],
            r['current_month_case_sales'], r['month_change_vs_ly'],
            r['l12m_case_sales'], r['report_year'], r['report_month']
        ))

    conn.commit()

    # Verify
    cur.execute('''
        SELECT COUNT(*) FROM new_nabca.raw_brand_leaders
        WHERE report_year = %s AND report_month = %s
    ''', (year, month))

    final_count = cur.fetchone()[0]
    print(f"    Verified: {final_count} records in database")

    cur.close()
    conn.close()


# Main execution
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
    print("BRAND LEADERS EXTRACTION (Pages 3-4 only)")
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

        # Download from S3
        s3_pdf_key = f"raw-pdfs/{pdf_name}"
        print(f"  Downloading from S3...")
        response = s3.get_object(Bucket=BUCKET, Key=s3_pdf_key)
        pdf_bytes = response['Body'].read()

        # Extract pages 3-4
        print(f"  Extracting pages 3-4...")
        pages_3_4_bytes = extract_pages_3_4(pdf_bytes)

        # Upload subset to S3 for Textract
        temp_s3_key = f"temp/brand_leaders_pages_{year}_{month:02d}.pdf"
        print(f"  Uploading subset to S3...")
        s3.put_object(Bucket=BUCKET, Key=temp_s3_key, Body=pages_3_4_bytes)

        # Call Textract
        blocks = call_textract_s3(temp_s3_key)

        # Extract data
        records = extract_brand_leaders(blocks, year, month)

        print(f"  Extracted {len(records)} records")
        all_records.extend(records)

        # Save individual month CSV
        output_file = f"output/brand_leaders_{year}_{month:02d}.csv"
        if records:
            fieldnames = ['brand', 'type', 'rank', 'pct_total', 'ytd_case_sales',
                          'ytd_change_vs_ly', 'current_month_case_sales',
                          'month_change_vs_ly', 'l12m_case_sales',
                          'report_year', 'report_month']

            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(records)

            print(f"  Saved CSV: {output_file}")

            # Upload to Supabase if requested
            if upload_to_db:
                upload_to_supabase(records, year, month)

        # Clean up temp S3 file
        try:
            s3.delete_object(Bucket=BUCKET, Key=temp_s3_key)
        except:
            pass

    # Save combined CSV
    if all_records:
        combined_file = "output/brand_leaders_all_months.csv"
        fieldnames = ['brand', 'type', 'rank', 'pct_total', 'ytd_case_sales',
                      'ytd_change_vs_ly', 'current_month_case_sales',
                      'month_change_vs_ly', 'l12m_case_sales',
                      'report_year', 'report_month']

        with open(combined_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_records)

        print(f"\n{'='*70}")
        print(f"COMPLETED: {len(all_records)} total records")
        print(f"CSV saved: {combined_file}")
        print(f"Months processed: {len(months_to_process)}")
        if upload_to_db:
            print(f"Uploaded to Supabase: raw_brand_leaders table")
        print("Done!")
