"""
Complete month-by-month breakdown for vendor_summary
Shows both record-level accuracy and vendor TOTAL validation
"""
import pandas as pd
import psycopg2
import json
import os
from datetime import datetime
from collections import defaultdict

log_file = f"vendor_monthly_split_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

def log_print(msg):
    print(msg)
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(msg + '\n')
        f.flush()

log_print("=" * 120)
log_print("VENDOR SUMMARY - COMPLETE MONTHLY BREAKDOWN")
log_print("=" * 120)
log_print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

conn = psycopg2.connect(
    host='db.tnricrwvrnsnfbvrvoor.supabase.co',
    port=5432,
    database='postgres',
    user='postgres',
    password='pkWEbDa5HkTSGV9j'
)
cur = conn.cursor()

MONTHS = [
    (2024, 7), (2024, 8), (2024, 9), (2024, 10), (2024, 11), (2024, 12),
    (2025, 1), (2025, 2), (2025, 3), (2025, 4), (2025, 5), (2025, 6),
    (2025, 7), (2025, 8), (2025, 9), (2025, 10), (2025, 11), (2025, 12)
]

output_dir = "output"
cache_dir = "cache"
all_results = []

for year, month in MONTHS:
    log_print("=" * 120)
    log_print(f"MONTH: {year}-{month:02d}")
    log_print("=" * 120)

    # Load Excel (PDF extraction)
    excel_file = f"vendor_summary_{year}_{month:02d}.xlsx"
    excel_path = os.path.join(output_dir, excel_file)

    if not os.path.exists(excel_path):
        log_print(f"ERROR: Excel file not found\n")
        continue

    df_excel = pd.read_excel(excel_path, engine='openpyxl')
    df_excel_brands = df_excel[df_excel['brand'] != 'TOTAL VENDOR'].copy()

    log_print(f"\nPDF Extraction (Excel):")
    log_print(f"  Total records: {len(df_excel):,}")
    log_print(f"  Brand records: {len(df_excel_brands):,}")
    log_print(f"  TOTAL VENDOR rows: {len(df_excel) - len(df_excel_brands):,}")

    # Get database records
    cur.execute('''
        SELECT vendor, brand, class,
               l12m_this_year, l12m_prior_year,
               ytd_this_year, ytd_last_year,
               curr_month_this_year, curr_month_last_year
        FROM new_nabca.raw_vendor_summary
        WHERE report_year = %s AND report_month = %s
    ''', (year, month))
    db_rows = cur.fetchall()

    log_print(f"\nDatabase:")
    log_print(f"  Total records: {len(db_rows):,}")

    # STEP 2: Record-by-Record Comparison (Brand records only)
    log_print(f"\n{'='*60}")
    log_print("STEP 2: RECORD-BY-RECORD COMPARISON (Brand-Level Accuracy)")
    log_print('='*60)

    db_lookup = {}
    for row in db_rows:
        vendor, brand, cls, l12m_ty, l12m_ly, ytd_ty, ytd_ly, cm_ty, cm_ly = row
        key = (vendor, brand, cls)
        db_lookup[key] = {
            'l12m_this_year': l12m_ty,
            'l12m_prior_year': l12m_ly,
            'ytd_this_year': ytd_ty,
            'ytd_last_year': ytd_ly,
            'curr_month_this_year': cm_ty,
            'curr_month_last_year': cm_ly
        }

    record_errors = 0
    missing_records = 0
    value_mismatches = 0

    for idx, excel_row in df_excel_brands.iterrows():
        vendor = excel_row['vendor']
        brand = excel_row['brand']
        cls = excel_row['class']
        key = (vendor, brand, cls)

        if key not in db_lookup:
            record_errors += 1
            missing_records += 1
            continue

        db_record = db_lookup[key]
        has_mismatch = False

        for col in ['l12m_this_year', 'l12m_prior_year', 'ytd_this_year',
                    'ytd_last_year', 'curr_month_this_year', 'curr_month_last_year']:
            excel_val = excel_row.get(col)
            db_val = db_record.get(col)

            excel_is_null = pd.isna(excel_val) or excel_val is None
            db_is_null = db_val is None

            if excel_is_null and db_is_null:
                continue

            if excel_is_null != db_is_null:
                has_mismatch = True
                break

            if not excel_is_null:
                try:
                    if abs(float(excel_val) - float(db_val)) > 0.01:
                        has_mismatch = True
                        break
                except:
                    has_mismatch = True
                    break

        if has_mismatch:
            record_errors += 1
            value_mismatches += 1

    record_accuracy = ((len(df_excel_brands) - record_errors) / len(df_excel_brands) * 100) if len(df_excel_brands) > 0 else 0

    log_print(f"  Brand records compared: {len(df_excel_brands):,}")
    log_print(f"  Missing in DB: {missing_records:,}")
    log_print(f"  Value mismatches: {value_mismatches:,}")
    log_print(f"  Total errors: {record_errors:,}")
    log_print(f"  Brand-level accuracy: {record_accuracy:.2f}%")

    # STEP 3: TOTAL Row Validation (Mathematical Verification)
    log_print(f"\n{'='*60}")
    log_print("STEP 3: VENDOR TOTAL VALIDATION (Mathematical Accuracy)")
    log_print('='*60)

    cache_file = os.path.join(cache_dir, f"textract_vendor_summary_{year}_{month:02d}.json")

    if not os.path.exists(cache_file):
        log_print(f"  WARNING: Cache file not found, skipping validation\n")
        all_results.append({
            'month': f"{year}-{month:02d}",
            'brand_records': len(df_excel_brands),
            'record_errors': record_errors,
            'record_accuracy': record_accuracy,
            'vendor_count': 0,
            'vendor_errors': 0,
            'vendor_accuracy': 0
        })
        continue

    # Calculate vendor sums from Excel and DB
    vendor_sums_excel = defaultdict(lambda: defaultdict(float))
    vendor_sums_db = defaultdict(lambda: defaultdict(float))

    for _, row in df_excel_brands.iterrows():
        vendor = row['vendor']
        for col in ['l12m_this_year', 'l12m_prior_year', 'ytd_this_year',
                    'ytd_last_year', 'curr_month_this_year', 'curr_month_last_year']:
            val = row.get(col)
            if pd.notna(val) and val != 0:
                vendor_sums_excel[vendor][col] += float(val)

    for row in db_rows:
        vendor = row[0]
        vals = {'l12m_this_year': row[3], 'l12m_prior_year': row[4],
                'ytd_this_year': row[5], 'ytd_last_year': row[6],
                'curr_month_this_year': row[7], 'curr_month_last_year': row[8]}
        for col, val in vals.items():
            if val is not None and val != 0:
                vendor_sums_db[vendor][col] += float(val)

    # Compare vendor totals
    vendor_mismatches = 0
    vendors_with_errors = 0
    vendor_error_details = []

    for vendor in vendor_sums_excel:
        vendor_has_error = False
        for col in ['l12m_this_year', 'l12m_prior_year', 'ytd_this_year',
                    'ytd_last_year', 'curr_month_this_year', 'curr_month_last_year']:
            excel_sum = vendor_sums_excel[vendor].get(col, 0)
            db_sum = vendor_sums_db[vendor].get(col, 0)

            if excel_sum > 0 or db_sum > 0:
                if abs(excel_sum - db_sum) > 0.01:
                    vendor_mismatches += 1
                    vendor_has_error = True

        if vendor_has_error:
            vendors_with_errors += 1
            if len(vendor_error_details) < 5:
                vendor_error_details.append(vendor)

    total_vendor_checks = len(vendor_sums_excel) * 6
    vendor_accuracy = ((total_vendor_checks - vendor_mismatches) / total_vendor_checks * 100) if total_vendor_checks > 0 else 0

    log_print(f"  Vendors checked: {len(vendor_sums_excel):,}")
    log_print(f"  Vendors with TOTAL mismatches: {vendors_with_errors:,}")
    log_print(f"  Total column mismatches (vendors x 6 cols): {vendor_mismatches:,}")
    log_print(f"  Vendor TOTAL accuracy: {vendor_accuracy:.2f}%")

    if vendor_error_details:
        log_print(f"\n  Sample vendors with TOTAL mismatches:")
        for v in vendor_error_details:
            log_print(f"    - {v}")

    log_print("")

    all_results.append({
        'month': f"{year}-{month:02d}",
        'brand_records': len(df_excel_brands),
        'record_errors': record_errors,
        'record_accuracy': record_accuracy,
        'vendor_count': len(vendor_sums_excel),
        'vendor_errors': vendors_with_errors,
        'vendor_accuracy': vendor_accuracy
    })

# SUMMARY TABLE
log_print("\n" + "=" * 120)
log_print("COMPLETE MONTHLY SUMMARY")
log_print("=" * 120)
log_print(f"\n{'Month':<12} {'Brand Recs':<12} {'Rec Errors':<12} {'Rec Acc %':<12} {'Vendors':<10} {'Vend Err':<10} {'Vend Acc %':<12}")
log_print("-" * 120)

total_brand_records = 0
total_record_errors = 0
total_vendors = 0
total_vendor_errors = 0

for r in all_results:
    log_print(f"{r['month']:<12} {r['brand_records']:<12,} {r['record_errors']:<12,} "
              f"{r['record_accuracy']:<11.2f}% {r['vendor_count']:<10,} "
              f"{r['vendor_errors']:<10,} {r['vendor_accuracy']:<11.2f}%")

    total_brand_records += r['brand_records']
    total_record_errors += r['record_errors']
    total_vendors += r['vendor_count']
    total_vendor_errors += r['vendor_errors']

overall_record_acc = ((total_brand_records - total_record_errors) / total_brand_records * 100) if total_brand_records > 0 else 0

log_print("-" * 120)
log_print(f"{'TOTAL/AVG':<12} {total_brand_records:<12,} {total_record_errors:<12,} "
          f"{overall_record_acc:<11.2f}% {total_vendors:<10,} {total_vendor_errors:<10,}")

log_print("\n" + "=" * 120)
log_print("INTERPRETATION")
log_print("=" * 120)
log_print("""
Column Definitions:
  - Brand Recs: Number of brand records (excluding TOTAL VENDOR rows)
  - Rec Errors: Missing records or value mismatches at brand level
  - Rec Acc %: Percentage of brand records that match PDF exactly
  - Vendors: Number of unique vendors in that month
  - Vend Err: Number of vendors where sum(brands) != TOTAL VENDOR from PDF
  - Vend Acc %: Percentage of vendor TOTAL validations that passed

Two Types of Validation:
  1. Record-Level (Step 2): Does each brand record match between PDF and DB?
  2. Vendor TOTAL (Step 3): Does sum(brands per vendor) = TOTAL VENDOR row in PDF?
""")

log_print(f"\nLog saved to: {log_file}")
log_print("=" * 120)

cur.close()
conn.close()
