"""
Recalculate vendor_summary accuracy EXCLUDING TOTAL VENDOR rows
Since TOTAL VENDOR rows were intentionally not uploaded to database
"""
import pandas as pd
import psycopg2
import os
from datetime import datetime

log_file = f"vendor_accuracy_no_totals_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

def log_print(msg):
    print(msg)
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(msg + '\n')
        f.flush()

log_print("=" * 100)
log_print("VENDOR SUMMARY - ACCURACY EXCLUDING TOTAL VENDOR ROWS")
log_print("=" * 100)
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
total_brand_records = 0
total_brand_errors = 0
all_results = []

for year, month in MONTHS:
    # Load Excel (PDF extraction)
    excel_file = f"vendor_summary_{year}_{month:02d}.xlsx"
    excel_path = os.path.join(output_dir, excel_file)

    if not os.path.exists(excel_path):
        continue

    df_excel = pd.read_excel(excel_path, engine='openpyxl')

    # EXCLUDE TOTAL VENDOR rows from Excel
    df_excel_brands = df_excel[df_excel['brand'] != 'TOTAL VENDOR'].copy()

    # Get database records (no TOTAL VENDOR rows in DB)
    cur.execute('''
        SELECT vendor, brand, class,
               l12m_this_year, l12m_prior_year,
               ytd_this_year, ytd_last_year,
               curr_month_this_year, curr_month_last_year
        FROM new_nabca.raw_vendor_summary
        WHERE report_year = %s AND report_month = %s
    ''', (year, month))
    db_rows = cur.fetchall()

    # Create lookup
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

    # Compare brand records only
    errors = 0
    error_details = []

    for idx, excel_row in df_excel_brands.iterrows():
        vendor = excel_row['vendor']
        brand = excel_row['brand']
        cls = excel_row['class']
        key = (vendor, brand, cls)

        if key not in db_lookup:
            errors += 1
            if len(error_details) < 5:
                error_details.append(f"Missing: {vendor} | {brand} | {cls}")
            continue

        # Compare values
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
            errors += 1
            if len(error_details) < 5:
                error_details.append(f"Value mismatch: {vendor} | {brand} | {cls}")

    brand_records = len(df_excel_brands)
    accuracy = ((brand_records - errors) / brand_records * 100) if brand_records > 0 else 0

    log_print(f"{year}-{month:02d}: {brand_records:,} brand records, {errors:,} errors, {accuracy:.2f}% accurate")
    if error_details:
        for err in error_details:
            log_print(f"  - {err}")

    total_brand_records += brand_records
    total_brand_errors += errors
    all_results.append({
        'month': f"{year}-{month:02d}",
        'records': brand_records,
        'errors': errors,
        'accuracy': accuracy
    })

overall_accuracy = ((total_brand_records - total_brand_errors) / total_brand_records * 100) if total_brand_records > 0 else 0

log_print("\n" + "=" * 100)
log_print("OVERALL RESULTS (BRAND RECORDS ONLY - EXCLUDING TOTAL VENDOR)")
log_print("=" * 100)
log_print(f"""
Total brand records verified:     {total_brand_records:,}
Brand records with errors:        {total_brand_errors:,}
Brand-level accuracy:             {overall_accuracy:.2f}%

Note: This excludes TOTAL VENDOR rows which were intentionally not uploaded.
This shows how accurate the actual brand-level data is.
""")

log_print("\nPer-Month Details:")
log_print(f"{'Month':<12} {'Records':<12} {'Errors':<10} {'Accuracy':<12}")
log_print("-" * 60)
for r in all_results:
    log_print(f"{r['month']:<12} {r['records']:<12,} {r['errors']:<10,} {r['accuracy']:<11.2f}%")

log_print(f"\nLog saved to: {log_file}")
log_print("=" * 100)

cur.close()
conn.close()
