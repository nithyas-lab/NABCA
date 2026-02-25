"""
Comprehensive Data Quality Scan for All 18 Months
Step 1: Identify all extraction errors
Step 2: Fix ONLY issues we're 100% certain about
"""
import psycopg2
from collections import defaultdict
from datetime import datetime

log_file = f"data_quality_scan_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

def log_print(msg):
    print(msg)
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(msg + '\n')
        f.flush()

conn = psycopg2.connect(
    host='db.tnricrwvrnsnfbvrvoor.supabase.co',
    port=5432,
    database='postgres',
    user='postgres',
    password='pkWEbDa5HkTSGV9j'
)
cur = conn.cursor()

log_print("=" * 120)
log_print("COMPREHENSIVE DATA QUALITY SCAN - ALL 18 MONTHS")
log_print("=" * 120)
log_print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

MONTHS = [
    (2024, 7), (2024, 8), (2024, 9), (2024, 10), (2024, 11), (2024, 12),
    (2025, 1), (2025, 2), (2025, 3), (2025, 4), (2025, 5), (2025, 6),
    (2025, 7), (2025, 8), (2025, 9), (2025, 10), (2025, 11), (2025, 12)
]

# Track issues across all months
all_issues = {
    'duplicated_class': 0,
    'truncated_class': 0,
    'null_class': 0,
    'short_vendor': 0,
    'short_brand': 0
}

monthly_results = []

log_print("STEP 1: SCANNING ALL MONTHS FOR DATA QUALITY ISSUES\n")

for year, month in MONTHS:
    log_print(f"{'='*120}")
    log_print(f"MONTH: {year}-{month:02d}")
    log_print(f"{'='*120}")

    # Get total records
    cur.execute("""
        SELECT COUNT(*)
        FROM new_nabca.raw_vendor_summary
        WHERE report_year = %s AND report_month = %s
    """, (year, month))
    total = cur.fetchone()[0]

    issues = {}

    # Issue 1: Duplicated class names (e.g., "DOM DOM WHSKY-STRT-BRBN/TN WHSKY-STRT-BRBN/TN")
    cur.execute("""
        SELECT vendor, brand, class, id
        FROM new_nabca.raw_vendor_summary
        WHERE report_year = %s AND report_month = %s
        AND class LIKE '%% %%'  -- Has spaces
        AND (
            class LIKE '%%DOM DOM%%' OR
            class LIKE '%%WHSKY WHSKY%%' OR
            class LIKE '%%VODKA VODKA%%' OR
            class LIKE '%%GIN GIN%%' OR
            class LIKE '%%RUM RUM%%' OR
            class LIKE '%%TEQUILA TEQUILA%%' OR
            class LIKE '%%SCOTCH SCOTCH%%'
        )
    """, (year, month))
    dup_class = cur.fetchall()
    issues['duplicated_class'] = len(dup_class)
    all_issues['duplicated_class'] += len(dup_class)

    # Issue 2: Truncated class names (length <= 5 or just "DOM", "IMP", etc)
    cur.execute("""
        SELECT vendor, brand, class, id
        FROM new_nabca.raw_vendor_summary
        WHERE report_year = %s AND report_month = %s
        AND (
            LENGTH(class) <= 5 OR
            class IN ('DOM', 'IMP', 'FRGN', 'BTLD')
        )
        AND class IS NOT NULL
    """, (year, month))
    trunc_class = cur.fetchall()
    issues['truncated_class'] = len(trunc_class)
    all_issues['truncated_class'] += len(trunc_class)

    # Issue 3: NULL class
    cur.execute("""
        SELECT COUNT(*)
        FROM new_nabca.raw_vendor_summary
        WHERE report_year = %s AND report_month = %s
        AND class IS NULL
    """, (year, month))
    null_class = cur.fetchone()[0]
    issues['null_class'] = null_class
    all_issues['null_class'] += null_class

    # Issue 4: Very short vendor names (1-3 chars)
    cur.execute("""
        SELECT COUNT(*)
        FROM new_nabca.raw_vendor_summary
        WHERE report_year = %s AND report_month = %s
        AND LENGTH(vendor) <= 3
    """, (year, month))
    short_vendor = cur.fetchone()[0]
    issues['short_vendor'] = short_vendor
    all_issues['short_vendor'] += short_vendor

    # Issue 5: Very short brand names (1 char)
    cur.execute("""
        SELECT COUNT(*)
        FROM new_nabca.raw_vendor_summary
        WHERE report_year = %s AND report_month = %s
        AND LENGTH(brand) <= 1
    """, (year, month))
    short_brand = cur.fetchone()[0]
    issues['short_brand'] = short_brand
    all_issues['short_brand'] += short_brand

    # Calculate error rate
    total_errors = sum(issues.values())
    error_rate = (total_errors / total) * 100 if total > 0 else 0

    log_print(f"  Total records: {total:,}")
    log_print(f"  Duplicated class names: {issues['duplicated_class']:,}")
    log_print(f"  Truncated class names: {issues['truncated_class']:,}")
    log_print(f"  NULL class: {issues['null_class']:,}")
    log_print(f"  Short vendor names (<=3 chars): {issues['short_vendor']:,}")
    log_print(f"  Short brand names (1 char): {issues['short_brand']:,}")
    log_print(f"  Total errors: {total_errors:,} ({error_rate:.2f}%)")
    log_print(f"  Estimated accuracy: {100 - error_rate:.2f}%\n")

    monthly_results.append({
        'month': f"{year}-{month:02d}",
        'total': total,
        'errors': total_errors,
        'accuracy': 100 - error_rate,
        **issues
    })

# SUMMARY
log_print("\n" + "=" * 120)
log_print("OVERALL SUMMARY - ALL 18 MONTHS")
log_print("=" * 120)

total_records = sum(r['total'] for r in monthly_results)
total_errors = sum(r['errors'] for r in monthly_results)
overall_accuracy = ((total_records - total_errors) / total_records * 100) if total_records > 0 else 0

log_print(f"\nTotal records across all months: {total_records:,}")
log_print(f"\nTotal issues found:")
log_print(f"  - Duplicated class names: {all_issues['duplicated_class']:,}")
log_print(f"  - Truncated class names: {all_issues['truncated_class']:,}")
log_print(f"  - NULL class: {all_issues['null_class']:,}")
log_print(f"  - Short vendor names: {all_issues['short_vendor']:,}")
log_print(f"  - Short brand names: {all_issues['short_brand']:,}")
log_print(f"\nTotal errors: {total_errors:,}")
log_print(f"\n**TRUE PDF-LEVEL ACCURACY: {overall_accuracy:.2f}%**")

log_print("\n" + "=" * 120)
log_print("PER-MONTH BREAKDOWN")
log_print("=" * 120)
log_print(f"\n{'Month':<12} {'Records':<12} {'Errors':<10} {'Accuracy':<12} {'Dup Cls':<10} {'Trunc':<10} {'NULL':<10}")
log_print("-" * 120)

for r in monthly_results:
    log_print(f"{r['month']:<12} {r['total']:<12,} {r['errors']:<10,} {r['accuracy']:<11.2f}% "
              f"{r['duplicated_class']:<10,} {r['truncated_class']:<10,} {r['null_class']:<10,}")

log_print("\n" + "=" * 120)
log_print("WHAT CAN BE SAFELY FIXED?")
log_print("=" * 120)
log_print("""
SAFE TO FIX (100% certain):
  1. Duplicated class names - Remove obvious duplicates
     Example: "DOM DOM WHSKY-STRT-BRBN/TN WHSKY-STRT-BRBN/TN" -> "DOM WHSKY-STRT-BRBN/TN"

NOT SAFE TO FIX (would introduce errors):
  2. Truncated class "DOM" - Can't guess if it should be DOM WHSKY-STRT or DOM WHSKY-IMP
  3. NULL class - Don't know what the correct class should be
  4. Short vendor/brand names - Might be legitimate abbreviations

Recommendation:
  - Fix #1 (duplicated classes) automatically
  - For #2-4, these need manual verification against PDF source
""")

log_print(f"\nLog saved to: {log_file}")
log_print("=" * 120)

cur.close()
conn.close()
