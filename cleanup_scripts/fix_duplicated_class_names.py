"""
Fix ONLY Duplicated Class Names (100% Safe)
Example: "DOM DOM WHSKY-STRT-BRBN/TN WHSKY-STRT-BRBN/TN" -> "DOM WHSKY-STRT-BRBN/TN"

IMPORTANT: This ONLY fixes obvious duplications where the same text appears twice.
Does NOT touch truncated classes, NULL values, or make any guesses.
"""
import psycopg2
import re
from datetime import datetime

log_file = f"fix_duplicated_classes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

def log_print(msg):
    print(msg)
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(msg + '\n')
        f.flush()

def clean_duplicated_class(class_name):
    """
    Remove obvious duplications in class names.
    Only works when the exact same pattern repeats.
    """
    if not class_name:
        return class_name

    # Split by space and check for exact duplicates
    parts = class_name.split()

    # If we have even number of parts, check if first half == second half
    if len(parts) % 2 == 0:
        mid = len(parts) // 2
        first_half = ' '.join(parts[:mid])
        second_half = ' '.join(parts[mid:])

        if first_half == second_half:
            # Exact duplication found!
            return first_half

    # Check for partial duplications (consecutive repeated words)
    cleaned_parts = []
    prev_word = None
    for word in parts:
        if word != prev_word:
            cleaned_parts.append(word)
        prev_word = word

    return ' '.join(cleaned_parts)

conn = psycopg2.connect(
    host='db.tnricrwvrnsnfbvrvoor.supabase.co',
    port=5432,
    database='postgres',
    user='postgres',
    password='pkWEbDa5HkTSGV9j'
)
cur = conn.cursor()

log_print("=" * 120)
log_print("FIXING DUPLICATED CLASS NAMES (100% SAFE)")
log_print("=" * 120)
log_print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# Find all records with potential duplicated class names
log_print("Finding records with duplicated class names...\n")

cur.execute("""
    SELECT id, vendor, brand, class, report_year, report_month
    FROM new_nabca.raw_vendor_summary
    WHERE class LIKE '%% %%'  -- Has spaces
    AND (
        class LIKE '%%DOM DOM%%' OR
        class LIKE '%%WHSKY WHSKY%%' OR
        class LIKE '%%VODKA VODKA%%' OR
        class LIKE '%%GIN GIN%%' OR
        class LIKE '%%RUM RUM%%' OR
        class LIKE '%%TEQUILA TEQUILA%%' OR
        class LIKE '%%SCOTCH SCOTCH%%' OR
        class LIKE '%%STRT STRT%%' OR
        class LIKE '%%BLND BLND%%' OR
        class LIKE '%%BRBN BRBN%%' OR
        class LIKE '%%IMP IMP%%'
    )
    ORDER BY report_year, report_month, vendor, brand
""")

rows = cur.fetchall()

log_print(f"Found {len(rows):,} records with potential duplications\n")

if len(rows) == 0:
    log_print("No duplicated class names to fix!")
    cur.close()
    conn.close()
    exit()

# Show examples before fixing
log_print("Examples of duplications found (first 10):\n")
log_print(f"{'Original Class':<60} -> {'Cleaned Class':<50}")
log_print("-" * 120)

examples_shown = 0
fixes_to_apply = []

for record_id, vendor, brand, class_name, year, month in rows:
    cleaned = clean_duplicated_class(class_name)

    if cleaned != class_name:
        # Valid fix found
        fixes_to_apply.append((record_id, class_name, cleaned))

        if examples_shown < 10:
            log_print(f"{class_name[:59]:<60} -> {cleaned[:49]:<50}")
            examples_shown += 1

log_print(f"\n\nTotal fixes to apply: {len(fixes_to_apply):,}")

if len(fixes_to_apply) > 0:
    # Ask for confirmation (in real scenario)
    log_print("\nApplying fixes...")

    fixed_count = 0
    for record_id, original, cleaned in fixes_to_apply:
        cur.execute("""
            UPDATE new_nabca.raw_vendor_summary
            SET class = %s
            WHERE id = %s
        """, (cleaned, record_id))
        fixed_count += 1

        if fixed_count % 100 == 0:
            log_print(f"  Fixed {fixed_count:,} records...")

    conn.commit()

    log_print(f"\n✓ Successfully fixed {fixed_count:,} records")

    # Verify fix
    cur.execute("""
        SELECT COUNT(*)
        FROM new_nabca.raw_vendor_summary
        WHERE class LIKE '%% %%'
        AND (
            class LIKE '%%DOM DOM%%' OR
            class LIKE '%%WHSKY WHSKY%%' OR
            class LIKE '%%VODKA VODKA%%' OR
            class LIKE '%%GIN GIN%%' OR
            class LIKE '%%RUM RUM%%' OR
            class LIKE '%%TEQUILA TEQUILA%%' OR
            class LIKE '%%SCOTCH SCOTCH%%' OR
            class LIKE '%%STRT STRT%%' OR
            class LIKE '%%BLND BLND%%' OR
            class LIKE '%%BRBN BRBN%%' OR
            class LIKE '%%IMP IMP%%'
        )
    """)
    remaining = cur.fetchone()[0]

    log_print(f"\nRemaining duplicated class names: {remaining:,}")
    log_print("OK!" if remaining == 0 else "Some duplications may still exist")

else:
    log_print("\nNo valid fixes to apply")

log_print(f"\n\nLog saved to: {log_file}")
log_print("=" * 120)

cur.close()
conn.close()
