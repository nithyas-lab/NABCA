"""
Fix brand names that have TOTAL VENDOR merged into them
Example: "WASTELAND J JCE TOTAL VENDOR" -> "WASTELAND J JCE"
"""
import psycopg2

conn = psycopg2.connect(
    host='db.tnricrwvrnsnfbvrvoor.supabase.co',
    port=5432,
    database='postgres',
    user='postgres',
    password='pkWEbDa5HkTSGV9j'
)
cur = conn.cursor()

print("=" * 100)
print("FIXING MERGED TOTAL VENDOR IN BRAND NAMES")
print("=" * 100)

# Find rows with merged TOTAL VENDOR
cur.execute("""
    SELECT vendor, brand, class, report_year, report_month
    FROM new_nabca.raw_vendor_summary
    WHERE brand LIKE '%TOTAL VENDOR%'
    AND brand <> 'TOTAL VENDOR'
    LIMIT 20
""")

rows = cur.fetchall()

if rows:
    print(f"\nFound examples (showing first 20):\n")
    print(f"{'Vendor':<30} {'Original Brand':<40} -> {'Cleaned Brand':<40}")
    print("-" * 100)

    for vendor, brand, cls, year, month in rows:
        # Remove TOTAL VENDOR from brand name
        cleaned_brand = brand.replace(' TOTAL VENDOR', '').replace('TOTAL VENDOR ', '').strip()
        print(f"{vendor[:29]:<30} {brand[:39]:<40} -> {cleaned_brand[:39]:<40}")
else:
    print("\nNo merged TOTAL VENDOR found in brand names")

# Count total affected
cur.execute("""
    SELECT COUNT(*)
    FROM new_nabca.raw_vendor_summary
    WHERE brand LIKE '%TOTAL VENDOR%'
    AND brand <> 'TOTAL VENDOR'
""")
total = cur.fetchone()[0]
print(f"\n\nTotal rows affected: {total:,}")

if total > 0:
    print("\nProceeding to fix...")

    # Update rows to remove TOTAL VENDOR from brand name
    cur.execute("""
        UPDATE new_nabca.raw_vendor_summary
        SET brand = TRIM(REPLACE(REPLACE(brand, ' TOTAL VENDOR', ''), 'TOTAL VENDOR ', ''))
        WHERE brand LIKE '%TOTAL VENDOR%'
        AND brand <> 'TOTAL VENDOR'
    """)

    updated = cur.rowcount
    conn.commit()

    print(f"Updated {updated:,} rows")

    # Verify fix
    cur.execute("""
        SELECT COUNT(*)
        FROM new_nabca.raw_vendor_summary
        WHERE brand LIKE '%TOTAL VENDOR%'
        AND brand <> 'TOTAL VENDOR'
    """)
    remaining = cur.fetchone()[0]

    print(f"\nRemaining rows with merged TOTAL VENDOR: {remaining:,}")
    print("OK!" if remaining == 0 else "ERROR - Still have merged TOTAL VENDOR!")
else:
    print("\nNo rows to fix!")

cur.close()
conn.close()

print("=" * 100)
