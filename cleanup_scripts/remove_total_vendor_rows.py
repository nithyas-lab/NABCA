"""
Remove rows where brand is exactly "TOTAL VENDOR"
These are vendor summary rows that shouldn't be in the database
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
print("REMOVING ROWS WHERE BRAND = 'TOTAL VENDOR'")
print("=" * 100)

# Count rows with brand = TOTAL VENDOR
cur.execute("""
    SELECT COUNT(*)
    FROM new_nabca.raw_vendor_summary
    WHERE brand = 'TOTAL VENDOR'
""")
total = cur.fetchone()[0]

print(f"\nRows with brand = 'TOTAL VENDOR': {total:,}")

if total > 0:
    # Show sample
    cur.execute("""
        SELECT vendor, brand, class, report_year, report_month
        FROM new_nabca.raw_vendor_summary
        WHERE brand = 'TOTAL VENDOR'
        LIMIT 10
    """)

    rows = cur.fetchall()
    print(f"\nSample rows (first 10):")
    print(f"{'Vendor':<30} {'Brand':<20} {'Class':<20} {'Year-Month':<12}")
    print("-" * 100)
    for vendor, brand, cls, year, month in rows:
        cls_display = cls if cls else "NULL"
        print(f"{vendor[:29]:<30} {brand:<20} {cls_display[:19]:<20} {year}-{month:02d}")

    # Delete rows
    print(f"\nDeleting {total:,} rows...")
    cur.execute("""
        DELETE FROM new_nabca.raw_vendor_summary
        WHERE brand = 'TOTAL VENDOR'
    """)

    deleted = cur.rowcount
    conn.commit()

    print(f"Deleted: {deleted:,} rows")

    # Verify deletion
    cur.execute("""
        SELECT COUNT(*)
        FROM new_nabca.raw_vendor_summary
        WHERE brand = 'TOTAL VENDOR'
    """)
    remaining = cur.fetchone()[0]

    print(f"\nRemaining rows with brand = 'TOTAL VENDOR': {remaining:,}")
    print("OK!" if remaining == 0 else "ERROR - Still have TOTAL VENDOR rows!")

    # Show final record count
    cur.execute("SELECT COUNT(*) FROM new_nabca.raw_vendor_summary")
    final_count = cur.fetchone()[0]
    print(f"\nFinal record count in table: {final_count:,}")
else:
    print("\nNo rows to delete!")

cur.close()
conn.close()

print("=" * 100)
