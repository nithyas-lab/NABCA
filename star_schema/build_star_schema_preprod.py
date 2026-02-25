"""
NABCA Star Schema Builder for nabca-pre-prod (amber-tree)

Builds dimension and fact tables from raw tables in the nabca-pre-prod schema
on supabase-amber-tree (xhvsvhiysnacdinclncn).

Tables created:
  Dimensions: dim_vendor, dim_class, dim_time, dim_brand, dim_bottle_size
  Facts: fact_brand_summary, fact_bottle_sales, fact_brand_leaders,
         fact_current_month_by_class, fact_ytd_by_class, fact_l12m_by_class,
         fact_vendor_performance, fact_vendor_by_class

Usage:
  python build_star_schema_preprod.py
"""

import psycopg2
from psycopg2.extras import execute_batch
import sys
import time

# ============================================================
# Configuration
# ============================================================

DB_CONFIG = {
    'host': 'db.xhvsvhiysnacdinclncn.supabase.co',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'Nithya123*',
    'port': 5432,
    'connect_timeout': 30
}

SCHEMA = 'nabca-pre-prod'

# Batch sizes
DIM_BATCH_SIZE = 500
FACT_BATCH_SIZE = 1000

# Bottle size definitions (pre-populated)
BOTTLE_SIZES = [
    {'bottle_size_id': 1, 'bottle_size_name': '1.75L',          'bottle_size_ml': 1750, 'sort_order': 1},
    {'bottle_size_id': 2, 'bottle_size_name': '1.0L',           'bottle_size_ml': 1000, 'sort_order': 2},
    {'bottle_size_id': 3, 'bottle_size_name': '750ml',          'bottle_size_ml': 750,  'sort_order': 3},
    {'bottle_size_id': 4, 'bottle_size_name': '750ml Traveler', 'bottle_size_ml': 750,  'sort_order': 4},
    {'bottle_size_id': 5, 'bottle_size_name': '375ml',          'bottle_size_ml': 375,  'sort_order': 5},
    {'bottle_size_id': 6, 'bottle_size_name': '200ml',          'bottle_size_ml': 200,  'sort_order': 6},
    {'bottle_size_id': 7, 'bottle_size_name': '100ml',          'bottle_size_ml': 100,  'sort_order': 7},
    {'bottle_size_id': 8, 'bottle_size_name': '50ml',           'bottle_size_ml': 50,   'sort_order': 8},
]

# Mapping from raw_brand_summary bottle columns -> bottle_size_id
BOTTLE_COLUMN_MAP = {
    'curr_month_175l':          1,  # 1.75L
    'curr_month_1l':            2,  # 1.0L
    'curr_month_750ml':         3,  # 750ml
    'curr_month_750ml_traveler': 4, # 750ml Traveler
    'curr_month_375ml':         5,  # 375ml
    'curr_month_200ml':         6,  # 200ml
    'curr_month_100ml':         7,  # 100ml
    'curr_month_50ml':          8,  # 50ml
}


# ============================================================
# Helper Functions
# ============================================================

def clean_numeric(val):
    """Clean text values to numeric. Handles commas, %, OCR artifacts."""
    if val is None or val == '' or val == 'None':
        return None
    try:
        val = str(val).replace(',', '').replace('%', '').replace(' ', '')
        val = val.replace('. O', '0').replace('.O', '0').replace(' O', '0')
        return float(val)
    except (ValueError, TypeError):
        return None


def clean_int(val):
    """Clean text values to integer."""
    n = clean_numeric(val)
    if n is None:
        return None
    return int(round(n))


def get_month_name(month):
    """Get month name from number."""
    months = ['', 'January', 'February', 'March', 'April', 'May', 'June',
              'July', 'August', 'September', 'October', 'November', 'December']
    return months[month] if 1 <= month <= 12 else ''


def q(name):
    """Quote a schema-qualified name."""
    return f'"{SCHEMA}"."{name}"'


def fetch_table(cursor, table_name):
    """Fetch all rows from a raw table as list of dicts."""
    cursor.execute(f'SELECT * FROM {q(table_name)};')
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def insert_batched(cursor, conn, table_name, columns, rows, batch_size=1000,
                   on_conflict=None):
    """Insert rows in batches using execute_batch. Returns count inserted.

    on_conflict: tuple of (conflict_columns, update_columns) for UPSERT.
                 If provided, uses ON CONFLICT (conflict_cols) DO UPDATE SET ...
                 If None, uses plain INSERT (fails on duplicates).
    """
    if not rows:
        return 0

    cols_str = ', '.join([f'"{c}"' for c in columns])
    placeholders = ', '.join(['%s'] * len(columns))

    if on_conflict:
        conflict_cols, update_cols = on_conflict
        conflict_str = ', '.join([f'"{c}"' for c in conflict_cols])
        update_str = ', '.join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
        sql = (f'INSERT INTO {q(table_name)} ({cols_str}) VALUES ({placeholders}) '
               f'ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str};')
    else:
        sql = f'INSERT INTO {q(table_name)} ({cols_str}) VALUES ({placeholders});'

    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        execute_batch(cursor, sql, batch, page_size=batch_size)
        conn.commit()
        total += len(batch)
        print(f"    Inserted {total:,}/{len(rows):,} rows...", end='\r')

    print(f"    Inserted {total:,} rows                     ")
    return total


# ============================================================
# DDL: Create all star schema tables
# ============================================================

DDL_SQL = f"""
-- ============================================================
-- Drop existing tables (facts first, then dimensions)
-- ============================================================

DROP TABLE IF EXISTS "{SCHEMA}".fact_bottle_sales CASCADE;
DROP TABLE IF EXISTS "{SCHEMA}".fact_brand_summary CASCADE;
DROP TABLE IF EXISTS "{SCHEMA}".fact_brand_leaders CASCADE;
DROP TABLE IF EXISTS "{SCHEMA}".fact_current_month_by_class CASCADE;
DROP TABLE IF EXISTS "{SCHEMA}".fact_ytd_by_class CASCADE;
DROP TABLE IF EXISTS "{SCHEMA}".fact_l12m_by_class CASCADE;
DROP TABLE IF EXISTS "{SCHEMA}".fact_vendor_performance CASCADE;
DROP TABLE IF EXISTS "{SCHEMA}".fact_vendor_by_class CASCADE;
DROP TABLE IF EXISTS "{SCHEMA}".dim_brand CASCADE;
DROP TABLE IF EXISTS "{SCHEMA}".dim_bottle_size CASCADE;
DROP TABLE IF EXISTS "{SCHEMA}".dim_vendor CASCADE;
DROP TABLE IF EXISTS "{SCHEMA}".dim_class CASCADE;
DROP TABLE IF EXISTS "{SCHEMA}".dim_time CASCADE;

-- ============================================================
-- Dimension Tables
-- ============================================================

CREATE TABLE "{SCHEMA}".dim_vendor (
    vendor_id SERIAL PRIMARY KEY,
    vendor_name TEXT NOT NULL UNIQUE
);

CREATE TABLE "{SCHEMA}".dim_class (
    class_id SERIAL PRIMARY KEY,
    class_name TEXT NOT NULL UNIQUE
);

CREATE TABLE "{SCHEMA}".dim_time (
    time_id SERIAL PRIMARY KEY,
    report_month INTEGER NOT NULL,
    report_year INTEGER NOT NULL,
    report_period TEXT NOT NULL,
    month_name TEXT,
    quarter INTEGER,
    UNIQUE (report_year, report_month)
);

CREATE TABLE "{SCHEMA}".dim_brand (
    brand_id SERIAL PRIMARY KEY,
    brand_name TEXT NOT NULL,
    vendor_id INTEGER REFERENCES "{SCHEMA}".dim_vendor(vendor_id),
    class_id INTEGER REFERENCES "{SCHEMA}".dim_class(class_id),
    UNIQUE (brand_name, vendor_id, class_id)
);

CREATE TABLE "{SCHEMA}".dim_bottle_size (
    bottle_size_id INTEGER PRIMARY KEY,
    bottle_size_name TEXT NOT NULL UNIQUE,
    bottle_size_ml INTEGER,
    sort_order INTEGER
);

-- ============================================================
-- Fact Tables
-- ============================================================

CREATE TABLE "{SCHEMA}".fact_brand_summary (
    fact_id SERIAL PRIMARY KEY,
    brand_id INTEGER REFERENCES "{SCHEMA}".dim_brand(brand_id),
    vendor_id INTEGER REFERENCES "{SCHEMA}".dim_vendor(vendor_id),
    class_id INTEGER REFERENCES "{SCHEMA}".dim_class(class_id),
    time_id INTEGER REFERENCES "{SCHEMA}".dim_time(time_id),
    case_sales_l12m NUMERIC(15, 2),
    case_sales_last_ytd NUMERIC(15, 2),
    ytd_case_sales NUMERIC(15, 2),
    current_month_case_sales NUMERIC(15, 2),
    UNIQUE (brand_id, time_id)
);

CREATE TABLE "{SCHEMA}".fact_bottle_sales (
    fact_id SERIAL PRIMARY KEY,
    brand_id INTEGER REFERENCES "{SCHEMA}".dim_brand(brand_id),
    vendor_id INTEGER REFERENCES "{SCHEMA}".dim_vendor(vendor_id),
    class_id INTEGER REFERENCES "{SCHEMA}".dim_class(class_id),
    time_id INTEGER REFERENCES "{SCHEMA}".dim_time(time_id),
    bottle_size_id INTEGER REFERENCES "{SCHEMA}".dim_bottle_size(bottle_size_id),
    case_sales NUMERIC(15, 2),
    UNIQUE (brand_id, time_id, bottle_size_id)
);

CREATE TABLE "{SCHEMA}".fact_brand_leaders (
    fact_id SERIAL PRIMARY KEY,
    brand_id INTEGER REFERENCES "{SCHEMA}".dim_brand(brand_id),
    class_id INTEGER REFERENCES "{SCHEMA}".dim_class(class_id),
    time_id INTEGER REFERENCES "{SCHEMA}".dim_time(time_id),
    ytd_rank INTEGER,
    ytd_pct_total NUMERIC(10, 4),
    ytd_case_sales NUMERIC(15, 2),
    ytd_change_vs_ly NUMERIC(15, 2),
    current_month_case_sales NUMERIC(15, 2),
    month_change_vs_ly NUMERIC(15, 2),
    l12m_case_sales NUMERIC(15, 2)
);

CREATE TABLE "{SCHEMA}".fact_current_month_by_class (
    fact_id SERIAL PRIMARY KEY,
    class_id INTEGER REFERENCES "{SCHEMA}".dim_class(class_id),
    time_id INTEGER REFERENCES "{SCHEMA}".dim_time(time_id),
    total_cases INTEGER,
    pct_total_spirits NUMERIC(10, 4),
    UNIQUE (class_id, time_id)
);

CREATE TABLE "{SCHEMA}".fact_ytd_by_class (
    fact_id SERIAL PRIMARY KEY,
    class_id INTEGER REFERENCES "{SCHEMA}".dim_class(class_id),
    time_id INTEGER REFERENCES "{SCHEMA}".dim_time(time_id),
    total_ytd_cases INTEGER,
    pct_total_spirits NUMERIC(10, 4),
    UNIQUE (class_id, time_id)
);

CREATE TABLE "{SCHEMA}".fact_l12m_by_class (
    fact_id SERIAL PRIMARY KEY,
    class_id INTEGER REFERENCES "{SCHEMA}".dim_class(class_id),
    time_id INTEGER REFERENCES "{SCHEMA}".dim_time(time_id),
    total_l12m_cases INTEGER,
    pct_total_spirits NUMERIC(10, 4),
    UNIQUE (class_id, time_id)
);

CREATE TABLE "{SCHEMA}".fact_vendor_performance (
    fact_id SERIAL PRIMARY KEY,
    vendor_id INTEGER REFERENCES "{SCHEMA}".dim_vendor(vendor_id),
    time_id INTEGER REFERENCES "{SCHEMA}".dim_time(time_id),
    rank INTEGER,
    market_share NUMERIC(10, 4),
    l12m_cases_this_year INTEGER,
    l12m_cases_prior_year INTEGER,
    l12m_change_pct NUMERIC(10, 4),
    ytd_cases_this_year INTEGER,
    ytd_cases_last_year INTEGER,
    ytd_change_pct NUMERIC(10, 4),
    curr_month_this_year INTEGER,
    curr_month_last_year INTEGER,
    curr_month_change_pct NUMERIC(10, 4),
    UNIQUE (vendor_id, time_id)
);

CREATE TABLE "{SCHEMA}".fact_vendor_by_class (
    fact_id SERIAL PRIMARY KEY,
    vendor_id INTEGER REFERENCES "{SCHEMA}".dim_vendor(vendor_id),
    class_id INTEGER REFERENCES "{SCHEMA}".dim_class(class_id),
    time_id INTEGER REFERENCES "{SCHEMA}".dim_time(time_id),
    rank INTEGER,
    market_share NUMERIC(10, 4),
    l12m_cases_this_year INTEGER,
    l12m_cases_prior_year INTEGER,
    l12m_change_pct NUMERIC(10, 4),
    ytd_cases_this_year INTEGER,
    ytd_cases_last_year INTEGER,
    ytd_change_pct NUMERIC(10, 4),
    curr_month_this_year INTEGER,
    curr_month_last_year INTEGER,
    curr_month_change_pct NUMERIC(10, 4),
    UNIQUE (vendor_id, class_id, time_id)
);

-- ============================================================
-- Row Level Security (allow all access via service role)
-- ============================================================

ALTER TABLE "{SCHEMA}".dim_vendor ENABLE ROW LEVEL SECURITY;
ALTER TABLE "{SCHEMA}".dim_class ENABLE ROW LEVEL SECURITY;
ALTER TABLE "{SCHEMA}".dim_time ENABLE ROW LEVEL SECURITY;
ALTER TABLE "{SCHEMA}".dim_brand ENABLE ROW LEVEL SECURITY;
ALTER TABLE "{SCHEMA}".dim_bottle_size ENABLE ROW LEVEL SECURITY;
ALTER TABLE "{SCHEMA}".fact_brand_summary ENABLE ROW LEVEL SECURITY;
ALTER TABLE "{SCHEMA}".fact_bottle_sales ENABLE ROW LEVEL SECURITY;
ALTER TABLE "{SCHEMA}".fact_brand_leaders ENABLE ROW LEVEL SECURITY;
ALTER TABLE "{SCHEMA}".fact_current_month_by_class ENABLE ROW LEVEL SECURITY;
ALTER TABLE "{SCHEMA}".fact_ytd_by_class ENABLE ROW LEVEL SECURITY;
ALTER TABLE "{SCHEMA}".fact_l12m_by_class ENABLE ROW LEVEL SECURITY;
ALTER TABLE "{SCHEMA}".fact_vendor_performance ENABLE ROW LEVEL SECURITY;
ALTER TABLE "{SCHEMA}".fact_vendor_by_class ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Full access" ON "{SCHEMA}".dim_vendor FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Full access" ON "{SCHEMA}".dim_class FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Full access" ON "{SCHEMA}".dim_time FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Full access" ON "{SCHEMA}".dim_brand FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Full access" ON "{SCHEMA}".dim_bottle_size FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Full access" ON "{SCHEMA}".fact_brand_summary FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Full access" ON "{SCHEMA}".fact_bottle_sales FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Full access" ON "{SCHEMA}".fact_brand_leaders FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Full access" ON "{SCHEMA}".fact_current_month_by_class FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Full access" ON "{SCHEMA}".fact_ytd_by_class FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Full access" ON "{SCHEMA}".fact_l12m_by_class FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Full access" ON "{SCHEMA}".fact_vendor_performance FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Full access" ON "{SCHEMA}".fact_vendor_by_class FOR ALL USING (true) WITH CHECK (true);
"""

# ============================================================
# Index DDL (created after data load)
# ============================================================

INDEX_SQL = f"""
-- Fact brand summary indexes
CREATE INDEX IF NOT EXISTS idx_fbs_brand ON "{SCHEMA}".fact_brand_summary(brand_id);
CREATE INDEX IF NOT EXISTS idx_fbs_vendor ON "{SCHEMA}".fact_brand_summary(vendor_id);
CREATE INDEX IF NOT EXISTS idx_fbs_class ON "{SCHEMA}".fact_brand_summary(class_id);
CREATE INDEX IF NOT EXISTS idx_fbs_time ON "{SCHEMA}".fact_brand_summary(time_id);

-- Fact bottle sales indexes
CREATE INDEX IF NOT EXISTS idx_fbottle_brand ON "{SCHEMA}".fact_bottle_sales(brand_id);
CREATE INDEX IF NOT EXISTS idx_fbottle_time ON "{SCHEMA}".fact_bottle_sales(time_id);
CREATE INDEX IF NOT EXISTS idx_fbottle_size ON "{SCHEMA}".fact_bottle_sales(bottle_size_id);

-- Fact brand leaders indexes
CREATE INDEX IF NOT EXISTS idx_fbl_brand ON "{SCHEMA}".fact_brand_leaders(brand_id);
CREATE INDEX IF NOT EXISTS idx_fbl_class ON "{SCHEMA}".fact_brand_leaders(class_id);
CREATE INDEX IF NOT EXISTS idx_fbl_time ON "{SCHEMA}".fact_brand_leaders(time_id);

-- Class-level fact indexes
CREATE INDEX IF NOT EXISTS idx_fcm_class ON "{SCHEMA}".fact_current_month_by_class(class_id);
CREATE INDEX IF NOT EXISTS idx_fcm_time ON "{SCHEMA}".fact_current_month_by_class(time_id);
CREATE INDEX IF NOT EXISTS idx_fytd_class ON "{SCHEMA}".fact_ytd_by_class(class_id);
CREATE INDEX IF NOT EXISTS idx_fytd_time ON "{SCHEMA}".fact_ytd_by_class(time_id);
CREATE INDEX IF NOT EXISTS idx_fl12m_class ON "{SCHEMA}".fact_l12m_by_class(class_id);
CREATE INDEX IF NOT EXISTS idx_fl12m_time ON "{SCHEMA}".fact_l12m_by_class(time_id);

-- Vendor fact indexes
CREATE INDEX IF NOT EXISTS idx_fvp_vendor ON "{SCHEMA}".fact_vendor_performance(vendor_id);
CREATE INDEX IF NOT EXISTS idx_fvp_time ON "{SCHEMA}".fact_vendor_performance(time_id);
CREATE INDEX IF NOT EXISTS idx_fvbc_vendor ON "{SCHEMA}".fact_vendor_by_class(vendor_id);
CREATE INDEX IF NOT EXISTS idx_fvbc_class ON "{SCHEMA}".fact_vendor_by_class(class_id);
CREATE INDEX IF NOT EXISTS idx_fvbc_time ON "{SCHEMA}".fact_vendor_by_class(time_id);
"""


# ============================================================
# Main
# ============================================================

def main():
    start_time = time.time()

    print("=" * 70)
    print("  NABCA Star Schema Builder - nabca-pre-prod (amber-tree)")
    print("=" * 70)

    # ----------------------------------------------------------
    # Connect
    # ----------------------------------------------------------
    print("\nConnecting to database...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()
        print(f"  Connected to {DB_CONFIG['host']}")
    except Exception as e:
        print(f"  ERROR: Connection failed: {e}")
        sys.exit(1)

    # ----------------------------------------------------------
    # Step 1: Create DDL
    # ----------------------------------------------------------
    print("\n[Step 1/7] Creating star schema tables...")
    try:
        cursor.execute(DDL_SQL)
        conn.commit()
        print("  Created 5 dimension + 8 fact tables with RLS policies")
    except Exception as e:
        conn.rollback()
        print(f"  ERROR creating tables: {e}")
        sys.exit(1)

    # ----------------------------------------------------------
    # Step 2: Populate dim_bottle_size
    # ----------------------------------------------------------
    print("\n[Step 2/7] Populating dim_bottle_size...")
    bottle_rows = [
        (bs['bottle_size_id'], bs['bottle_size_name'], bs['bottle_size_ml'], bs['sort_order'])
        for bs in BOTTLE_SIZES
    ]
    insert_batched(cursor, conn, 'dim_bottle_size',
                   ['bottle_size_id', 'bottle_size_name', 'bottle_size_ml', 'sort_order'],
                   bottle_rows, batch_size=DIM_BATCH_SIZE)

    # ----------------------------------------------------------
    # Step 3: Fetch all raw data
    # ----------------------------------------------------------
    print("\n[Step 3/7] Fetching raw data from 8 tables...")

    raw_tables = {}
    for table in ['raw_brand_summary', 'raw_brand_leaders', 'raw_current_month',
                   'raw_ytd', 'raw_rolling_12m', 'raw_top100_vendors',
                   'raw_top20_vendors_by_class', 'raw_vendor_summary']:
        print(f"  Fetching {table}...", end=' ')
        data = fetch_table(cursor, table)
        raw_tables[table] = data
        print(f"{len(data):,} rows")

    # ----------------------------------------------------------
    # Step 4: Build & populate dimensions
    # ----------------------------------------------------------
    print("\n[Step 4/7] Building dimension tables...")

    # --- dim_vendor ---
    print("\n  Building dim_vendor...")
    vendors = set()
    for row in raw_tables['raw_brand_summary']:
        if row.get('vendor'):
            vendors.add(str(row['vendor']).strip())
    for row in raw_tables['raw_vendor_summary']:
        if row.get('vendor'):
            vendors.add(str(row['vendor']).strip())
    for row in raw_tables['raw_top100_vendors']:
        if row.get('vendor_name'):
            vendors.add(str(row['vendor_name']).strip())
    for row in raw_tables['raw_top20_vendors_by_class']:
        if row.get('vendor_name'):
            vendors.add(str(row['vendor_name']).strip())
    vendors.discard('')

    vendor_rows = [(v,) for v in sorted(vendors)]
    insert_batched(cursor, conn, 'dim_vendor', ['vendor_name'],
                   vendor_rows, batch_size=DIM_BATCH_SIZE)
    print(f"    {len(vendor_rows):,} unique vendors")

    # --- dim_class ---
    print("\n  Building dim_class...")
    classes = set()
    for row in raw_tables['raw_brand_summary']:
        if row.get('class'):
            classes.add(str(row['class']).strip())
    for row in raw_tables['raw_current_month']:
        if row.get('class_name'):
            classes.add(str(row['class_name']).strip())
    for row in raw_tables['raw_ytd']:
        if row.get('class_name'):
            classes.add(str(row['class_name']).strip())
    for row in raw_tables['raw_rolling_12m']:
        if row.get('class_name'):
            classes.add(str(row['class_name']).strip())
    for row in raw_tables['raw_brand_leaders']:
        if row.get('type'):
            classes.add(str(row['type']).strip())
    for row in raw_tables['raw_top20_vendors_by_class']:
        if row.get('class'):
            classes.add(str(row['class']).strip())
    for row in raw_tables['raw_vendor_summary']:
        if row.get('class'):
            classes.add(str(row['class']).strip())
    classes.discard('')

    class_rows = [(c,) for c in sorted(classes)]
    insert_batched(cursor, conn, 'dim_class', ['class_name'],
                   class_rows, batch_size=DIM_BATCH_SIZE)
    print(f"    {len(class_rows):,} unique classes")

    # --- dim_time ---
    print("\n  Building dim_time...")
    periods = set()
    for table_data in raw_tables.values():
        for row in table_data:
            y = row.get('report_year')
            m = row.get('report_month')
            if y and m:
                periods.add((int(y), int(m)))

    time_rows = []
    for year, month in sorted(periods):
        quarter = (month - 1) // 3 + 1
        report_period = f"{year}-{str(month).zfill(2)}"
        time_rows.append((month, year, report_period, get_month_name(month), quarter))

    insert_batched(cursor, conn, 'dim_time',
                   ['report_month', 'report_year', 'report_period', 'month_name', 'quarter'],
                   time_rows, batch_size=DIM_BATCH_SIZE)
    print(f"    {len(time_rows):,} time periods")

    # --- Fetch dimension lookups ---
    print("\n  Loading dimension lookups...")

    cursor.execute(f'SELECT vendor_id, vendor_name FROM {q("dim_vendor")};')
    vendor_ids = {row[1]: row[0] for row in cursor.fetchall()}
    print(f"    {len(vendor_ids):,} vendor IDs loaded")

    cursor.execute(f'SELECT class_id, class_name FROM {q("dim_class")};')
    class_ids = {row[1]: row[0] for row in cursor.fetchall()}
    print(f"    {len(class_ids):,} class IDs loaded")

    cursor.execute(f'SELECT time_id, report_year, report_month FROM {q("dim_time")};')
    time_ids = {(row[1], row[2]): row[0] for row in cursor.fetchall()}
    print(f"    {len(time_ids):,} time IDs loaded")

    # --- dim_brand ---
    print("\n  Building dim_brand...")
    brands = set()  # (brand_name, vendor_id, class_id)
    for row in raw_tables['raw_brand_summary']:
        brand_name = str(row.get('brand', '') or '').strip()
        vendor_name = str(row.get('vendor', '') or '').strip()
        class_name = str(row.get('class', '') or '').strip()
        if brand_name and vendor_name and class_name:
            vid = vendor_ids.get(vendor_name)
            cid = class_ids.get(class_name)
            if vid and cid:
                brands.add((brand_name, vid, cid))

    # Also from vendor_summary (has brand, vendor, class)
    for row in raw_tables['raw_vendor_summary']:
        brand_name = str(row.get('brand', '') or '').strip()
        vendor_name = str(row.get('vendor', '') or '').strip()
        class_name = str(row.get('class', '') or '').strip()
        if brand_name and vendor_name and class_name:
            vid = vendor_ids.get(vendor_name)
            cid = class_ids.get(class_name)
            if vid and cid:
                brands.add((brand_name, vid, cid))

    brand_rows = [(b[0], b[1], b[2]) for b in sorted(brands)]
    insert_batched(cursor, conn, 'dim_brand',
                   ['brand_name', 'vendor_id', 'class_id'],
                   brand_rows, batch_size=DIM_BATCH_SIZE)
    print(f"    {len(brand_rows):,} unique brands")

    # Fetch brand lookups
    cursor.execute(f'SELECT brand_id, brand_name, vendor_id, class_id FROM {q("dim_brand")};')
    brand_ids = {}
    for row in cursor.fetchall():
        brand_ids[(row[1], row[2], row[3])] = row[0]
    print(f"    {len(brand_ids):,} brand IDs loaded")

    # ----------------------------------------------------------
    # Step 5: Build & populate fact tables
    # ----------------------------------------------------------
    print("\n[Step 5/7] Building fact tables...")

    # --- fact_brand_summary ---
    print("\n  Building fact_brand_summary...")
    fact_rows = []
    skipped = 0
    for row in raw_tables['raw_brand_summary']:
        brand_name = str(row.get('brand', '') or '').strip()
        vendor_name = str(row.get('vendor', '') or '').strip()
        class_name = str(row.get('class', '') or '').strip()
        year = row.get('report_year')
        month = row.get('report_month')

        if not all([brand_name, vendor_name, class_name, year, month]):
            skipped += 1
            continue

        vid = vendor_ids.get(vendor_name)
        cid = class_ids.get(class_name)
        tid = time_ids.get((int(year), int(month)))
        bid = brand_ids.get((brand_name, vid, cid)) if vid and cid else None

        if not all([vid, cid, tid, bid]):
            skipped += 1
            continue

        fact_rows.append((
            bid, vid, cid, tid,
            row.get('l12m_cases_ty'),
            row.get('l12m_cases_ly'),
            row.get('ytd_cases_ty'),
            row.get('curr_month_cases'),
        ))

    insert_batched(cursor, conn, 'fact_brand_summary',
                   ['brand_id', 'vendor_id', 'class_id', 'time_id',
                    'case_sales_l12m', 'case_sales_last_ytd',
                    'ytd_case_sales', 'current_month_case_sales'],
                   fact_rows, batch_size=FACT_BATCH_SIZE)
    if skipped:
        print(f"    ({skipped:,} rows skipped due to missing dimension keys)")

    # --- fact_bottle_sales ---
    print("\n  Building fact_bottle_sales (normalized from brand_summary bottle columns)...")
    bottle_rows = []
    for row in raw_tables['raw_brand_summary']:
        brand_name = str(row.get('brand', '') or '').strip()
        vendor_name = str(row.get('vendor', '') or '').strip()
        class_name = str(row.get('class', '') or '').strip()
        year = row.get('report_year')
        month = row.get('report_month')

        if not all([brand_name, vendor_name, class_name, year, month]):
            continue

        vid = vendor_ids.get(vendor_name)
        cid = class_ids.get(class_name)
        tid = time_ids.get((int(year), int(month)))
        bid = brand_ids.get((brand_name, vid, cid)) if vid and cid else None

        if not all([vid, cid, tid, bid]):
            continue

        for col_name, bsize_id in BOTTLE_COLUMN_MAP.items():
            val = row.get(col_name)
            if val is not None and val != 0:
                bottle_rows.append((bid, vid, cid, tid, bsize_id, val))

    insert_batched(cursor, conn, 'fact_bottle_sales',
                   ['brand_id', 'vendor_id', 'class_id', 'time_id',
                    'bottle_size_id', 'case_sales'],
                   bottle_rows, batch_size=FACT_BATCH_SIZE)

    # --- fact_brand_leaders ---
    print("\n  Building fact_brand_leaders...")
    # Brand leaders has brand + type but no vendor. We match via (brand_name, class_id).
    brand_by_name_class = {}
    for (bname, vid, cid), bid in brand_ids.items():
        key = (bname, cid)
        if key not in brand_by_name_class:
            brand_by_name_class[key] = bid

    leader_rows = []
    skipped = 0
    for row in raw_tables['raw_brand_leaders']:
        brand_name = str(row.get('brand', '') or '').strip()
        type_name = str(row.get('type', '') or '').strip()
        year = row.get('report_year')
        month = row.get('report_month')

        if not all([brand_name, type_name, year, month]):
            skipped += 1
            continue

        cid = class_ids.get(type_name)
        tid = time_ids.get((int(year), int(month)))
        bid = brand_by_name_class.get((brand_name, cid)) if cid else None

        if not all([cid, tid]):
            skipped += 1
            continue

        leader_rows.append((
            bid,  # may be None if brand not in brand_summary
            cid, tid,
            clean_int(row.get('rank')),
            clean_numeric(row.get('pct_total')),
            clean_numeric(row.get('ytd_case_sales')),
            clean_numeric(row.get('ytd_change_vs_ly')),
            clean_numeric(row.get('current_month_case_sales')),
            clean_numeric(row.get('month_change_vs_ly')),
            clean_numeric(row.get('l12m_case_sales')),
        ))

    insert_batched(cursor, conn, 'fact_brand_leaders',
                   ['brand_id', 'class_id', 'time_id',
                    'ytd_rank', 'ytd_pct_total', 'ytd_case_sales',
                    'ytd_change_vs_ly', 'current_month_case_sales',
                    'month_change_vs_ly', 'l12m_case_sales'],
                   leader_rows, batch_size=FACT_BATCH_SIZE)
    if skipped:
        print(f"    ({skipped:,} rows skipped due to missing dimension keys)")

    # --- fact_current_month_by_class ---
    print("\n  Building fact_current_month_by_class...")
    cm_rows = []
    skipped = 0
    for row in raw_tables['raw_current_month']:
        class_name = str(row.get('class_name', '') or '').strip()
        year = row.get('report_year')
        month = row.get('report_month')

        if not all([class_name, year, month]):
            skipped += 1
            continue

        cid = class_ids.get(class_name)
        tid = time_ids.get((int(year), int(month)))

        if not all([cid, tid]):
            skipped += 1
            continue

        cm_rows.append((
            cid, tid,
            row.get('total_cases'),
            row.get('pct_of_class'),
        ))

    insert_batched(cursor, conn, 'fact_current_month_by_class',
                   ['class_id', 'time_id', 'total_cases', 'pct_total_spirits'],
                   cm_rows, batch_size=FACT_BATCH_SIZE)
    if skipped:
        print(f"    ({skipped:,} rows skipped)")

    # --- fact_ytd_by_class ---
    print("\n  Building fact_ytd_by_class...")
    ytd_rows = []
    skipped = 0
    for row in raw_tables['raw_ytd']:
        class_name = str(row.get('class_name', '') or '').strip()
        year = row.get('report_year')
        month = row.get('report_month')

        if not all([class_name, year, month]):
            skipped += 1
            continue

        cid = class_ids.get(class_name)
        tid = time_ids.get((int(year), int(month)))

        if not all([cid, tid]):
            skipped += 1
            continue

        ytd_rows.append((
            cid, tid,
            row.get('ytd_total_cases'),
            row.get('pct_of_class'),
        ))

    insert_batched(cursor, conn, 'fact_ytd_by_class',
                   ['class_id', 'time_id', 'total_ytd_cases', 'pct_total_spirits'],
                   ytd_rows, batch_size=FACT_BATCH_SIZE)
    if skipped:
        print(f"    ({skipped:,} rows skipped)")

    # --- fact_l12m_by_class ---
    print("\n  Building fact_l12m_by_class...")
    l12m_rows = []
    skipped = 0
    for row in raw_tables['raw_rolling_12m']:
        class_name = str(row.get('class_name', '') or '').strip()
        year = row.get('report_year')
        month = row.get('report_month')

        if not all([class_name, year, month]):
            skipped += 1
            continue

        cid = class_ids.get(class_name)
        tid = time_ids.get((int(year), int(month)))

        if not all([cid, tid]):
            skipped += 1
            continue

        l12m_rows.append((
            cid, tid,
            row.get('r12m_total_cases'),
            row.get('pct_of_class'),
        ))

    insert_batched(cursor, conn, 'fact_l12m_by_class',
                   ['class_id', 'time_id', 'total_l12m_cases', 'pct_total_spirits'],
                   l12m_rows, batch_size=FACT_BATCH_SIZE)
    if skipped:
        print(f"    ({skipped:,} rows skipped)")

    # --- fact_vendor_performance ---
    print("\n  Building fact_vendor_performance...")
    vp_rows = []
    skipped = 0
    for row in raw_tables['raw_top100_vendors']:
        vendor_name = str(row.get('vendor_name', '') or '').strip()
        year = row.get('report_year')
        month = row.get('report_month')

        if not all([vendor_name, year, month]):
            skipped += 1
            continue

        vid = vendor_ids.get(vendor_name)
        tid = time_ids.get((int(year), int(month)))

        if not all([vid, tid]):
            skipped += 1
            continue

        vp_rows.append((
            vid, tid,
            row.get('rank'),
            row.get('market_share'),
            row.get('l12m_cases_this_year'),
            row.get('l12m_cases_prior_year'),
            row.get('l12m_change_pct'),
            row.get('ytd_cases_this_year'),
            row.get('ytd_cases_last_year'),
            row.get('ytd_change_pct'),
            row.get('curr_month_this_year'),
            row.get('curr_month_last_year'),
            row.get('curr_month_change_pct'),
        ))

    insert_batched(cursor, conn, 'fact_vendor_performance',
                   ['vendor_id', 'time_id', 'rank', 'market_share',
                    'l12m_cases_this_year', 'l12m_cases_prior_year', 'l12m_change_pct',
                    'ytd_cases_this_year', 'ytd_cases_last_year', 'ytd_change_pct',
                    'curr_month_this_year', 'curr_month_last_year', 'curr_month_change_pct'],
                   vp_rows, batch_size=FACT_BATCH_SIZE)
    if skipped:
        print(f"    ({skipped:,} rows skipped)")

    # --- fact_vendor_by_class ---
    print("\n  Building fact_vendor_by_class...")
    vbc_rows = []
    skipped = 0
    for row in raw_tables['raw_top20_vendors_by_class']:
        vendor_name = str(row.get('vendor_name', '') or '').strip()
        class_name = str(row.get('class', '') or '').strip()
        year = row.get('report_year')
        month = row.get('report_month')

        if not all([vendor_name, class_name, year, month]):
            skipped += 1
            continue

        vid = vendor_ids.get(vendor_name)
        cid = class_ids.get(class_name)
        tid = time_ids.get((int(year), int(month)))

        if not all([vid, cid, tid]):
            skipped += 1
            continue

        vbc_rows.append((
            vid, cid, tid,
            row.get('rank'),
            row.get('market_share'),
            row.get('l12m_cases_this_year'),
            row.get('l12m_cases_prior_year'),
            row.get('l12m_change_pct'),
            row.get('ytd_cases_this_year'),
            row.get('ytd_cases_last_year'),
            row.get('ytd_change_pct'),
            row.get('curr_month_this_year'),
            row.get('curr_month_last_year'),
            row.get('curr_month_change_pct'),
        ))

    insert_batched(cursor, conn, 'fact_vendor_by_class',
                   ['vendor_id', 'class_id', 'time_id', 'rank', 'market_share',
                    'l12m_cases_this_year', 'l12m_cases_prior_year', 'l12m_change_pct',
                    'ytd_cases_this_year', 'ytd_cases_last_year', 'ytd_change_pct',
                    'curr_month_this_year', 'curr_month_last_year', 'curr_month_change_pct'],
                   vbc_rows, batch_size=FACT_BATCH_SIZE)
    if skipped:
        print(f"    ({skipped:,} rows skipped)")

    # ----------------------------------------------------------
    # Step 6: Create indexes
    # ----------------------------------------------------------
    print("\n[Step 6/7] Creating indexes...")
    try:
        cursor.execute(INDEX_SQL)
        conn.commit()
        print("  Indexes created")
    except Exception as e:
        conn.rollback()
        print(f"  WARNING: Index creation failed: {e}")

    # ----------------------------------------------------------
    # Step 7: Verify row counts
    # ----------------------------------------------------------
    print("\n[Step 7/7] Verifying row counts...")
    print()

    star_tables = [
        'dim_vendor', 'dim_class', 'dim_time', 'dim_brand', 'dim_bottle_size',
        'fact_brand_summary', 'fact_bottle_sales', 'fact_brand_leaders',
        'fact_current_month_by_class', 'fact_ytd_by_class', 'fact_l12m_by_class',
        'fact_vendor_performance', 'fact_vendor_by_class',
    ]

    print(f"  {'Table':<35} {'Rows':>12}")
    print(f"  {'-'*35} {'-'*12}")
    for tbl in star_tables:
        cursor.execute(f'SELECT COUNT(*) FROM {q(tbl)};')
        count = cursor.fetchone()[0]
        print(f"  {tbl:<35} {count:>12,}")

    # Raw table comparison
    print()
    print(f"  {'Raw Table':<35} {'Rows':>12}")
    print(f"  {'-'*35} {'-'*12}")
    for tbl_name, data in raw_tables.items():
        print(f"  {tbl_name:<35} {len(data):>12,}")

    # ----------------------------------------------------------
    # Cleanup
    # ----------------------------------------------------------
    cursor.close()
    conn.close()

    elapsed = time.time() - start_time
    print(f"\n{'=' * 70}")
    print(f"  Star schema build complete in {elapsed:.1f}s")
    print(f"{'=' * 70}")


if __name__ == '__main__':
    main()
