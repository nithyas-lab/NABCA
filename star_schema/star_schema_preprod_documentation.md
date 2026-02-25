# NABCA Star Schema Documentation — Pre-Prod

## Overview

This document describes the star schema design for the NABCA (National Alcohol Beverage Control Association) spirits sales data. The star schema transforms 8 raw extraction tables into 5 dimension tables and 8 fact tables optimized for analytics queries.

**Script**: `build_star_schema_preprod.py`
**Database**: Supabase PostgreSQL (amber-tree — `xhvsvhiysnacdinclncn`)
**Schema**: Configurable via `SCHEMA` variable (default: `nabca-pre-prod`)
**Connection**: psycopg2 (direct PostgreSQL connection)

---

## Why a Star Schema?

The raw tables repeat text values in every row — e.g., the vendor name `"DIAGEO"` appears in 10,000+ rows. A star schema replaces repeated text with integer foreign keys and stores the text once in a dimension lookup table.

**Benefits:**
- Faster queries (join on integers instead of text comparison)
- Smaller storage footprint (IDs vs repeated strings)
- Consistent naming (one source of truth per entity)
- Easier aggregation (GROUP BY dimension keys, filter by dimension attributes)

**Before (raw):**
```
| brand          | vendor       | class              | report_year | report_month | l12m_cases_ty |
|----------------|-------------|--------------------|-------------|--------------|---------------|
| TITOS HANDMADE | FIFTH GEN   | VODKA 80 PRF       | 2025        | 7            | 245000        |
| TITOS HANDMADE | FIFTH GEN   | VODKA 80 PRF       | 2025        | 8            | 248000        |
```

**After (star):**
```
dim_brand:  brand_id=42, brand_name="TITOS HANDMADE", vendor_id=15, class_id=7
dim_vendor: vendor_id=15, vendor_name="FIFTH GEN"
dim_class:  class_id=7,  class_name="VODKA 80 PRF"
dim_time:   time_id=3,   report_year=2025, report_month=7

fact_brand_summary: brand_id=42, vendor_id=15, class_id=7, time_id=3, case_sales_l12m=245000
```

---

## Schema Configuration

The script uses a single variable to control which schema all tables are created in:

```python
SCHEMA = 'nabca-pre-prod'   # Change this to target a different schema
```

All DDL, inserts, and queries use this variable. To deploy to a different schema (e.g., `nabca-prod`, `nabca-star`), change this one line and re-run.

---

## ER Diagram

```
                            ┌──────────────────────┐
                            │      dim_time         │
                            │──────────────────────│
                            │ time_id (PK)          │
                            │ report_month          │
                            │ report_year           │
                            │ report_period          │
                            │ month_name            │
                            │ quarter               │
                            └──────────┬────────────┘
                                       │
           ┌───────────────────────────┼──────────────────────────────┐
           │                           │                              │
           ▼                           ▼                              ▼
┌────────────────────┐     ┌────────────────────┐       ┌───────────────────────┐
│    dim_vendor      │     │     dim_class       │       │   dim_bottle_size     │
│────────────────────│     │────────────────────│       │───────────────────────│
│ vendor_id (PK)     │     │ class_id (PK)       │       │ bottle_size_id (PK)   │
│ vendor_name (UQ)   │     │ class_name (UQ)     │       │ bottle_size_name (UQ) │
└────────┬───────────┘     └─────────┬──────────┘       │ bottle_size_ml        │
         │                           │                   │ sort_order            │
         │              ┌────────────┴──────────┐        └───────────┬──────────┘
         │              │                       │                    │
         ▼              ▼                       │                    │
┌──────────────────────────────────┐            │                    │
│          dim_brand               │            │                    │
│──────────────────────────────────│            │                    │
│ brand_id (PK)                    │            │                    │
│ brand_name                       │            │                    │
│ vendor_id (FK → dim_vendor)      │            │                    │
│ class_id  (FK → dim_class)       │            │                    │
│ UNIQUE(brand_name,vendor,class)  │            │                    │
└──────────────┬───────────────────┘            │                    │
               │                                │                    │
               ▼                                ▼                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                              FACT TABLES                                     │
│                                                                              │
│  fact_brand_summary ──────── FK → dim_brand, dim_vendor, dim_class, dim_time │
│  fact_bottle_sales  ──────── FK → dim_brand, dim_vendor, dim_class,          │
│                                    dim_time, dim_bottle_size                 │
│  fact_brand_leaders ──────── FK → dim_brand, dim_class, dim_time             │
│  fact_current_month_by_class FK → dim_class, dim_time                        │
│  fact_ytd_by_class ───────── FK → dim_class, dim_time                        │
│  fact_l12m_by_class ──────── FK → dim_class, dim_time                        │
│  fact_vendor_performance ─── FK → dim_vendor, dim_time                       │
│  fact_vendor_by_class ────── FK → dim_vendor, dim_class, dim_time            │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Dimension Tables (5)

### 1. dim_vendor

Unique vendor (supplier/distributor) names collected from brand_summary, vendor_summary, top100_vendors, and top20_vendors_by_class.

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `vendor_id` | SERIAL | PRIMARY KEY | Auto-generated surrogate key |
| `vendor_name` | TEXT | NOT NULL, UNIQUE | Vendor name as extracted from PDFs |

**Source raw tables:** `raw_brand_summary.vendor`, `raw_vendor_summary.vendor`, `raw_top100_vendors.vendor_name`, `raw_top20_vendors_by_class.vendor_name`

**Referenced by:** fact_brand_summary, fact_bottle_sales, fact_vendor_performance, fact_vendor_by_class, dim_brand

---

### 2. dim_class

Unique spirit class/category names (e.g., "VODKA 80 PRF", "DOM WHSKY-STRT-BRBN") collected from all tables that have class information.

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `class_id` | SERIAL | PRIMARY KEY | Auto-generated surrogate key |
| `class_name` | TEXT | NOT NULL, UNIQUE | Spirit class name from PDFs |

**Source raw tables:** `raw_brand_summary.class`, `raw_current_month.class_name`, `raw_ytd.class_name`, `raw_rolling_12m.class_name`, `raw_brand_leaders.type`, `raw_top20_vendors_by_class.class`, `raw_vendor_summary.class`

**Referenced by:** fact_brand_summary, fact_bottle_sales, fact_brand_leaders, fact_current_month_by_class, fact_ytd_by_class, fact_l12m_by_class, fact_vendor_by_class, dim_brand

---

### 3. dim_time

Unique reporting periods. Each row is one (year, month) combination. Derived from all 8 raw tables.

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `time_id` | SERIAL | PRIMARY KEY | Auto-generated surrogate key |
| `report_month` | INTEGER | NOT NULL | Month number (1-12) |
| `report_year` | INTEGER | NOT NULL | 4-digit year |
| `report_period` | TEXT | NOT NULL | Formatted as `"YYYY-MM"` (e.g., `"2025-07"`) |
| `month_name` | TEXT | | Full month name (e.g., `"July"`) |
| `quarter` | INTEGER | | Calendar quarter (1-4) |
| | | UNIQUE(report_year, report_month) | One row per year-month |

**Data range:** July 2024 through December 2025 (18 months)

**Referenced by:** All 8 fact tables

---

### 4. dim_brand

Unique brands identified by the combination of (brand_name, vendor, class). The same brand name under different vendors or classes is treated as a separate entity.

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `brand_id` | SERIAL | PRIMARY KEY | Auto-generated surrogate key |
| `brand_name` | TEXT | NOT NULL | Brand name as extracted from PDFs |
| `vendor_id` | INTEGER | FK → dim_vendor | The vendor that owns this brand |
| `class_id` | INTEGER | FK → dim_class | The spirit class this brand belongs to |
| | | UNIQUE(brand_name, vendor_id, class_id) | One row per brand-vendor-class combo |

**Source raw tables:** `raw_brand_summary` (brand + vendor + class), `raw_vendor_summary` (brand + vendor + class)

**Referenced by:** fact_brand_summary, fact_bottle_sales, fact_brand_leaders

**Note:** `raw_brand_leaders` has brand + type (class) but no vendor. For fact_brand_leaders, the script uses a secondary lookup `(brand_name, class_id)` → `brand_id`, matching to the first brand_id found. If a brand exists under multiple vendors in the same class, the match may be arbitrary. brand_id can be NULL in fact_brand_leaders if the brand doesn't exist in brand_summary/vendor_summary.

---

### 5. dim_bottle_size

Pre-populated reference table for the 8 standard bottle sizes used in NABCA reporting. Not derived from raw data — these are static values.

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `bottle_size_id` | INTEGER | PRIMARY KEY | Fixed ID (1-8) |
| `bottle_size_name` | TEXT | NOT NULL, UNIQUE | Display name |
| `bottle_size_ml` | INTEGER | | Size in milliliters |
| `sort_order` | INTEGER | | Display sort order (1=largest) |

**Pre-populated values:**

| ID | Name | ML | Sort |
|----|------|----|------|
| 1 | 1.75L | 1750 | 1 |
| 2 | 1.0L | 1000 | 2 |
| 3 | 750ml | 750 | 3 |
| 4 | 750ml Traveler | 750 | 4 |
| 5 | 375ml | 375 | 5 |
| 6 | 200ml | 200 | 6 |
| 7 | 100ml | 100 | 7 |
| 8 | 50ml | 50 | 8 |

**Referenced by:** fact_bottle_sales

---

## Fact Tables (8)

### 1. fact_brand_summary

**Source:** `raw_brand_summary` (413K+ rows)
**Grain:** One row per brand per month
**Purpose:** Core sales metrics — L12M cases, YTD cases, and current month cases per brand

| Column | Type | Constraint | Source Raw Column |
|--------|------|------------|-------------------|
| `fact_id` | SERIAL | PRIMARY KEY | — |
| `brand_id` | INTEGER | FK → dim_brand | `brand` + `vendor` + `class` → dim_brand lookup |
| `vendor_id` | INTEGER | FK → dim_vendor | `vendor` → dim_vendor lookup |
| `class_id` | INTEGER | FK → dim_class | `class` → dim_class lookup |
| `time_id` | INTEGER | FK → dim_time | `(report_year, report_month)` → dim_time lookup |
| `case_sales_l12m` | NUMERIC(15,2) | | `l12m_cases_ty` (INTEGER in raw) |
| `case_sales_last_ytd` | NUMERIC(15,2) | | `l12m_cases_ly` (INTEGER in raw) |
| `ytd_case_sales` | NUMERIC(15,2) | | `ytd_cases_ty` (INTEGER in raw) |
| `current_month_case_sales` | NUMERIC(15,2) | | `curr_month_cases` (INTEGER in raw) |
| | | UNIQUE(brand_id, time_id) | Dedup constraint |

**Duplicate handling:** Raw data has 5 known duplicate (brand, vendor, class, year, month) groups. The script uses `ON CONFLICT (brand_id, time_id) DO UPDATE` to keep the last-seen values.

**Indexes:** brand_id, vendor_id, class_id, time_id

---

### 2. fact_bottle_sales

**Source:** `raw_brand_summary` (normalized from 8 wide bottle columns)
**Grain:** One row per brand per month per bottle size
**Purpose:** Bottle-size breakdown of current month sales

| Column | Type | Constraint | Source |
|--------|------|------------|--------|
| `fact_id` | SERIAL | PRIMARY KEY | — |
| `brand_id` | INTEGER | FK → dim_brand | Same lookup as fact_brand_summary |
| `vendor_id` | INTEGER | FK → dim_vendor | Same lookup |
| `class_id` | INTEGER | FK → dim_class | Same lookup |
| `time_id` | INTEGER | FK → dim_time | Same lookup |
| `bottle_size_id` | INTEGER | FK → dim_bottle_size | Determined by which column the value came from |
| `case_sales` | NUMERIC(15,2) | | The bottle column's value |
| | | UNIQUE(brand_id, time_id, bottle_size_id) | Dedup constraint |

**Normalization logic:**

Each `raw_brand_summary` row has 8 bottle columns. These get "unpivoted" into separate rows:

```
Raw row:  curr_month_175l=500, curr_month_1l=NULL, curr_month_750ml=200, ...rest NULL/0

Becomes:
  (brand_id, time_id, bottle_size_id=1, case_sales=500)   ← 1.75L
  (brand_id, time_id, bottle_size_id=3, case_sales=200)   ← 750ml
```

**Column → bottle_size_id mapping:**

| Raw Column | bottle_size_id | Bottle Size |
|------------|----------------|-------------|
| `curr_month_175l` | 1 | 1.75L |
| `curr_month_1l` | 2 | 1.0L |
| `curr_month_750ml` | 3 | 750ml |
| `curr_month_750ml_traveler` | 4 | 750ml Traveler |
| `curr_month_375ml` | 5 | 375ml |
| `curr_month_200ml` | 6 | 200ml |
| `curr_month_100ml` | 7 | 100ml |
| `curr_month_50ml` | 8 | 50ml |

**NULL/zero exclusion:** Rows are only created where the bottle column value is NOT NULL and NOT 0. This keeps the table lean.

**Duplicate handling:** Uses `ON CONFLICT (brand_id, time_id, bottle_size_id) DO UPDATE`.

**Indexes:** brand_id, time_id, bottle_size_id

---

### 3. fact_brand_leaders

**Source:** `raw_brand_leaders`
**Grain:** One row per brand per class per month (top-ranked brands)
**Purpose:** YTD and L12M rankings for leading brands within each spirit class

| Column | Type | Constraint | Source Raw Column |
|--------|------|------------|-------------------|
| `fact_id` | SERIAL | PRIMARY KEY | — |
| `brand_id` | INTEGER | FK → dim_brand (nullable) | `brand` → lookup by (brand_name, class_id) |
| `class_id` | INTEGER | FK → dim_class | `type` → dim_class lookup |
| `time_id` | INTEGER | FK → dim_time | `(report_year, report_month)` |
| `ytd_rank` | INTEGER | | `rank` (TEXT → INT via clean_int) |
| `ytd_pct_total` | NUMERIC(10,4) | | `pct_total` (TEXT → NUMERIC via clean_numeric) |
| `ytd_case_sales` | NUMERIC(15,2) | | `ytd_case_sales` (TEXT → NUMERIC) |
| `ytd_change_vs_ly` | NUMERIC(15,2) | | `ytd_change_vs_ly` (TEXT → NUMERIC) |
| `current_month_case_sales` | NUMERIC(15,2) | | `current_month_case_sales` (TEXT → NUMERIC) |
| `month_change_vs_ly` | NUMERIC(15,2) | | `month_change_vs_ly` (TEXT → NUMERIC) |
| `l12m_case_sales` | NUMERIC(15,2) | | `l12m_case_sales` (TEXT → NUMERIC) |

**No UNIQUE constraint** — brand_leaders can have legitimate duplicates (same brand re-ranked in different contexts).

**TEXT → NUMERIC conversion:** All metric columns in `raw_brand_leaders` are stored as TEXT (due to Textract OCR). The script applies `clean_numeric()` / `clean_int()` which strips commas, percent signs, and common OCR artifacts (e.g., `". O"` → `"0"`).

**brand_id can be NULL:** `raw_brand_leaders` has brand + type but no vendor. The script does a best-effort match via `(brand_name, class_id)`. If a brand exists in brand_leaders but not in brand_summary/vendor_summary, `brand_id` will be NULL.

**Indexes:** brand_id, class_id, time_id

---

### 4. fact_current_month_by_class

**Source:** `raw_current_month` (~1,422 rows)
**Grain:** One row per class per month
**Purpose:** Current month total cases and percentage by spirit class

| Column | Type | Constraint | Source Raw Column |
|--------|------|------------|-------------------|
| `fact_id` | SERIAL | PRIMARY KEY | — |
| `class_id` | INTEGER | FK → dim_class | `class_name` → dim_class lookup |
| `time_id` | INTEGER | FK → dim_time | `(report_year, report_month)` |
| `total_cases` | INTEGER | | `total_cases` (INTEGER in raw) |
| `pct_total_spirits` | NUMERIC(10,4) | | `pct_of_class` (NUMERIC in raw) |
| | | UNIQUE(class_id, time_id) | One row per class per month |

**Note on `pct_total_spirits`:** This column is mapped from `raw_current_month.pct_of_class`, which represents the percentage share within the class category (avg ~33%). The raw table also has a separate column `pct_total_dist_spirits` (avg ~8%, only populated for ~14% of rows) which represents the percentage of total distilled spirits. The current mapping uses `pct_of_class`. If the pipeline needs the total-spirits percentage instead, change the source column to `pct_total_dist_spirits`.

**Indexes:** class_id, time_id

---

### 5. fact_ytd_by_class

**Source:** `raw_ytd` (~1,338 rows)
**Grain:** One row per class per month
**Purpose:** Year-to-date total cases and percentage by spirit class

| Column | Type | Constraint | Source Raw Column |
|--------|------|------------|-------------------|
| `fact_id` | SERIAL | PRIMARY KEY | — |
| `class_id` | INTEGER | FK → dim_class | `class_name` → dim_class lookup |
| `time_id` | INTEGER | FK → dim_time | `(report_year, report_month)` |
| `total_ytd_cases` | INTEGER | | `ytd_total_cases` (INTEGER in raw) |
| `pct_total_spirits` | NUMERIC(10,4) | | `pct_of_class` (NUMERIC in raw) |
| | | UNIQUE(class_id, time_id) | One row per class per month |

**Same `pct_of_class` note as fact_current_month_by_class above.**

**Indexes:** class_id, time_id

---

### 6. fact_l12m_by_class

**Source:** `raw_rolling_12m` (~1,578 rows)
**Grain:** One row per class per month
**Purpose:** Rolling 12-month total cases and percentage by spirit class

| Column | Type | Constraint | Source Raw Column |
|--------|------|------------|-------------------|
| `fact_id` | SERIAL | PRIMARY KEY | — |
| `class_id` | INTEGER | FK → dim_class | `class_name` → dim_class lookup |
| `time_id` | INTEGER | FK → dim_time | `(report_year, report_month)` |
| `total_l12m_cases` | INTEGER | | `r12m_total_cases` (INTEGER in raw) |
| `pct_total_spirits` | NUMERIC(10,4) | | `pct_of_class` (NUMERIC in raw) |
| | | UNIQUE(class_id, time_id) | One row per class per month |

**Same `pct_of_class` note as above.**

**Indexes:** class_id, time_id

---

### 7. fact_vendor_performance

**Source:** `raw_top100_vendors`
**Grain:** One row per vendor per month (top 100 vendors only)
**Purpose:** Vendor-level market share, rankings, and L12M/YTD/current month metrics with year-over-year comparisons

| Column | Type | Constraint | Source Raw Column |
|--------|------|------------|-------------------|
| `fact_id` | SERIAL | PRIMARY KEY | — |
| `vendor_id` | INTEGER | FK → dim_vendor | `vendor_name` → dim_vendor lookup |
| `time_id` | INTEGER | FK → dim_time | `(report_year, report_month)` |
| `rank` | INTEGER | | `rank` (INTEGER in raw) |
| `market_share` | NUMERIC(10,4) | | `market_share` (NUMERIC in raw) |
| `l12m_cases_this_year` | INTEGER | | `l12m_cases_this_year` |
| `l12m_cases_prior_year` | INTEGER | | `l12m_cases_prior_year` |
| `l12m_change_pct` | NUMERIC(10,4) | | `l12m_change_pct` |
| `ytd_cases_this_year` | INTEGER | | `ytd_cases_this_year` |
| `ytd_cases_last_year` | INTEGER | | `ytd_cases_last_year` |
| `ytd_change_pct` | NUMERIC(10,4) | | `ytd_change_pct` |
| `curr_month_this_year` | INTEGER | | `curr_month_this_year` |
| `curr_month_last_year` | INTEGER | | `curr_month_last_year` |
| `curr_month_change_pct` | NUMERIC(10,4) | | `curr_month_change_pct` |
| | | UNIQUE(vendor_id, time_id) | One row per vendor per month |

**Indexes:** vendor_id, time_id

---

### 8. fact_vendor_by_class

**Source:** `raw_top20_vendors_by_class`
**Grain:** One row per vendor per class per month (top 20 vendors within each class)
**Purpose:** Vendor performance broken down by spirit class

| Column | Type | Constraint | Source Raw Column |
|--------|------|------------|-------------------|
| `fact_id` | SERIAL | PRIMARY KEY | — |
| `vendor_id` | INTEGER | FK → dim_vendor | `vendor_name` → dim_vendor lookup |
| `class_id` | INTEGER | FK → dim_class | `class` → dim_class lookup |
| `time_id` | INTEGER | FK → dim_time | `(report_year, report_month)` |
| `rank` | INTEGER | | `rank` |
| `market_share` | NUMERIC(10,4) | | `market_share` |
| `l12m_cases_this_year` | INTEGER | | `l12m_cases_this_year` |
| `l12m_cases_prior_year` | INTEGER | | `l12m_cases_prior_year` |
| `l12m_change_pct` | NUMERIC(10,4) | | `l12m_change_pct` |
| `ytd_cases_this_year` | INTEGER | | `ytd_cases_this_year` |
| `ytd_cases_last_year` | INTEGER | | `ytd_cases_last_year` |
| `ytd_change_pct` | NUMERIC(10,4) | | `ytd_change_pct` |
| `curr_month_this_year` | INTEGER | | `curr_month_this_year` |
| `curr_month_last_year` | INTEGER | | `curr_month_last_year` |
| `curr_month_change_pct` | NUMERIC(10,4) | | `curr_month_change_pct` |
| | | UNIQUE(vendor_id, class_id, time_id) | One row per vendor-class-month |

**Indexes:** vendor_id, class_id, time_id

---

## Raw Table → Star Schema Mapping Summary

| Raw Table | → Fact Table | Dimensions Used | Row Volume |
|-----------|-------------|-----------------|------------|
| `raw_brand_summary` | `fact_brand_summary` | brand, vendor, class, time | 413K+ |
| `raw_brand_summary` (bottle cols) | `fact_bottle_sales` | brand, vendor, class, time, bottle_size | ~275K (non-null bottles only) |
| `raw_brand_leaders` | `fact_brand_leaders` | brand, class, time | varies |
| `raw_current_month` | `fact_current_month_by_class` | class, time | ~1,422 |
| `raw_ytd` | `fact_ytd_by_class` | class, time | ~1,338 |
| `raw_rolling_12m` | `fact_l12m_by_class` | class, time | ~1,578 |
| `raw_top100_vendors` | `fact_vendor_performance` | vendor, time | varies |
| `raw_top20_vendors_by_class` | `fact_vendor_by_class` | vendor, class, time | varies |
| `raw_vendor_summary` | *(no separate fact)* | Used for dim_brand + dim_vendor population only | 425K+ |

**Note on raw_vendor_summary:** This table's data overlaps heavily with raw_brand_summary (same brands, vendors, classes). It contributes to dimension population (ensuring all vendors and brands are captured) but does not have its own dedicated fact table.

---

## Dimension Population Sources

Each dimension collects unique values from multiple raw tables to ensure completeness:

### dim_vendor sources:
```
raw_brand_summary.vendor
raw_vendor_summary.vendor
raw_top100_vendors.vendor_name
raw_top20_vendors_by_class.vendor_name
```

### dim_class sources:
```
raw_brand_summary.class
raw_current_month.class_name
raw_ytd.class_name
raw_rolling_12m.class_name
raw_brand_leaders.type
raw_top20_vendors_by_class.class
raw_vendor_summary.class
```

### dim_time sources:
```
All 8 raw tables: (report_year, report_month)
```

### dim_brand sources:
```
raw_brand_summary: (brand, vendor, class) → (brand_name, vendor_id, class_id)
raw_vendor_summary: (brand, vendor, class) → (brand_name, vendor_id, class_id)
```

### dim_bottle_size:
```
Pre-populated (8 static rows, not derived from raw data)
```

---

## Data Quality & Known Issues

### 1. Duplicate Rows in raw_brand_summary

5 known duplicate `(brand, vendor, class, year, month)` groups exist:

| Brand | Vendor | Class | Period |
|-------|--------|-------|--------|
| LIMOUSIN | MHW LIMITED | DOM WHSKY-STRT-RYE | 2024-10 |
| IRONCLAD | IRONCLAD DIST | DOM WHSKY-STRT-OTH | 2025-09 |
| OLE SMOKY | OLE SMOKY DIST | DOM WHSKY-BLND | 2025-12 |
| IRONCLAD | IRONCLAD DIST | DOM WHSKY-STRT-OTH | 2025-02 |
| AMARULA COFFEE | TERLATO WNS INT | CRDL-CRM LQR | 2025-02 |

**Handling:** The script uses `ON CONFLICT DO UPDATE` (upsert) for fact_brand_summary and fact_bottle_sales. The last-seen duplicate's values are kept.

### 2. pct_of_class vs pct_total_dist_spirits

The three class-level fact tables (current_month, ytd, l12m) have `pct_total_spirits` mapped from `pct_of_class`. These are different metrics:

| Column | Meaning | Coverage | Avg Value |
|--------|---------|----------|-----------|
| `pct_of_class` | % share within the class | ~95% of rows | ~33% |
| `pct_total_dist_spirits` | % of total distilled spirits | ~15% of rows | ~8% |

Current mapping uses `pct_of_class` because it has much better coverage. To switch, update the source column in the three fact-building sections of the script.

### 3. brand_id Nullable in fact_brand_leaders

`raw_brand_leaders` contains brand + type (class) but no vendor. Since dim_brand requires (brand_name, vendor_id, class_id), the script does a best-effort match via `(brand_name, class_id)` only. If a brand appears in brand_leaders but not in brand_summary or vendor_summary, `brand_id` will be NULL.

### 4. TEXT Columns in raw_brand_leaders

All numeric columns in `raw_brand_leaders` (`rank`, `pct_total`, `ytd_case_sales`, etc.) are stored as TEXT in the raw table (artifact of Textract OCR extraction). The script applies `clean_numeric()` and `clean_int()` which handle:
- Commas: `"1,234"` → `1234`
- Percent signs: `"12.5%"` → `12.5`
- Spaces: `"1 234"` → `1234`
- OCR artifacts: `". O"` → `"0"`, `".O"` → `"0"`

Values that cannot be parsed become NULL.

---

## Row Level Security (RLS)

All 13 tables have RLS enabled with a permissive "Full access" policy:

```sql
ALTER TABLE schema.table_name ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Full access" ON schema.table_name FOR ALL USING (true) WITH CHECK (true);
```

This allows unrestricted access via the service role key while keeping RLS active (Supabase requirement for REST API access).

---

## Indexes

Indexes are created on all foreign key columns after data load for query performance:

| Table | Indexed Columns |
|-------|----------------|
| fact_brand_summary | brand_id, vendor_id, class_id, time_id |
| fact_bottle_sales | brand_id, time_id, bottle_size_id |
| fact_brand_leaders | brand_id, class_id, time_id |
| fact_current_month_by_class | class_id, time_id |
| fact_ytd_by_class | class_id, time_id |
| fact_l12m_by_class | class_id, time_id |
| fact_vendor_performance | vendor_id, time_id |
| fact_vendor_by_class | vendor_id, class_id, time_id |

---

## Script Execution Flow

```
Step 1/7  Create DDL (DROP + CREATE all 13 tables + RLS policies)
Step 2/7  Populate dim_bottle_size (8 static rows)
Step 3/7  Fetch all raw data from 8 tables into memory
Step 4/7  Build & populate dimensions
            → dim_vendor (DISTINCT vendors from 4 tables)
            → dim_class (DISTINCT classes from 7 tables)
            → dim_time (DISTINCT year-month from all 8 tables)
            → Load dimension ID lookups into Python dicts
            → dim_brand (DISTINCT brand+vendor+class from 2 tables)
            → Load brand ID lookups
Step 5/7  Build & populate facts (8 fact tables)
            → fact_brand_summary (with ON CONFLICT upsert)
            → fact_bottle_sales (normalized, with ON CONFLICT upsert)
            → fact_brand_leaders (TEXT→NUMERIC conversion)
            → fact_current_month_by_class
            → fact_ytd_by_class
            → fact_l12m_by_class
            → fact_vendor_performance
            → fact_vendor_by_class
Step 6/7  Create indexes on all FK columns
Step 7/7  Verify row counts (star tables + raw tables comparison)
```

**Idempotent:** Safe to re-run. `DROP IF EXISTS CASCADE` at start clears previous star schema tables. Raw tables are never modified.

**Batch sizes:** 500 for dimensions, 1000 for facts (configurable via `DIM_BATCH_SIZE` and `FACT_BATCH_SIZE`).

---

## Example Analytics Queries

### Top 10 brands by YTD case sales (latest month)
```sql
SELECT b.brand_name, v.vendor_name, c.class_name,
       f.ytd_case_sales, f.current_month_case_sales
FROM schema.fact_brand_summary f
JOIN schema.dim_brand b ON f.brand_id = b.brand_id
JOIN schema.dim_vendor v ON f.vendor_id = v.vendor_id
JOIN schema.dim_class c ON f.class_id = c.class_id
JOIN schema.dim_time t ON f.time_id = t.time_id
WHERE t.report_period = '2025-12'
ORDER BY f.ytd_case_sales DESC
LIMIT 10;
```

### Bottle size distribution for a brand
```sql
SELECT b.brand_name, bs.bottle_size_name, SUM(fb.case_sales) as total_cases
FROM schema.fact_bottle_sales fb
JOIN schema.dim_brand b ON fb.brand_id = b.brand_id
JOIN schema.dim_bottle_size bs ON fb.bottle_size_id = bs.bottle_size_id
WHERE b.brand_name = 'TITOS HANDMADE'
GROUP BY b.brand_name, bs.bottle_size_name, bs.sort_order
ORDER BY bs.sort_order;
```

### Vendor market share trend
```sql
SELECT t.report_period, v.vendor_name, f.rank, f.market_share
FROM schema.fact_vendor_performance f
JOIN schema.dim_vendor v ON f.vendor_id = v.vendor_id
JOIN schema.dim_time t ON f.time_id = t.time_id
WHERE v.vendor_name = 'DIAGEO'
ORDER BY t.report_period;
```

### Class comparison — L12M vs YTD vs Current Month
```sql
SELECT c.class_name,
       l.total_l12m_cases,
       y.total_ytd_cases,
       m.total_cases as current_month_cases
FROM schema.dim_class c
JOIN schema.fact_l12m_by_class l ON c.class_id = l.class_id
JOIN schema.fact_ytd_by_class y ON c.class_id = y.class_id AND l.time_id = y.time_id
JOIN schema.fact_current_month_by_class m ON c.class_id = m.class_id AND l.time_id = m.time_id
JOIN schema.dim_time t ON l.time_id = t.time_id
WHERE t.report_period = '2025-12'
ORDER BY l.total_l12m_cases DESC;
```

---

## Verification Checklist

After running the script, verify:

1. **Dimension counts:**
   ```sql
   SELECT 'dim_vendor' as tbl, COUNT(*) FROM schema.dim_vendor
   UNION ALL SELECT 'dim_class', COUNT(*) FROM schema.dim_class
   UNION ALL SELECT 'dim_time', COUNT(*) FROM schema.dim_time
   UNION ALL SELECT 'dim_brand', COUNT(*) FROM schema.dim_brand
   UNION ALL SELECT 'dim_bottle_size', COUNT(*) FROM schema.dim_bottle_size;
   ```

2. **Fact counts match raw (approximately):**
   - `fact_brand_summary` ≈ `raw_brand_summary` (minus rows with NULL keys, minus duplicates)
   - `fact_current_month_by_class` ≈ `raw_current_month`
   - `fact_ytd_by_class` ≈ `raw_ytd`
   - `fact_l12m_by_class` ≈ `raw_rolling_12m`

3. **No orphaned FKs:**
   ```sql
   SELECT COUNT(*) FROM schema.fact_brand_summary f
   LEFT JOIN schema.dim_brand b ON f.brand_id = b.brand_id
   WHERE b.brand_id IS NULL;
   -- Should return 0
   ```

4. **Sample join works end-to-end:**
   ```sql
   SELECT b.brand_name, v.vendor_name, c.class_name,
          t.report_period, f.ytd_case_sales
   FROM schema.fact_brand_summary f
   JOIN schema.dim_brand b ON f.brand_id = b.brand_id
   JOIN schema.dim_vendor v ON f.vendor_id = v.vendor_id
   JOIN schema.dim_class c ON f.class_id = c.class_id
   JOIN schema.dim_time t ON f.time_id = t.time_id
   LIMIT 5;
   ```
