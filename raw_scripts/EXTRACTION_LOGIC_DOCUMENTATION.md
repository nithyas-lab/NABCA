# NABCA Data Extraction - Complete Logic Documentation

**Purpose:** Detailed explanation of extraction logic for each table
**Last Updated:** 2026-02-06
**Version:** Production v3.0

---

## Table of Contents

1. [Category Tables (pdfplumber)](#category-tables)
2. [Brand Tables (AWS Textract)](#brand-tables)
3. [Vendor Tables (AWS Textract)](#vendor-tables)
4. [Validation Logic](#validation-logic)
5. [Error Handling](#error-handling)

---

## Category Tables

### Table: `raw_ytd` (Year-to-Date)

**Script:** `ytd.py`
**Method:** pdfplumber (text extraction)
**Source:** Pages 7-8 (except July 2025: pages 5-6)

#### Extraction Logic:

1. **Page Detection:**
   ```
   - Find page with "THIS YEAR TO DATE" header
   - Extract next 2 pages as YTD section
   - Exception: July 2025 uses pages 5-6
   ```

2. **Text Extraction:**
   ```
   - Use pdfplumber to extract all text from pages
   - Parse line by line looking for spirit categories
   - Match against known category list (VODKA, WHISKY, GIN, etc.)
   ```

3. **Column Mapping:**
   ```
   Position-based extraction:
   - Column 1: class_name (spirit category)
   - Column 3: pct_of_class (percentage)
   - Column 4: ytd_total_cases (total cases)
   - Column 7: ytd_cases_750ml (750ml bottles)
   - Column 9: ytd_cases_375ml (375ml bottles)
   ```

4. **Data Cleaning:**
   ```
   - Remove header rows ("PERCENT BY SIZE", "TWO YEAR SPIRITS")
   - Filter out NULL total_cases rows
   - Remove junk rows with pattern matching
   ```

5. **Validation:**
   ```
   - Verify key categories exist (TOTAL DOM WHSKY, TOTAL VODKA, etc.)
   - Check: sum(subcategories) = TOTAL category
   - Verify row count is 72-79 rows per month
   ```

**Output:** ~75 category records per month

---

### Table: `raw_rolling_12m` (Rolling 12 Month)

**Script:** `rolling_12m.py`
**Method:** AWS Textract (TABLE detection)
**Source:** Pages 9-10 (except July 2025: pages 7-8)

#### Extraction Logic:

1. **Page Detection:**
   ```
   - Find page with "ROLLING TWELVE MONTH" header
   - Extract pages 9-10 for most months
   - Exception: July 2025 uses pages 7-8
   ```

2. **Textract Processing:**
   ```
   - Upload PDF to S3
   - Call Textract StartDocumentAnalysis with TABLES feature
   - Wait for job completion (async)
   - Retrieve TABLE blocks from Textract response
   ```

3. **Table Parsing:**
   ```
   - Extract tables from Textract blocks
   - Map columns by position:
     - Col 1: class_name
     - Col 3: pct_of_class
     - Col 4: r12m_total_cases
     - Col 7: r12m_cases_750ml
     - Col 9: r12m_cases_375ml
   ```

4. **Data Cleaning:**
   ```
   - Filter junk rows (headers, footers)
   - Remove NULL values
   - Convert numeric strings to integers
   ```

5. **Validation:**
   ```
   - Verify category structure matches expected hierarchy
   - Check: sum(subcategories) = TOTAL category
   - Verify reasonable data ranges
   ```

**Output:** ~75 category records per month

---

### Table: `raw_current_month`

**Script:** `current_month.py`
**Method:** pdfplumber (text extraction)
**Source:** Pages 5-6

#### Extraction Logic:

1. **Page Detection:**
   ```
   - Find page with "CURRENT MONTH" header
   - Extract pages 5-6
   ```

2. **Text Extraction:**
   ```
   - Extract all text using pdfplumber
   - Parse line by line for spirit categories
   - Handle multi-line categories
   ```

3. **Column Mapping:**
   ```
   - Column 1: class_name
   - Column 2: curr_month_pct
   - Column 3: curr_month_total_cases
   - Columns 4-10: Bottle sizes (1.75L, 1L, 750ml, etc.)
   ```

4. **Validation:**
   ```
   - Verify all major categories present
   - Check bottle size columns sum to total
   - Validate data ranges
   ```

**Output:** ~75 category records per month

---

## Brand Tables

### Table: `raw_brand_summary`

**Script:** `brand_summary.py`
**Method:** AWS Textract (TABLE detection with caching)
**Source:** "BRAND SUMMARY - ALL CONTROL STATES" section (350+ pages)

#### Extraction Logic:

1. **Page Detection:**
   ```
   - Search all pages for "BRAND SUMMARY" text
   - Identify start page (usually page 11)
   - Find end page by looking for next section
   - Extract 350-400 pages per PDF
   ```

2. **PDF Subsetting:**
   ```
   - Use PyPDF2 to extract only Brand Summary pages
   - Create smaller PDF (reduces Textract cost and time)
   - Upload subset to S3
   ```

3. **Textract Processing with Caching:**
   ```
   - Check if cache/textract_{year}_{month}.json exists
   - If cached: Load blocks from JSON (instant)
   - If not cached:
     - Call Textract StartDocumentAnalysis
     - Wait for completion (~2-5 minutes)
     - Save blocks to cache/textract_{year}_{month}.json
   ```

4. **Table Parsing:**
   ```
   For each table in Textract blocks:
     - Extract cells by row and column index
     - Detect structure: [Vendor | Brand | Class | Data columns]
   ```

5. **Column Boundary Detection:**
   ```
   Position-based extraction using X-coordinates:
   - vendor: Starting column (multi-line handling)
   - brand: After vendor name ends
   - class: Spirit class column
   - l12m_cases_ty: X position 0.27-0.335
   - l12m_cases_ly: X position 0.335-0.40
   - ytd_cases_ty: X position 0.40-0.47
   - curr_month_cases: X position 0.49-0.55
   - Bottle sizes: X positions 0.55-0.90
   ```

6. **Multi-Line Vendor Names:**
   ```
   - If first cell has no numeric data, it's a vendor name
   - Combine with next line if vendor name continues
   - Handle cases like "AMERICAN\nCRAFT SPIRITS"
   ```

7. **Class Detection:**
   ```
   - Match against known class list:
     ["VODKA-CLASSIC-DOM", "DOM WHSKY-STRT-BRBN/TN", etc.]
   - Handle split class names (Col1: "VODKA-CLASSIC-", Col2: "DOM")
   - Combine and validate against class list
   ```

8. **TOTAL Row Detection:**
   ```
   - Rows starting with "TOTAL" mark end of current class
   - Extract TOTAL row values for validation
   - Store: totals[class] = {col: value}
   ```

9. **Running Sum Validation:**
   ```
   For each class:
     - Accumulate sum of all brand values
     - When TOTAL row found:
       - Compare: sum(brands) vs TOTAL row from PDF
       - Calculate accuracy per column
       - Log mismatches > 1% difference
   ```

10. **Data Cleaning:**
    ```
    - Handle NULL values (convert to None)
    - Remove duplicate rows
    - Filter out junk text rows
    - Validate numeric ranges
    ```

**Output:** ~23,000-25,000 brand records per month

**Validation:** Sum(brands per class) = TOTAL row from PDF (99.84% accuracy)

---

### Table: `raw_brand_leaders`

**Script:** `brand_leaders.py`
**Method:** AWS Textract (TABLE detection)
**Source:** "BRAND LEADERS BY CLASS" section

#### Extraction Logic:

1. **Page Detection:**
   ```
   - Find "BRAND LEADERS" or "BY CLASS" text
   - Extract pages 3-4 typically
   ```

2. **Textract Processing:**
   ```
   - Simple table extraction
   - No caching (small table)
   ```

3. **Table Parsing:**
   ```
   Columns:
   - rank: Brand rank (1-100)
   - brand: Brand name
   - class: Spirit class
   - l12m_case_sales: Last 12 month sales
   ```

4. **Data Cleaning:**
   ```
   - Remove header rows
   - Validate rank sequence
   - Check for duplicates
   ```

**Output:** ~100 brand records per month

---

## Vendor Tables

### Table: `raw_vendor_summary`

**Script:** `vendor_summary.py`
**Method:** AWS Textract (TABLE detection with caching)
**Source:** "VENDOR SUMMARY - ALL CONTROL STATES" section (350+ pages)

#### Extraction Logic:

1. **Page Detection:**
   ```
   - Search for "VENDOR SUMMARY" text
   - Identify start page
   - Find end page before next section
   - Extract 350-400 pages per PDF
   ```

2. **PDF Subsetting:**
   ```
   - Extract only Vendor Summary pages using PyPDF2
   - Create smaller PDF to reduce processing time
   - Upload to S3 as temp/vendor_summary_{year}_{month}.pdf
   ```

3. **Textract Processing with Caching:**
   ```
   Cache file: cache/textract_vendor_summary_{year}_{month}.json

   If cache exists:
     - Load Textract blocks from JSON
     - Skip AWS Textract call (save time and money)
   Else:
     - Call StartDocumentAnalysis
     - Wait for completion
     - Save blocks to cache
   ```

4. **Column Boundary Detection (Critical!):**
   ```
   Position-based extraction using cell X-coordinates:

   - vendor: X < 0.15 (left column)
   - brand: 0.15 < X < 0.25
   - class: 0.25 < X < 0.35
   - l12m_this_year: 0.35 < X < 0.42
   - l12m_prior_year: 0.42 < X < 0.49
   - ytd_this_year: 0.49 < X < 0.56
   - ytd_last_year: 0.56 < X < 0.63
   - curr_month_this_year: 0.63 < X < 0.70
   - curr_month_last_year: 0.70 < X < 0.77

   Note: Boundaries calibrated through trial and analysis
   ```

5. **Vendor Detection:**
   ```
   A row is a vendor if:
     - First cell is non-empty
     - Has no numeric data in data columns
     - Typically uppercase text
     - May span multiple lines

   Special case: Handle multi-line vendor names
     - Example: "AMERICAN" (line 1) + "CRAFT SPIRITS" (line 2)
     - Combine lines until first brand row found
   ```

6. **Brand Detection:**
   ```
   A row is a brand if:
     - Current vendor is set
     - Has brand name in column 2
     - Has numeric data in data columns
     - Class field is populated
   ```

7. **TOTAL VENDOR Row Detection:**
   ```
   Rows with brand = "TOTAL VENDOR":
     - These are vendor summary rows showing vendor total
     - Used for validation only
     - NOT uploaded to database (intentionally excluded)
     - Store for validation: vendor_totals[vendor] = {cols: values}
   ```

8. **Class Name Handling:**
   ```
   Issues found:
     - Truncated: "DOM" instead of "DOM WHSKY-STRT-BRBN/TN"
     - Duplicated: "DOM DOM WHSKY-STRT-BRBN/TN WHSKY-STRT-BRBN/TN"
     - NULL: Empty class field

   Solutions:
     - Log truncated classes (cannot fix - need PDF verification)
     - Fix duplications: Remove repeated text
     - Accept NULLs: Mark for manual review
   ```

9. **Vendor Total Validation:**
   ```
   For each vendor:
     1. Calculate: sum(all brand values per column)
     2. Compare: calculated_sum vs TOTAL VENDOR row from PDF
     3. Calculate accuracy: matches / total * 100
     4. Log mismatches > 1% difference

   Validation columns:
     - l12m_this_year
     - l12m_prior_year
     - ytd_this_year
     - ytd_last_year
     - curr_month_this_year
     - curr_month_last_year
   ```

10. **Data Cleaning:**
    ```
    - Remove TOTAL VENDOR rows (vendor summaries)
    - Fix merged brand names (e.g., "BRAND NAME TOTAL VENDOR" → "BRAND NAME")
    - Fix duplicated class names (e.g., "DOM DOM WHSKY" → "DOM WHSKY")
    - Handle NULL values (keep as NULL - don't guess)
    - Validate numeric ranges
    - Remove negative values (set to NULL)
    ```

**Output:** ~23,000-25,000 brand records per month

**Validation:** Sum(brands per vendor) = TOTAL VENDOR row (98.73% accuracy)

**Known Issues:**
- 105 records with duplicated class names (edge cases)
- 1,994 records with truncated class names (0.47%)
- 3,290 records with NULL class (0.77%)

---

### Table: `raw_top100_vendors`

**Script:** `top100_vendors.py`
**Method:** AWS Textract (TABLE detection)
**Source:** "TOP 100 VENDORS" section

#### Extraction Logic:

1. **Page Detection:**
   ```
   - Find "TOP 100" text
   - Extract pages starting from page 9
   ```

2. **Table Parsing:**
   ```
   Columns:
   - rank: Vendor rank (1-100)
   - vendor: Vendor name
   - l12m_cases: Last 12 month cases
   - pct_change: Percentage change
   - market_share: Market share percentage
   ```

3. **Validation:**
   ```
   - Verify rank sequence 1-100
   - Check market_share sums to ~100%
   - Validate no duplicates
   ```

**Output:** ~100 vendor records per month

---

### Table: `raw_top20_vendors_by_class`

**Script:** `top20_by_class.py`
**Method:** pdfplumber (text extraction)
**Source:** "SHARE OF CLASS BY BRAND" section

#### Extraction Logic:

1. **Page Detection:**
   ```
   - Multi-page section
   - Each class has its own page
   - Parse page by page
   ```

2. **Class Detection:**
   ```
   - Find class header on each page
   - Extract top 20 vendors for that class
   ```

3. **Data Extraction:**
   ```
   Columns:
   - class: Spirit class
   - rank: Vendor rank (1-20)
   - vendor: Vendor name
   - cases: Case volume
   - market_share: Share of class percentage
   ```

4. **Validation:**
   ```
   - Verify 20 vendors per class
   - Check market_share adds to reasonable total
   - Validate ranks 1-20
   ```

**Output:** ~20 vendors × ~40 classes = ~800 records per month

---

## Validation Logic

### 3-Step Verification Methodology

#### Step 1: PDF Re-extraction
```
Purpose: Validate against original source
Process:
  1. Re-extract data from PDF using AWS Textract
  2. Save to Excel files for comparison
  3. Load Excel files as "PDF truth"
```

#### Step 2: Record-by-Record Comparison
```
Purpose: Verify each record matches between PDF and database
Process:
  For each record in Excel:
    1. Look up same record in database (vendor, brand, class, year, month)
    2. Compare all columns:
       - l12m_this_year
       - l12m_prior_year
       - ytd_this_year
       - ytd_last_year
       - curr_month_this_year
       - curr_month_last_year
    3. Flag if:
       - Record missing in database
       - Any column value differs by > 0.01
    4. Calculate: accuracy = (matching_records / total_records) × 100
```

#### Step 3: TOTAL Row Validation
```
Purpose: Verify mathematical consistency
Process:
  For vendor_summary:
    For each vendor:
      1. Calculate: sum(all brands for this vendor per column)
      2. Load TOTAL VENDOR row from PDF
      3. Compare: calculated_sum vs PDF_total
      4. Flag if difference > 1%
      5. Calculate: accuracy = (matching_vendors / total_vendors) × 100

  For brand_summary:
    For each class:
      1. Calculate: sum(all brands in this class per column)
      2. Load TOTAL row from PDF
      3. Compare: calculated_sum vs PDF_total
      4. Flag if difference > 1%
      5. Calculate: accuracy = (matching_classes / total_classes) × 100
```

---

## Error Handling

### Common Extraction Errors

#### 1. Merged Vendor Names
```
Issue: Vendor name appears in brand column
Example: Brand = "KING OF KENTUCKY TOTAL VENDOR"

Root Cause: Textract misaligns table cells

Solution:
  - Remove "TOTAL VENDOR" suffix from brand names
  - Log for manual review if complex merge
```

#### 2. Duplicated Class Names
```
Issue: Class name repeats twice
Example: "DOM DOM WHSKY-STRT-BRBN/TN WHSKY-STRT-BRBN/TN"

Root Cause: Textract reads same cell twice

Solution:
  - Detect exact duplications (first half = second half)
  - Remove duplication: keep first half only
  - Fixed 400 out of 505 cases automatically
```

#### 3. Truncated Class Names
```
Issue: Class name cut short
Example: "DOM" instead of "DOM WHSKY-STRT-BRBN/TN"

Root Cause: Textract boundary detection error

Solution:
  - Log as error (cannot fix - need PDF to determine correct value)
  - Mark for manual review
  - Do NOT guess (risk introducing new errors)
```

#### 4. NULL Class Values
```
Issue: Class field is empty
Example: class = NULL

Root Cause: Textract fails to read cell

Solution:
  - Keep as NULL
  - Do NOT guess or infer
  - Mark for manual PDF verification
```

#### 5. Negative Values
```
Issue: Numeric field has negative value
Example: l12m_this_year = -7

Root Cause: OCR misreads number or formatting issue

Solution:
  - Set to NULL
  - Log for review
```

#### 6. Duplicate Records
```
Issue: Same record appears twice
Example: Same vendor/brand/class/year/month combination

Root Cause: Processing error or re-upload

Solution:
  - Use PostgreSQL ctid to identify exact duplicates
  - Keep first occurrence only
  - DELETE using: WHERE ctid NOT IN (SELECT MIN(ctid) GROUP BY keys)
```

---

## Performance Optimization

### Caching Strategy
```
Textract Results Caching:
  - Save blocks to cache/{table}_{year}_{month}.json
  - Check cache before calling Textract
  - Reduces cost: $0.015/page × 350 pages = $5.25 saved per month
  - Reduces time: 5 minutes → instant
```

### Batch Processing
```
Excel Output:
  - Write to Excel in batches of 5,000 records
  - Prevents memory issues with large datasets

Database Upload:
  - Insert in batches of 1,000 records
  - Commit after each batch
  - Rollback on error
```

### Parallel Processing
```
Where Possible:
  - Process multiple months concurrently
  - Use async Textract jobs
  - Parallel database queries
```

---

## Data Quality Metrics


### Error Rates by Type

| Error Type | Count | % of Records |
|------------|-------|--------------|
| Duplicated class names | 105 | 0.02% |
| Truncated class names | 1,994 | 0.47% |
| NULL class values | 3,290 | 0.77% |
| Short vendor names | 2 | <0.01% |
| Short brand names | 113 | 0.03% |
| **Total Errors** | **5,504** | **1.29%** |

---

## Conclusion

This documentation covers the complete extraction logic for all 8 NABCA tables. Each table uses appropriate extraction methods (pdfplumber for simple tables, AWS Textract for complex tables) with comprehensive validation and error handling.

**Key Takeaways:**
- Position-based column extraction for accuracy
- TOTAL row validation for mathematical verification
- 3-step verification methodology
- Conservative error handling 
- Comprehensive logging for manual review

**Overall System Accuracy: 98.7%+**

---

**Document Version:** 1.0
**Last Updated:** 2026-02-06
**Author:** NABCA Data Engineering Team
