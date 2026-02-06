# NABCA Data Extraction Scripts

## Overview
This folder contains **production-ready scripts** for extracting data from NABCA (National Alcohol Beverage Control Association) monthly PDF reports into a PostgreSQL/Supabase database.

Each script extracts data from **multiple months** (October 2024 - November 2025, 14 months total) and uploads to the corresponding database table.

---

## 📁 Scripts by Table

### 1. **rolling_12m.py**
- **Table**: `new_nabca.raw_rolling_12m`
- **Source**: Pages 9-10 (most months), Pages 7-8 (July 2025)
- **Extraction Method**: AWS Textract (asynchronous)
- **Data**: Rolling 12-month category summaries with 750ml and 375ml breakdowns
- **Usage**:
  ```bash
  python rolling_12m.py                 # Extract all months to CSV
  python rolling_12m.py --upload        # Extract and upload to database
  python rolling_12m.py 2025-10         # Extract specific month(s)
  ```

### 2. **ytd.py**
- **Table**: `new_nabca.raw_ytd`
- **Source**: Pages 7-8 (most months), Pages 5-6 (July 2025)
- **Extraction Method**: pdfplumber
- **Data**: Year-to-date category summaries with size breakdowns
- **Usage**:
  ```bash
  python ytd.py                         # Extract all months to CSV
  python ytd.py --upload                # Extract and upload to database
  python ytd.py 2025-12                 # Extract specific month(s)
  ```

### 3. **current_month.py**
- **Table**: `new_nabca.raw_current_month`
- **Source**: Pages 3-4 (most months), Pages 1-2 (July 2025)
- **Extraction Method**: pdfplumber
- **Data**: Current month category summaries with size breakdowns
- **Usage**:
  ```bash
  python current_month.py               # Extract all months to CSV
  python current_month.py --upload      # Extract and upload to database
  python current_month.py 2025-09       # Extract specific month(s)
  ```

### 4. **brand_leaders.py**
- **Table**: `new_nabca.raw_brand_leaders`
- **Source**: Pages 3-4
- **Extraction Method**: AWS Textract
- **Data**: Top 100 brand leaders with volume and growth metrics
- **Usage**:
  ```bash
  python brand_leaders.py               # Extract all months to CSV
  python brand_leaders.py --upload      # Extract and upload to database
  python brand_leaders.py 2024-11       # Extract specific month(s)
  ```

### 5. **brand_summary.py**
- **Table**: `new_nabca.raw_brand_summary`
- **Source**: Section: "BRAND SUMMARY - ALL CONTROL STATES"
- **Extraction Method**: AWS Textract (with caching)
- **Data**: Comprehensive brand-level sales data (~25,000 records/month)
- **Features**:
  - Automatic Textract job management
  - Local JSON caching for faster re-runs
  - Vendor total validation
- **Usage**:
  ```bash
  python brand_summary.py               # Extract all months
  python brand_summary.py 2025-08       # Extract specific month(s)
  ```

### 6. **top100_vendors.py**
- **Table**: `new_nabca.raw_top100_vendors`
- **Source**: Pages 9 onward (TOP 100 section)
- **Extraction Method**: AWS Textract
- **Data**: Top 100 vendors with market share and sales metrics
- **Usage**:
  ```bash
  python top100_vendors.py              # Extract all months
  ```

### 7. **top20_by_class.py**
- **Table**: `new_nabca.raw_top20_by_class`
- **Source**: Section: "SHARE OF CLASS BY BRAND"
- **Extraction Method**: pdfplumber with multi-page detection
- **Data**: Top 20 brands per class with market share
- **Usage**:
  ```bash
  python top20_by_class.py              # Extract all months
  ```

### 8. **vendor_summary.py**
- **Table**: `new_nabca.raw_vendor_summary`
- **Source**: Section: "VENDOR SUMMARY - ALL CONTROL STATES"
- **Extraction Method**: AWS Textract (with caching)
- **Data**: Vendor/brand hierarchy with volume data (~25,000 records/month)
- **Features**:
  - Vendor/brand hierarchy parsing
  - Vendor total validation (L12M, YTD columns)
  - Class name truncation fixes
  - Textract output caching
- **Usage**:
  ```bash
  python vendor_summary.py              # Extract all months
  python vendor_summary.py 2025-11      # Extract specific month(s)
  ```

---

## 🔧 Configuration

All scripts connect to the same Supabase database:
- **Host**: `db.tnricrwvrnsnfbvrvoor.supabase.co`
- **Database**: `postgres`
- **Schema**: `new_nabca`
- **S3 Bucket**: `nabca-spirit-monthly-report`

**Important**: Update the database credentials in each script before use.

---

## 📊 Data Coverage

All scripts extract data from:
- **July 2024** - December 2025 (18 months)
- Total records across all tables: **~700,000 records**

### Records per Table (approximate):
- `rolling_12m`: ~630 records (45 classes × 14 months)
- `ytd`: ~630 records
- `current_month`: ~630 records
- `brand_leaders`: ~1,400 records (100 brands × 14 months)
- `brand_summary`: ~350,000 records (25,000/month × 14 months)
- `top100_vendors`: ~1,400 records (100 vendors × 14 months)
- `top20_by_class`: ~7,000 records (varies by month)
- `vendor_summary`: ~350,000 records (25,000/month × 14 months)

---

## 🚀 Quick Start

1. **Install dependencies**:
   ```bash
   pip install boto3 psycopg2-binary pdfplumber pandas
   ```

2. **Configure AWS credentials** (for Textract-based scripts):
   ```bash
   aws configure
   ```

3. **Update database credentials** in each script

4. **Run extraction**:
   ```bash
   # For pdfplumber-based scripts (ytd, current_month, top100_vendors, top20_by_class)
   python ytd.py --upload

   # For Textract-based scripts (rolling_12m, brand_leaders, brand_summary, vendor_summary)
   python brand_summary.py
   ```

---

## 📝 Notes

### Script Types:
1. **pdfplumber scripts** (ytd, current_month, top100_vendors, top20_by_class):
   - Fast execution (~5-10 seconds per month)
   - No AWS costs
   - CSV output + optional database upload

2. **AWS Textract scripts** (rolling_12m, brand_leaders, brand_summary, vendor_summary):
   - Slower first run (~2-5 minutes per month)
   - Requires AWS credentials and costs ~$0.05 per PDF page
   - Some use local JSON caching for faster subsequent runs (brand_summary, vendor_summary)
   - CSV output + optional database upload (rolling_12m, brand_leaders)
   - Direct database upload (brand_summary, vendor_summary)

### Special Cases:
- **July 2025**: Different page layout for rolling_12m, ytd, and current_month tables
- **Vendor Summary**: Uses vendor total validation to ensure extraction accuracy
- **Brand Summary**: Handles duplicate rows and junk text filtering

### Data Quality:
- All scripts have been verified with >98% accuracy
- Vendor/brand totals validated where applicable
- Duplicate detection and removal
- NULL value handling for empty categories

---

## 📧 Support

For questions or issues, please contact the development team.

---

**Last Updated**: February 2026
**Version**: 1.0 (Production)
