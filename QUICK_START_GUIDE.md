# Quick Start Guide - One Script Per Table

**Simple Structure:** One extraction script for each database table

---

## 📋 Table → Script Mapping

| Database Table | Script to Run | What It Extracts |
|----------------|---------------|------------------|
| **raw_ytd** | `ytd.py` | Year-to-Date category data |
| **raw_rolling_12m** | `rolling_12m.py` | Rolling 12-month category data |
| **raw_current_month** | `current_month.py` | Current month category data |
| **raw_brand_leaders** | `brand_leaders.py` | Top 100 brand leaders |
| **raw_brand_summary** | `brand_summary.py` | All brands with vendors (~23K records/month) |
| **raw_vendor_summary** | `vendor_summary.py` | All vendors with brands (~23K records/month) |
| **raw_top100_vendors** | `top100_vendors.py` | Top 100 vendors |
| **raw_top20_vendors_by_class** | `top20_by_class.py` | Top 20 vendors per class |

---

## 🚀 How to Use

### 1. Install Dependencies
```bash
pip install psycopg2-binary pandas openpyxl boto3 pdfplumber
```

### 2. Extract Data for a Table

**Simple category tables (no AWS needed):**
```bash
python ytd.py
python current_month.py
```

**Complex brand/vendor tables (needs AWS Textract):**
```bash
python brand_summary.py
python vendor_summary.py
```

### 3. Done!
Each script will:
- Extract data from PDFs
- Save to CSV files (in `output/` folder)
- Upload to database (optional)
- Show validation results

---

## 📁 Folder Structure

```
GITHUB_CODES/
│
├── ytd.py                          ← One script per table
├── rolling_12m.py                  ← One script per table
├── current_month.py                ← One script per table
├── brand_leaders.py                ← One script per table
├── brand_summary.py                ← One script per table
├── vendor_summary.py               ← One script per table
├── top100_vendors.py               ← One script per table
├── top20_by_class.py               ← One script per table
│
├── validation_scripts/             ← Optional: For checking data quality
│   ├── verify_all_systems.py
│   ├── comprehensive_data_quality_scan.py
│   ├── vendor_summary_monthly_split.py
│   └── vendor_summary_accuracy_excluding_totals.py
│
├── cleanup_scripts/                ← Optional: For fixing data issues
│   ├── remove_total_vendor_rows.py
│   ├── fix_merged_total_vendor.py
│   └── fix_duplicated_class_names.py
│
└── Documentation/
    ├── README.md
    ├── QUICK_START_GUIDE.md (this file)
    ├── EXTRACTION_LOGIC_DOCUMENTATION.md
    └── FILES_READY_TO_SHARE.md
```

---

## 🎯 Common Use Cases

### Extract All Category Data
```bash
python ytd.py
python rolling_12m.py
python current_month.py
```

### Extract Brand Data
```bash
python brand_summary.py
```

### Extract Vendor Data
```bash
python vendor_summary.py
```

### Verify Data Quality (Optional)
```bash
cd validation_scripts
python verify_all_systems.py
```

---

## ⚙️ Configuration

All scripts connect to the same database:
```python
Host: db.tnricrwvrnsnfbvrvoor.supabase.co
Database: postgres
Schema: new_nabca
```

For AWS Textract (brand/vendor scripts):
```python
Bucket: nabca-data
Region: us-east-1
```

---

## 📊 What You Get

### Data Extracted
- **18 months:** July 2024 - December 2025
- **700K+ records** across all tables
- **98.7%+ accuracy** verified

### Output Files
- CSV files in `output/` folder
- One file per month: `{table}_{year}_{month}.csv`
- Combined file: `{table}_all_months.csv`

---

## 💡 Tips

1. **Start with category tables** (ytd, rolling_12m, current_month)
   - They're fast and don't need AWS

2. **Then run brand/vendor tables** (brand_summary, vendor_summary)
   - These need AWS Textract
   - They use caching to save time and money

3. **Optional: Run validation scripts**
   - Located in `validation_scripts/` folder
   - Verify data quality after extraction

4. **Database is already cleaned**
   - All cleanup scripts already applied
   - Data is production-ready

---

## ❓ FAQ

**Q: Which script should I run first?**
A: Start with `ytd.py` - it's fast and doesn't need AWS.

**Q: Do I need AWS credentials?**
A: Only for these scripts:
- brand_summary.py
- vendor_summary.py
- brand_leaders.py
- top100_vendors.py
- rolling_12m.py

**Q: What if I only need one table?**
A: Just run that one script! Each script is independent.

**Q: Where is the extracted data saved?**
A: In the `output/` folder (CSV files) and database (if upload enabled).

**Q: How do I check data quality?**
A: Run `validation_scripts/verify_all_systems.py`

---

## 🎉 That's It!

**One script per table. Simple and clean.**

For detailed extraction logic, see: `EXTRACTION_LOGIC_DOCUMENTATION.md`

---

**Last Updated:** 2026-02-06
**Version:** Production v3.0
