"""
Extract VENDOR SUMMARY - ALL CONTROL STATES from NABCA PDFs (v3)
================================================================

Improved extraction with:
1. Extract data directly from PDF/Textract first
2. Detect truncated text (e.g., short class names ending abruptly)
3. Only use fuzzy matching for truncated entries
4. Cross-reference with known vendor/class lists from raw_brand_summary

Row detection based on analysis:
- VENDOR row: x < 0.064, single item (just vendor name), no class/numbers
- BRAND row: has class (item at x ~0.18) and/or numeric data
- TOTAL row: text starts with "TOTAL"
"""

import boto3
import pdfplumber
import io
import json
import time
import os
import csv
import re
from PyPDF2 import PdfReader, PdfWriter
from collections import defaultdict
from difflib import SequenceMatcher

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, will use system environment variables

# AWS Configuration - Load from environment variables
# Set these in your environment or create a .env file (see .env.example)
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("AWS_BUCKET", "nabca-data")

if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
    raise ValueError("AWS_ACCESS_KEY and AWS_SECRET_KEY must be set in environment variables")

# PDF to date mapping
PDF_DATE_MAP = {
    "631_9L_0724.PDF": (2024, 7),
    "631_9L_0824.PDF": (2024, 8),
    "631_9L_0924.PDF": (2024, 9),
    "631_9L_1024.PDF": (2024, 10),
    "631_9L_1124.PDF": (2024, 11),
    "631_9L_1224.PDF": (2024, 12),
    "631_9L_0125.pdf": (2025, 1),
    "631_9L_0225.PDF": (2025, 2),
    "631_9L_0325.PDF": (2025, 3),
    "631_9L_0425.PDF": (2025, 4),
    "631_9L_0525.PDF": (2025, 5),
    "631_9L_0625.PDF": (2025, 6),
    "631_9L_0725.PDF": (2025, 7),
    "631_9L_0825.PDF": (2025, 8),
    "631_9L_0925.PDF": (2025, 9),
    "631_9L_1025.PDF": (2025, 10),
    "631_9L_1125.PDF": (2025, 11),
    "631_9L_1225.PDF": (2025, 12),
}

# Known class names (66 classes from raw_brand_summary)
KNOWN_CLASSES = [
    'AFTER DINNER', 'ALE/PILSNER', 'AMERICAN WHISKEY', 'ANISETTE',
    'APERITIFS', 'ARMAGNAC', 'BLEND', 'BLENDED WHISKEY', 'BOURBON',
    'BRANDY', 'BRANDY & COGNAC', 'CANADIAN WHISKEY', 'COCKTAILS',
    'COGNAC', 'CORN WHISKEY', 'CREAM LIQUEUR', 'CREME DE CACAO',
    'CREME DE CASSIS', 'CREME DE MENTHE', 'CURACAO', 'DARK RUM',
    'FLAVORED BRANDY', 'FLAVORED GIN', 'FLAVORED RUM', 'FLAVORED VODKA',
    'FLAVORED WHISKEY', 'FRUIT FLAVORED', 'GIN', 'GRAIN ALCOHOL',
    'GRAPPA', 'HERBAL', 'IMPORTED WHISKEY', 'IRISH WHISKEY',
    'JAPANESE WHISKY', 'KIRSCH', 'LIGHT RUM', 'LIGHT WHISKEY',
    'LIQUEURS', 'LIQUEURS / CORDIALS', 'LITER OR LESS', 'MALT',
    'MARASCHINO', 'MEZCAL', 'MISC BRANDY', 'MISC DISTILLED SPIRITS',
    'MISC IMPORTED WHISKEY', 'MISC WHISKEY', 'NEUTRAL SPIRITS',
    'OTHER', 'OTHER TEQUILA', 'PREPARED COCKTAILS',
    'PREPARED COCKTAILS / RTD', 'PRLM FLVRD', 'ROCK & RYE', 'RTD',
    'RUM', 'RYE WHISKEY', 'SCHNAPPS', 'SCOTCH', 'SINGLE MALT SCOTCH',
    'SPICED RUM', 'STRAIGHT BOURBON', 'STRAIGHT RYE', 'TENNESSEE WHISKEY',
    'TEQUILA', 'TRIPLE SEC', 'VODKA', 'VODKA / MALT', 'WHISKEY'
]

# Reference data will be loaded at runtime
KNOWN_VENDORS = []

s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION
)

textract = boto3.client(
    'textract',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION
)


def load_reference_data():
    """Load vendor names from existing raw_brand_summary data if available"""
    global KNOWN_VENDORS

    # Try to load from cached file first
    vendor_cache = 'cache/known_vendors.json'
    if os.path.exists(vendor_cache):
        with open(vendor_cache, 'r') as f:
            KNOWN_VENDORS = json.load(f)
        print(f"  Loaded {len(KNOWN_VENDORS)} known vendors from cache")
        return

    # If not cached, try to load from Supabase output
    vendor_summary_file = 'output/vendor_summary_all_months.csv'
    if os.path.exists(vendor_summary_file):
        vendors = set()
        with open(vendor_summary_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('vendor'):
                    vendors.add(row['vendor'].upper().strip())
        KNOWN_VENDORS = sorted(list(vendors))
        print(f"  Loaded {len(KNOWN_VENDORS)} known vendors from CSV")
        return

    # Fallback: empty list (will extract without reference)
    print("  No reference vendor data available - will extract without fuzzy matching")


def is_truncated_class(text):
    """Detect if a class name appears truncated

    Returns True if:
    - Text is very short (less than 4 chars)
    - Text ends mid-word (common truncation patterns)
    - Text doesn't match any known class exactly
    """
    if not text:
        return False

    text = text.strip().upper()

    # If it matches a known class exactly, not truncated
    if text in [c.upper() for c in KNOWN_CLASSES]:
        return False

    # Very short text is likely truncated
    if len(text) < 4:
        return True

    # Common truncation patterns - partial class names
    truncation_patterns = [
        'WHIS', 'WHISKE', 'BOURBO', 'BOUR', 'BRAND', 'BRAN',
        'FLAVO', 'FLAVOR', 'VODKA', 'VOD', 'TEQU', 'TEQUIL',
        'LIQUEU', 'LIQUE', 'SCOTC', 'SCOT', 'COCK', 'COCKTA',
        'PREPARE', 'PREPAR', 'SINGLE', 'STRAIG', 'CREAM', 'CREA',
        'IMPORT', 'AMERIC', 'CANAD', 'TENNE', 'TENNESSE',
        'IRISH', 'JAPAN', 'MEXI', 'LIGHT', 'DARK', 'FLAV',
        'SPICE', 'GRAIN', 'NEUTR', 'MISC', 'ROCK', 'TRIP'
    ]

    # Check if text appears to be truncated version of known class
    for pattern in truncation_patterns:
        if text == pattern or text.startswith(pattern + ' '):
            return True

    return False


def is_truncated_vendor(text, known_vendors):
    """Detect if a vendor name appears truncated"""
    if not text or not known_vendors:
        return False

    text = text.strip().upper()

    # If it matches a known vendor exactly, not truncated
    if text in known_vendors:
        return False

    # Very short vendor name might be truncated
    if len(text) < 4:
        return True

    # Check if this looks like a prefix of a known vendor
    for vendor in known_vendors:
        if vendor.startswith(text) and len(vendor) > len(text) + 3:
            return True

    return False


def fuzzy_match_class(text):
    """Find best matching class name using fuzzy matching

    Only called when text is detected as truncated
    """
    if not text:
        return text

    text = text.strip().upper()

    # First try prefix matching
    for class_name in KNOWN_CLASSES:
        if class_name.upper().startswith(text):
            return class_name

    # Then try fuzzy matching with higher threshold
    best_match = None
    best_ratio = 0.0

    for class_name in KNOWN_CLASSES:
        ratio = SequenceMatcher(None, text, class_name.upper()).ratio()
        if ratio > best_ratio and ratio > 0.7:  # High threshold
            best_ratio = ratio
            best_match = class_name

    if best_match:
        return best_match

    return text  # Return original if no good match


def fuzzy_match_vendor(text, known_vendors):
    """Find best matching vendor name using fuzzy matching

    Only called when text is detected as truncated
    """
    if not text or not known_vendors:
        return text

    text = text.strip().upper()

    # First try prefix matching
    for vendor in known_vendors:
        if vendor.startswith(text):
            return vendor

    # Then try fuzzy matching with higher threshold
    best_match = None
    best_ratio = 0.0

    for vendor in known_vendors:
        ratio = SequenceMatcher(None, text, vendor).ratio()
        if ratio > best_ratio and ratio > 0.8:  # Very high threshold for vendors
            best_ratio = ratio
            best_match = vendor

    if best_match:
        return best_match

    return text  # Return original if no good match


def find_vendor_summary_pages(pdf_bytes):
    """Find pages containing VENDOR SUMMARY - ALL CONTROL STATES"""
    print("  Searching for VENDOR SUMMARY - ALL CONTROL STATES pages...")

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        section_start = None
        section_end = None

        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            text_upper = text.upper()

            is_vendor_summary = "VENDOR SUMMARY" in text_upper
            is_all_control = "ALL CONTROL STATES" in text_upper
            is_by_class = "BY CLASS" in text_upper
            is_top_20 = "TOP 20" in text_upper

            if is_vendor_summary and is_all_control and not is_by_class and not is_top_20:
                if section_start is None:
                    section_start = i
                    print(f"    Section starts at page {i+1}")
                section_end = i

        if section_start is None:
            print("    WARNING: Section not found!")
            return []

        print(f"    Section ends at page {section_end+1}")
        print(f"    Total pages: {section_end - section_start + 1}")

        return list(range(section_start, section_end + 1))


def extract_pages_as_pdf(pdf_bytes, page_numbers):
    """Extract specific pages into a new PDF"""
    reader = PdfReader(io.BytesIO(pdf_bytes))
    writer = PdfWriter()

    for page_num in page_numbers:
        if page_num < len(reader.pages):
            writer.add_page(reader.pages[page_num])

    output = io.BytesIO()
    writer.write(output)
    output.seek(0)
    return output.read()


def start_textract_job(s3_key):
    """Start async Textract job"""
    response = textract.start_document_analysis(
        DocumentLocation={
            'S3Object': {
                'Bucket': BUCKET,
                'Name': s3_key
            }
        },
        FeatureTypes=['TABLES']
    )
    return response['JobId']


def wait_for_job(job_id):
    """Wait for Textract job to complete"""
    while True:
        response = textract.get_document_analysis(JobId=job_id)
        status = response['JobStatus']

        if status == 'SUCCEEDED':
            return response
        elif status == 'FAILED':
            raise Exception(f"Textract job failed: {response.get('StatusMessage')}")

        print(f"      Job status: {status}...")
        time.sleep(5)


def get_all_results(job_id):
    """Get all paginated results"""
    all_blocks = []
    next_token = None

    while True:
        if next_token:
            response = textract.get_document_analysis(JobId=job_id, NextToken=next_token)
        else:
            response = textract.get_document_analysis(JobId=job_id)

        all_blocks.extend(response.get('Blocks', []))
        next_token = response.get('NextToken')

        if not next_token:
            break

    return all_blocks


def parse_vendor_summary_data(blocks, year='', month=''):
    """Parse Textract blocks to extract vendor/brand hierarchy data

    v3 improvements:
    - Extract data from PDF first
    - Detect truncated text
    - Only fuzzy match when truncated
    """

    # Group lines by page
    lines_by_page = defaultdict(list)

    for block in blocks:
        if block['BlockType'] == 'LINE':
            page = block.get('Page', 0)
            text = block.get('Text', '')
            geo = block.get('Geometry', {}).get('BoundingBox', {})
            y = geo.get('Top', 0)
            x = geo.get('Left', 0)
            width = geo.get('Width', 0)

            # Skip copyright text in left margin
            if x < 0.04:
                continue

            lines_by_page[page].append({
                'text': text,
                'y': y,
                'x': x,
                'width': width
            })

    records = []
    current_vendor = None
    current_vendor_original = None  # Keep original for tracking
    vendor_totals = {}
    vendor_sums = {}
    skipped_pages = 0
    truncation_fixes = {'class': 0, 'vendor': 0}

    # Skip first 10 pages (TOP 100 summary section)
    start_page = 11

    known_vendors_upper = [v.upper() for v in KNOWN_VENDORS] if KNOWN_VENDORS else []

    for page in sorted(lines_by_page.keys()):
        if page < start_page:
            skipped_pages += 1
            continue

        lines = lines_by_page[page]
        lines = sorted(lines, key=lambda l: (round(l['y'], 3), l['x']))

        # Group by Y position
        rows = []
        current_row = []
        prev_y = None

        for line in lines:
            if prev_y is not None and abs(line['y'] - prev_y) > 0.008:
                if current_row:
                    rows.append(current_row)
                current_row = []
            current_row.append(line)
            prev_y = line['y']

        if current_row:
            rows.append(current_row)

        # Parse each row
        for row in rows:
            row = sorted(row, key=lambda l: l['x'])

            if not row:
                continue

            # Skip header rows
            first_text = row[0]['text']
            row_text = ' '.join([l['text'] for l in row]).upper()

            # NOTE: Header detection is careful to avoid false positives:
            # - Removed 'CLASS' - matches 'VODKA-CLASSIC-DOM', 'GIN-CLASSIC-DOM'
            # - Use 'MONTH 20' patterns for months to avoid matching brand names like 'EL MAYOR'
            # - 'MAY' would match 'MAYOR', 'MARCH' would match 'MARCHNO'
            header_phrases_exact = ['VENDOR SUMMARY', 'CASE SALES', 'CONTROL STATES',
                                    'VENDOR / BRAND', 'LAST 12 MONTHS',
                                    'NABCA', 'COPYRIGHT']

            # Month patterns - must be followed by year (20xx)
            month_patterns = ['JANUARY 20', 'FEBRUARY 20', 'MARCH 20',
                              'APRIL 20', 'MAY 20', 'JUNE 20', 'JULY 20', 'AUGUST 20',
                              'SEPTEMBER 20', 'OCTOBER 20', 'NOVEMBER 20', 'DECEMBER 20']

            is_header = any(h in row_text for h in header_phrases_exact)
            is_header = is_header or any(m in row_text for m in month_patterns)

            if is_header:
                continue

            # Separate into columns
            # Col1 (Brand): x < 0.17
            # Col2 (Class): 0.17 <= x < 0.35 (class starts at x~0.178)
            # Col3 (Numeric): x >= 0.35
            col1_parts = [l for l in row if l['x'] < 0.17]
            col2_parts = [l for l in row if 0.17 <= l['x'] < 0.35]
            col3_parts = [l for l in row if l['x'] >= 0.35]

            if not col1_parts:
                continue

            col1_sorted = sorted(col1_parts, key=lambda p: p['x'])
            first_x = col1_sorted[0]['x']
            col1_text = ' '.join([p['text'] for p in col1_sorted]).strip()

            col2_text = ' '.join([p['text'] for p in sorted(col2_parts, key=lambda p: p['x'])]).strip()

            if not col1_text:
                continue

            col1_upper = col1_text.upper()

            # Determine row type
            is_total = col1_upper.startswith('TOTAL')
            has_class = bool(col2_parts)
            has_numeric = bool(col3_parts)

            # VENDOR detection: x < 0.064, no class, no numeric data in row
            is_vendor = first_x < 0.064 and not has_class and not has_numeric and not is_total

            # Parse numeric data
            numeric_values = {}
            if col3_parts and (has_class or is_total):
                # SEPARATE boundaries for BRAND vs TOTAL rows (they have different X-positions!)
                # BRAND row positions: 0.4215, 0.5131, 0.6620, 0.7563, 0.8566, 0.8598
                # TOTAL row positions: 0.3947, 0.4602, 0.5968, 0.6568, 0.8010, 0.8586

                if is_total:
                    # TOTAL row boundaries (typically left-shifted from brand rows)
                    COLUMN_BOUNDS = [
                        ('l12m_this_year', 0.370, 0.445),      # avg 0.3947
                        ('l12m_prior_year', 0.445, 0.520),     # avg 0.4602
                        ('ytd_this_year', 0.575, 0.635),       # avg 0.5968
                        ('ytd_last_year', 0.635, 0.715),       # avg 0.6568
                        ('curr_month_this_year', 0.780, 0.845),# avg 0.8010
                        ('curr_month_last_year', 0.845, 0.890),# avg 0.8586
                    ]
                else:
                    # BRAND row boundaries (based on actual row analysis)
                    # Example: SEAGRAM 7 CROWN has positions: 0.3862, 0.4527, 0.5859, 0.6463, 0.7792, 0.8395
                    # Note: 2025-01/02/03 have different column layout - use month-specific boundaries

                    if (year, month) in [(2025, 1), (2025, 2), (2025, 3)]:
                        # Special boundaries for 2025-01/02/03 (shifted layout)
                        # HYBRID OVERLAPPING: Different methods per column for best overall accuracy
                        COLUMN_BOUNDS = [
                            ('l12m_this_year', 0.370, 0.450),      # Covers 0.3860-0.4224
                            ('l12m_prior_year', 0.450, 0.540),     # Covers 0.4526-0.5053
                            ('ytd_this_year', 0.540, 0.635),       # Optimal split at 0.635
                            ('ytd_last_year', 0.635, 0.720),       # Optimal split at 0.635
                            ('curr_month_last_year', 0.795, 0.930),# Check curr_last FIRST
                            ('curr_month_this_year', 0.720, 0.805),# Check curr_this SECOND
                        ]
                    else:
                        # Standard boundaries for most months
                        # HYBRID OVERLAPPING: curr_last starts MUCH earlier to capture more values
                        COLUMN_BOUNDS = [
                            ('l12m_this_year', 0.370, 0.420),      # Covers 0.3862
                            ('l12m_prior_year', 0.420, 0.520),     # Covers 0.4527
                            ('ytd_this_year', 0.575, 0.625),       # Covers 0.5859
                            ('ytd_last_year', 0.625, 0.715),       # Covers 0.6463
                            ('curr_month_last_year', 0.750, 0.930),# Check curr_last FIRST - start at 0.750
                            ('curr_month_this_year', 0.715, 0.830),# Check curr_this SECOND
                        ]

                # Collect all numeric values with positions
                all_numeric_values = []

                for part in col3_parts:
                    text = part['text'].replace(',', '').replace(':', '').replace('%', '').strip()
                    x_pos = part['x']

                    if text in ['.00', '00', 'ON', '-', '']:
                        continue

                    try:
                        if '.' in text or '-' in text:
                            val = float(text)
                        else:
                            val = int(text)

                        all_numeric_values.append({'value': val, 'x': x_pos, 'text': text})
                    except:
                        pass

                # OVERLAPPING BOUNDARIES APPROACH:
                # Both columns use same filtering, but different (overlapping) x-position ranges
                # curr_last checked FIRST to capture values in overlap zone

                for item in all_numeric_values:
                    val = item['value']
                    text = item['text']
                    x_pos = item['x']

                    # Filter percentages for BRAND rows
                    has_decimal = '.' in text
                    is_small_with_decimal = (abs(val) <= 200 and has_decimal and val != int(val))
                    is_round_percentage = (abs(val) <= 250 and (text.endswith('.00') or text.endswith('.0')))
                    is_tiny_decimal = (abs(val) < 1 and has_decimal)
                    is_obvious_percentage = is_small_with_decimal or is_round_percentage or is_tiny_decimal

                    if not is_total and is_obvious_percentage:
                        continue

                    # Assign all 6 columns by x-position
                    # Order matters: curr_last is checked BEFORE curr_this (overlap zone goes to curr_last)
                    for col_name, x_min, x_max in COLUMN_BOUNDS:
                        if x_min <= x_pos < x_max:
                            if col_name not in numeric_values:
                                numeric_values[col_name] = val
                                break  # Stop after first match

            # Process based on row type
            if is_vendor:
                current_vendor_original = col1_text
                current_vendor = col1_text

                # Only fuzzy match if vendor appears truncated
                if known_vendors_upper and is_truncated_vendor(col1_text, known_vendors_upper):
                    matched = fuzzy_match_vendor(col1_text, known_vendors_upper)
                    if matched != col1_text.upper():
                        current_vendor = matched
                        truncation_fixes['vendor'] += 1

                vendor_sums[current_vendor] = defaultdict(int)

            elif is_total and current_vendor:
                vendor_totals[current_vendor] = numeric_values

                # Write TOTAL row to output (NO percentage columns)
                total_record = {
                    'vendor': current_vendor,
                    'brand': col1_text,  # Keep "TOTAL VENDOR_NAME" text
                    'class': '',  # TOTAL rows have no class
                    'l12m_this_year': numeric_values.get('l12m_this_year'),
                    'l12m_prior_year': numeric_values.get('l12m_prior_year'),
                    'ytd_this_year': numeric_values.get('ytd_this_year'),
                    'ytd_last_year': numeric_values.get('ytd_last_year'),
                    'curr_month_this_year': numeric_values.get('curr_month_this_year'),
                    'curr_month_last_year': numeric_values.get('curr_month_last_year'),
                }
                records.append(total_record)

            elif (has_class or has_numeric) and current_vendor:
                # BRAND row - extract data first
                # Capture rows with class OR numeric data (some brands may have class at odd positions)
                brand_name = col1_text
                class_name = col2_text

                # Only fuzzy match class if it appears truncated
                if is_truncated_class(class_name):
                    matched_class = fuzzy_match_class(class_name)
                    if matched_class != class_name.upper():
                        class_name = matched_class
                        truncation_fixes['class'] += 1

                record = {
                    'vendor': current_vendor,
                    'brand': brand_name,
                    'class': class_name,
                    'l12m_this_year': numeric_values.get('l12m_this_year'),
                    'l12m_prior_year': numeric_values.get('l12m_prior_year'),
                    'ytd_this_year': numeric_values.get('ytd_this_year'),
                    'ytd_last_year': numeric_values.get('ytd_last_year'),
                    'curr_month_this_year': numeric_values.get('curr_month_this_year'),
                    'curr_month_last_year': numeric_values.get('curr_month_last_year'),
                }
                records.append(record)

                # Add to running sums
                for col in ['l12m_this_year', 'l12m_prior_year', 'ytd_this_year',
                            'ytd_last_year', 'curr_month_this_year', 'curr_month_last_year']:
                    val = numeric_values.get(col)
                    if val is not None and isinstance(val, (int, float)):
                        vendor_sums[current_vendor][col] += int(val)

    print(f"    Skipped first {skipped_pages} pages (TOP 100 section)")
    print(f"    Total vendors: {len(vendor_totals)}")
    print(f"    Total brand records: {len(records)}")
    print(f"    Truncation fixes: {truncation_fixes['class']} classes, {truncation_fixes['vendor']} vendors")

    # Validation - check ALL vendors with TOTAL rows across ALL columns (NO percentage columns)
    print(f"    Validating ALL vendors across volume columns only...")

    # Define numeric columns to validate (NO percentage columns)
    numeric_columns = [
        'l12m_this_year', 'l12m_prior_year',
        'ytd_this_year', 'ytd_last_year',
        'curr_month_this_year', 'curr_month_last_year'
    ]

    # Track accuracy per column
    column_stats = {col: {'perfect': 0, 'good': 0, 'mismatch': 0, 'total': 0} for col in numeric_columns}

    # Track vendors with mismatches across any column
    vendor_mismatches = {}

    for vendor, totals in vendor_totals.items():
        sums = vendor_sums.get(vendor, {})

        # Check if vendor has significant L12M This Year (primary filter)
        exp_l12m = totals.get('l12m_this_year', 0)
        if not exp_l12m or exp_l12m < 1000:
            continue

        vendor_issues = []

        # Validate each numeric column
        for col in numeric_columns:
            calc_val = sums.get(col, 0)
            exp_val = totals.get(col, 0)

            # Skip percentage columns with zero expected value
            if '_pct_' in col and exp_val == 0:
                continue

            # Skip if both are zero or near-zero
            if abs(calc_val) < 0.01 and abs(exp_val) < 0.01:
                continue

            # Calculate difference percentage
            if exp_val != 0:
                diff_pct = abs(calc_val - exp_val) / abs(exp_val) * 100
            else:
                diff_pct = 100 if calc_val != 0 else 0

            column_stats[col]['total'] += 1

            if diff_pct <= 1:
                column_stats[col]['perfect'] += 1
            elif diff_pct <= 5:
                column_stats[col]['good'] += 1
            else:
                column_stats[col]['mismatch'] += 1
                vendor_issues.append({
                    'column': col,
                    'calc': calc_val,
                    'exp': exp_val,
                    'diff_pct': diff_pct
                })

        if vendor_issues:
            vendor_mismatches[vendor] = vendor_issues

    # Print validation summary per column
    print(f"    Validation Results by Column:")
    for col in numeric_columns:
        stats = column_stats[col]
        if stats['total'] > 0:
            accuracy = (stats['perfect'] + stats['good']) / stats['total'] * 100
            print(f"      {col:25} Perfect: {stats['perfect']:3} | Good: {stats['good']:3} | Mismatch: {stats['mismatch']:3} | Accuracy: {accuracy:5.1f}%")

    # Overall accuracy across all columns
    total_checks = sum(s['total'] for s in column_stats.values())
    total_ok = sum(s['perfect'] + s['good'] for s in column_stats.values())
    overall_accuracy = (total_ok / total_checks * 100) if total_checks > 0 else 0
    print(f"    Overall Accuracy (all columns): {overall_accuracy:.1f}%")

    # Show top 10 vendors with L12M validation
    sorted_vendors = sorted(vendor_sums.items(), key=lambda x: -x[1].get('l12m_this_year', 0))[:10]
    print(f"    Top 10 vendors (L12M This Year validation):")
    for vendor, sums in sorted_vendors:
        expected = vendor_totals.get(vendor, {})
        calc_l12m = sums.get('l12m_this_year', 0)
        exp_l12m = expected.get('l12m_this_year', 0)
        if exp_l12m:
            diff_pct = abs(calc_l12m - exp_l12m) / exp_l12m * 100
            status = "OK" if diff_pct <= 5 else "MISMATCH"
            issue_cols = len(vendor_mismatches.get(vendor, []))
            status_detail = f"{status} ({issue_cols} col issues)" if issue_cols > 0 else status
            print(f"      {vendor[:30]:32} calc={calc_l12m:>10,} exp={exp_l12m:>10,} [{status_detail}]")

    # Show vendors with most column mismatches
    if vendor_mismatches:
        print(f"    Vendors with most column mismatches:")
        sorted_mismatches = sorted(vendor_mismatches.items(), key=lambda x: -len(x[1]))[:5]
        for vendor, issues in sorted_mismatches:
            print(f"      {vendor[:30]:32} {len(issues)} columns with >5% mismatch")
            for issue in issues[:3]:  # Show top 3 issues
                print(f"        - {issue['column']:22} calc={issue['calc']:>10,.0f} exp={issue['exp']:>10,.0f} ({issue['diff_pct']:.1f}%)")

    return records, vendor_totals


def extract_vendor_summary(pdf_key, year, month):
    """Extract VENDOR SUMMARY from a single PDF"""
    print(f"\nProcessing {pdf_key} ({year}-{month:02d})...")

    cache_file = f"cache/textract_vendor_summary_{year}_{month:02d}.json"
    if os.path.exists(cache_file):
        print(f"  Loading cached Textract output...")
        with open(cache_file, 'r') as f:
            blocks = json.load(f)
        print(f"    Total blocks: {len(blocks)}")
    else:
        print("  Downloading PDF...")
        response = s3.get_object(Bucket=BUCKET, Key=pdf_key)
        pdf_bytes = response['Body'].read()
        print(f"    Size: {len(pdf_bytes) / 1024 / 1024:.1f} MB")

        pages = find_vendor_summary_pages(pdf_bytes)
        if not pages:
            print("  ERROR: Section not found!")
            return [], {}

        print(f"  Creating subset PDF ({len(pages)} pages)...")
        subset_pdf = extract_pages_as_pdf(pdf_bytes, pages)
        print(f"    Subset size: {len(subset_pdf) / 1024:.1f} KB")

        subset_key = f"temp/vendor_summary_{year}_{month:02d}.pdf"
        s3.put_object(Bucket=BUCKET, Key=subset_key, Body=subset_pdf)
        print(f"  Uploaded to s3://{BUCKET}/{subset_key}")

        print("  Starting Textract job...")
        job_id = start_textract_job(subset_key)
        print(f"    Job ID: {job_id}")

        print("  Waiting for Textract...")
        wait_for_job(job_id)
        print("    Completed!")

        print("  Retrieving results...")
        blocks = get_all_results(job_id)
        print(f"    Total blocks: {len(blocks)}")

        os.makedirs('cache', exist_ok=True)
        with open(cache_file, 'w') as f:
            json.dump(blocks, f)
        print(f"    Cached to {cache_file}")

        try:
            s3.delete_object(Bucket=BUCKET, Key=subset_key)
        except:
            pass

    print("  Parsing vendor/brand hierarchy...")
    records, totals = parse_vendor_summary_data(blocks, year, month)

    for record in records:
        record['report_year'] = year
        record['report_month'] = month

    print(f"    Extracted {len(records)} brand records")

    return records, totals


def main():
    import sys

    target_months = None
    if len(sys.argv) > 1:
        target_months = set()
        for arg in sys.argv[1:]:
            if '-' in arg:
                parts = arg.split('-')
                if len(parts) == 2:
                    target_months.add((int(parts[0]), int(parts[1])))

    print("=" * 60)
    print("VENDOR SUMMARY - ALL CONTROL STATES EXTRACTION (v3)")
    print("=" * 60)
    print("v3 improvements:")
    print("  - Extract from PDF first")
    print("  - Only fuzzy match truncated text")
    print("  - Cross-reference with known vendor/class lists")
    print("=" * 60)

    if target_months:
        print(f"Processing specific months: {sorted(target_months)}")

    # Load reference data
    print("\nLoading reference data...")
    load_reference_data()

    os.makedirs('output', exist_ok=True)

    all_records = []
    processed_months = []

    fieldnames = ['vendor', 'brand', 'class',
                  'l12m_this_year', 'l12m_prior_year',
                  'ytd_this_year', 'ytd_last_year',
                  'curr_month_this_year', 'curr_month_last_year',
                  'report_year', 'report_month']

    for pdf_name, (year, month) in sorted(PDF_DATE_MAP.items(), key=lambda x: (x[1][0], x[1][1])):
        if target_months and (year, month) not in target_months:
            continue

        pdf_key = f"raw-pdfs/{pdf_name}"

        try:
            records, totals = extract_vendor_summary(pdf_key, year, month)
            all_records.extend(records)
            processed_months.append((year, month, len(records)))

            month_csv = f'output/vendor_summary_{year}_{month:02d}.csv'
            with open(month_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(records)
            print(f"  Saved {len(records)} records to {month_csv}")

        except Exception as e:
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()

    combined_csv = 'output/vendor_summary_all_months.csv'
    if all_records:
        with open(combined_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(all_records)
        print(f"\n{'='*60}")
        print(f"COMBINED: Saved {len(all_records)} records to {combined_csv}")

    print(f"\n{'='*60}")
    print("EXTRACTION SUMMARY")
    print(f"{'='*60}")
    print(f"Total months processed: {len(processed_months)}")
    print(f"Total records extracted: {len(all_records)}")
    print(f"\nRecords per month:")
    for year, month, count in processed_months:
        print(f"  {year}-{month:02d}: {count:,} records")

    # Analyze unique classes
    print(f"\nUnique classes extracted:")
    class_counts = defaultdict(int)
    for r in all_records:
        if r.get('class'):
            class_counts[r['class']] += 1

    for class_name, count in sorted(class_counts.items(), key=lambda x: -x[1])[:20]:
        print(f"  {class_name:35}: {count:6,}")

    print(f"\nTop 15 vendors by record count:")
    vendor_counts = defaultdict(int)
    for r in all_records:
        vendor_counts[r['vendor']] += 1

    for vendor, count in sorted(vendor_counts.items(), key=lambda x: -x[1])[:15]:
        print(f"  {vendor[:40]:42}: {count:6,} brands")

    print("\nDone!")


if __name__ == "__main__":
    main()
