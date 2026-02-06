"""
Extract BRAND SUMMARY table from NABCA PDFs
- Separates Class and Brand into distinct columns
- Handles multi-line vendor names
- Uses async AWS Textract for accurate table extraction
- Validates totals against TOTAL rows
"""

import boto3
import pdfplumber
import io
import json
import time
import os
import csv
from PyPDF2 import PdfReader, PdfWriter
from collections import defaultdict

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

# PDF to date mapping (all available months - July 2024 to October 2025)
# Test with July 2024 only
PDF_DATE_MAP = {
    "631_9L_0724.PDF": (2024, 7),
    "631_9L_0824.PDF": (2024, 8),
    "631_9L_0924.PDF": (2024, 9),
    "631_9L_1024.PDF": (2024, 10),
    "631_9L_1124.PDF": (2024, 11),
    "631_9L_1224.PDF": (2024, 12),
    "631_9L_0125.pdf": (2025, 1),   # lowercase extension
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

# Complete list of known classes (from NABCA data)
# Includes both exact names and common OCR variations from Textract
KNOWN_CLASSES = [
    # Domestic Whiskey
    'DOM WHSKY-BLND', 'DOM WHSKY-SNGL MALT', 'DOM WHSKY-STRT-BRBN/TN',
    'DOM WHSKY-STRT-OTH', 'DOM WHSKY-STRT-RYE', 'DOM WHSKY-STRT-SM BTCH',
    # OCR variations for DOM WHSKY
    'DOM WHSKY-STRT-BRBN', 'DOM WHSKY -SNGL MALT', 'DOM WHSKY-STRT',
    # Scotch
    'SCOTCH-BLND-FRGN BTLD', 'SCOTCH-BLND-US BTLD', 'SCOTCH-SNGL MALT',
    # Canadian
    'CAN-FRGN BLND-FRGN BTLD', 'CAN-US BLND-US BTLD',
    # Irish
    'IRISH', 'IRISH-BLND', 'IRISH-SNGL MALT',
    # Other Imported Whiskey
    'OTH IMP WHSKY', 'OTH IMP WHSKY-BLND', 'OTH IMP WHSKY-SNGL MALT',
    # Brandy/Cognac
    'BRNDY/CGNC-ARMGNC', 'BRNDY/CGNC-CGNC-OTH', 'BRNDY/CGNC-CGNC-VS',
    'BRNDY/CGNC-CGNC-VSOP', 'BRNDY/CGNC-CGNC-XO', 'BRNDY/CGNC-DOM', 'BRNDY/CGNC-IMP',
    # OCR variations for BRNDY/CGNC
    # Note: 'BRNDY/CGNC-CGNC' is ambiguous (VS/VSOP/XO) - detected via TOTAL row backfill
    'BRNDY/CGNC-ARMGNO', 'BRNDY/CGNC-CGNC', 'BRNDY/CGNC-CGNC -OTH',
    # Rum
    'RUM-AGED/DARK', 'RUM-FLVRD', 'RUM-GOLD', 'RUM-LIGHT',
    # Gin
    'GIN-CLASSIC-DOM', 'GIN-CLASSIC-IMP', 'GIN-FLVRD-DOM', 'GIN-FLVRD-IMP',
    # Vodka
    'VODKA-CLASSIC-DOM', 'VODKA-CLASSIC-IMP', 'VODKA-FLVRD-DOM', 'VODKA-FLVRD-IMP',
    # Tequila
    'TEQUILA-ANEJO', 'TEQUILA-BLANCO', 'TEQUILA-CRISTALINO',
    'TEQUILA-FLAVORED', 'TEQUILA-GOLD', 'TEQUILA-REPOSADO',
    # Mezcal
    'MEZCAL-CRISTALINO', 'MEZCAL',
    # Cordials
    'CRDL-COFFEE LQR', 'CRDL-CRM LQR',
    'CRDL-LQR&SPC-AMRT', 'CRDL-LQR&SPC-ANSE FLVRD', 'CRDL-LQR&SPC-CRM',
    'CRDL-LQR&SPC-CURACAO', 'CRDL-LQR&SPC-FRT', 'CRDL-LQR&SPC-HZLNT',
    'CRDL-LQR&SPC-OTH', 'CRDL-LQR&SPC-SLOE GIN', 'CRDL-LQR&SPC-SPRT SPCTY',
    'CRDL-LQR&SPC-TRIPLE SEC', 'CRDL-LQR&SPC-WHSKY',
    'CRDL-SNPS-APPL', 'CRDL-SNPS-BTRSCTCH', 'CRDL-SNPS-CNNMN',
    'CRDL-SNPS-OTH', 'CRDL-SNPS-PEACH', 'CRDL-SNPS-PPRMNT',
    # OCR variations for CRDL
    'CRDL-SNPS-BTRSCTC', 'CRDL-SNPS-BTRS',  # Truncated variations
    'CRDL-LQR&SPC-FR', 'CRDL-LQR&SPC-ANSI FLVRD',  # Truncated/OCR variations
    # Other
    'COCKTAILS', 'NEUTRAL GRAIN SPIRIT', 'CACHACA'
]

# Normalize class names - map OCR variations to canonical names
CLASS_NORMALIZATION = {
    # DOM WHSKY variations
    'DOM WHSKY-STRT-BRBN': 'DOM WHSKY-STRT-BRBN/TN',
    'DOM WHSKY-STRT-BRBN-TN': 'DOM WHSKY-STRT-BRBN/TN',  # Dash instead of slash
    
    'DOM WHSKY-STRT-BRBNTN': 'DOM WHSKY-STRT-BRBN/TN',   # No separator
    'DOM WHSKY-STRT-BRBN1TN': 'DOM WHSKY-STRT-BRBN/TN',  # 1 instead of slash
    'DOM WHSKY-STRT-BRBN TN': 'DOM WHSKY-STRT-BRBN/TN',   # Space instead of slash
    'DOM WHSKY -SNGL MALT': 'DOM WHSKY-SNGL MALT',
    'DOM WHSKY-STRT': 'DOM WHSKY-STRT-OTH',
    # BRNDY/CGNC variations
    'BRNDY/CGNC-ARMGNO': 'BRNDY/CGNC-ARMGNC',
    'BRNDY/CGNC-CGNC -OTH': 'BRNDY/CGNC-CGNC-OTH',
    # Note: 'BRNDY/CGNC-CGNC' is ambiguous - could be VS, VSOP, or XO
    # Do NOT normalize it - let TOTAL row detection handle it
    'BRNDY/CGNC-CGNC-V S': 'BRNDY/CGNC-CGNC-VS',        # Space in VS
        'BRNDY/CGNC-CGNC- VS': 'BRNDY/CGNC-CGNC-VS',        # Space after dash
    'BRNDY/CGNC-CGNC-Vs': 'BRNDY/CGNC-CGNC-VS',         # Mixed case
    'BRNDY/CGNC-CGNC VS': 'BRNDY/CGNC-CGNC-VS',         # Missing dash
    'BRNDY CGNC-CGNC-VS': 'BRNDY/CGNC-CGNC-VS',         # Space instead of slash
    'BRNDY/CGNNC-CGNC-VS': 'BRNDY/CGNC-CGNC-VS',        # TOTAL row split merge error
    'BRNDY/CGN NC-CGNC-VS': 'BRNDY/CGNC-CGNC-VS',       # TOTAL row split with space
    'BRNDY/CG NC-CGNC-VS': 'BRNDY/CGNC-CGNC-VS',        # Space after CG
    'BRNDY/CGNC-CGNC-': 'BRNDY/CGNC-CGNC',              # Trailing dash (ambiguous - VS/VSOP/XO)
    'NC-CGNC-OTH': 'BRNDY/CGNC-CGNC-OTH',              # Truncated TOTAL row (missing BRNDY/CG)
    'NC-CGNC-VS': 'BRNDY/CGNC-CGNC-VS',                # Truncated TOTAL row
    'NC-CGNC-VSOP': 'BRNDY/CGNC-CGNC-VSOP',            # Truncated TOTAL row
    'NC-CGNC-XO': 'BRNDY/CGNC-CGNC-XO',                # Truncated TOTAL row
    # CRDL variations - OCR reads & as 8
    'CRDL-SNPS-BTRSCTC': 'CRDL-SNPS-BTRSCTCH',
    'CRDL-SNPS-BTRS': 'CRDL-SNPS-BTRSCTCH',   # Truncated class header
    'CRDL-LQR8SPC': 'CRDL-LQR&SPC',                      # 8 instead of &
    'CRDL-LQR8 SPC': 'CRDL-LQR&SPC',                     # 8 with space
    'CRDL-LQR8': 'CRDL-LQR&',                            # Just the prefix
    'CRDL-LQR&SPC-SPRT SPEC': 'CRDL-LQR&SPC-SPRT SPCTY',
    'CRDL-LQR&SPC-SPRT': 'CRDL-LQR&SPC-SPRT SPCTY',
    'CRDL-LQR&SPC-SPRT SPCT': 'CRDL-LQR&SPC-SPRT SPCTY', # Truncated SPCTY
    'CRDL-LQR&SPC- SPRT SPCTY': 'CRDL-LQR&SPC-SPRT SPCTY',
    'CRDL-LQR&SPC-SPR SPCTY': 'CRDL-LQR&SPC-SPRT SPCTY', # OCR reads SPR instead of SPRT
    'CRDL-LQR&SPC-FR': 'CRDL-LQR&SPC-FRT',             # Truncated FRT
    'CRDL-LQR&SPC-ANSI FLVRD': 'CRDL-LQR&SPC-ANSE FLVRD', # ANSI instead of ANSE
    'CRDL-LQR SPC-SLOE GIN': 'CRDL-LQR&SPC-SLOE GIN',   # Missing &
    'CRDL-LQR8 SPC-ANSE FLVR': 'CRDL-LQR&SPC-ANSE FLVRD', # 8 instead of &, truncated
    'CRDL-LQR&SPC-WHSK': 'CRDL-LQR&SPC-WHSKY',
    'CRDL-LQR&SPC- WHSKY': 'CRDL-LQR&SPC-WHSKY',
    'CRDL-LQR&SPC-WHSKY SPCTY': 'CRDL-LQR&SPC-WHSKY',   # Extra SPCTY at end
    'SPC-WHSKY': 'CRDL-LQR&SPC-WHSKY',                   # Missing prefix
    'SPC-WHSKY SPC': 'CRDL-LQR&SPC-WHSKY',               # OCR variant
    'CRDL-LQR&SPC-TRIPLE': 'CRDL-LQR&SPC-TRIPLE SEC',
    'TRIPLE SE': 'CRDL-LQR&SPC-TRIPLE SEC',              # Truncated
    'CRDL-LQR8 SPC-SLOE GIN': 'CRDL-LQR&SPC-SLOE GIN',   # 8 instead of &
    'CRDL-LQR8 &SPC-SPRT SPCT': 'CRDL-LQR&SPC-SPRT SPCTY', # Complex OCR error
    # OCR variations with space after dash
    'OTH IMP WHSKY- BLND': 'OTH IMP WHSKY-BLND',
}

# Class pattern prefixes for partial matching
CLASS_PATTERNS = [
    'DOM WHSKY-', 'SCOTCH-', 'CAN-', 'IRISH-', 'OTH IMP WHSKY',
    'GIN-CLASSIC', 'GIN-FLVRD', 'VODKA-CLASSIC', 'VODKA-FLVRD',
    'RUM-', 'BRNDY/CGNC', 'TEQUILA-', 'MEZCAL-', 'MEZCAL',
    'COCKTAILS', 'CRDL-', 'NEUTRAL', 'CACHACA'
]

# Words that are class suffixes (not vendors)
# These can appear after a dash or space in class names
CLASS_SUFFIXES = [
    'MALT', 'BLND', 'BTLD', 'BRBN', 'RYE', 'STRT', 'SNGL', 'FLVRD',
    'CLASSIC', 'GOLD', 'LIGHT', 'DARK', 'AGED', 'REPOSADO', 'BLANCO',
    'ANEJO', 'CRISTALINO', 'DOM', 'IMP', 'FRGN', 'TN', 'OTH', 'SM BTCH',
    'COFFEE', 'CRM', 'LQR', 'TRIPLE', 'SEC', 'FRT', 'SNPS', 'SPCTY',
    'BRBN/TN', 'BRBN TN', 'FRGN BTLD', 'US BTLD', 'SNGL MALT', '-OTH', '-VS', '-VSOP', '-XO',
    'ANSE FLVRD', 'SPRT SPCTY', 'SLOE GIN', 'TRIPLE SEC',
    # Cognac suffixes (VS, VSOP, XO)
    'VS', 'VSOP', 'XO', 'WHSKY',  # WHSKY is suffix for CRDL-LQR&SPC-WHSKY
]

# Patterns that indicate the start of a class name (prefixes)
CLASS_PREFIXES = [
    'DOM WHSKY', 'SCOTCH', 'CAN-', 'IRISH', 'OTH IMP',
    'GIN-', 'VODKA-', 'RUM-', 'BRNDY/CGNC', 'TEQUILA-', 'MEZCAL',
    'COCKTAILS', 'CRDL-', 'NEUTRAL', 'CACHACA'
]

def is_class_suffix(text):
    """Check if text is a class suffix (continuation of class name, not a vendor)

    IMPORTANT: This should be STRICT - only match actual class suffixes, not vendor names
    that happen to contain suffix-like substrings (e.g., "STOLLER IMPORTS" should NOT match
    just because "IMP" is in CLASS_SUFFIXES)
    """
    text_upper = text.upper().strip()

    # Empty text is not a suffix
    if not text_upper:
        return False

    # Check exact match first
    if text_upper in CLASS_SUFFIXES:
        return True

    # For multi-word suffixes (like "SNGL MALT", "TRIPLE SEC"), check if text starts with them
    for suffix in CLASS_SUFFIXES:
        if ' ' in suffix or '-' in suffix:  # Multi-word/compound suffix
            if text_upper == suffix or text_upper.startswith(suffix + ' ') or text_upper.startswith(suffix + '-'):
                return True

    # Class suffixes are typically SHORT (1-4 chars) like DOM, IMP, RYE, etc.
    # Vendor names are typically LONGER like "DISARONNO INTNL", "STOLLER IMPORTS"
    # Only accept short exact matches for single-word suffixes
    if len(text_upper) <= 4 and text_upper in CLASS_SUFFIXES:
        return True

    return False

def has_class_prefix(text):
    """Check if text EXACTLY starts with a class prefix (strict matching)"""
    text_upper = text.upper().strip()
    for prefix in CLASS_PREFIXES:
        # Must be exact prefix match with proper boundary
        if text_upper == prefix or text_upper.startswith(prefix + '-') or text_upper.startswith(prefix + ' '):
            return True
        # Also allow trailing dash (split class)
        if prefix.endswith('-') and text_upper.startswith(prefix):
            return True
    return False

def normalize_class_name(text):
    """Normalize class name to canonical form."""
    text_upper = text.upper().strip()

    # Strip common header words that may leak into class names
    # These appear when OCR merges header text with class row
    HEADER_WORDS_TO_STRIP = ['NABCA', 'CONTROL', 'STATES', 'BRAND', 'SUMMARY', 'PAGE']
    for word in HEADER_WORDS_TO_STRIP:
        if text_upper.endswith(' ' + word):
            text_upper = text_upper[:-len(word)-1].strip()

    # Normalize "- " (dash+space) to "-" - common OCR variation
    # e.g., "OTH IMP WHSKY- BLND" -> "OTH IMP WHSKY-BLND"
    import re
    text_upper = re.sub(r'-\s+', '-', text_upper)

    # Also normalize " -" (space+dash) to "-"
    text_upper = re.sub(r'\s+-', '-', text_upper)

    # Normalize spaces around slashes (BRBN/TN variations)
    # e.g., "BRBN /TN" or "BRBN/ TN" -> "BRBN/TN"
    text_upper = re.sub(r'\s+/', '/', text_upper)
    text_upper = re.sub(r'/\s+', '/', text_upper)

    # OCR reads & as 8 in "LQR&SPC" -> "LQR8SPC" or "LQR8 SPC"
    text_upper = text_upper.replace('LQR8', 'LQR&')
    text_upper = text_upper.replace('LQR& SPC', 'LQR&SPC')  # Fix extra space

    # Apply normalization mapping
    if text_upper in CLASS_NORMALIZATION:
        return CLASS_NORMALIZATION[text_upper]
    return text_upper

def matches_known_class(text):
    """Check if text matches or closely matches a known class name.

    Returns (matched_class, is_exact) tuple:
    - matched_class: the matching known class name (or None)
    - is_exact: True if exact match, False if fuzzy match
    """
    text_upper = text.upper().strip()

    # Apply normalization
    normalized = normalize_class_name(text_upper)
    if normalized != text_upper and normalized in KNOWN_CLASSES:
        return normalized, False

    # Exact match
    if text_upper in KNOWN_CLASSES:
        # Return normalized form
        return normalize_class_name(text_upper), True

    # Check for partial matches that complete to a known class
    for known_class in KNOWN_CLASSES:
        # Skip short class names to avoid false matches
        if len(known_class) < 5:
            continue
        # Allow off-by-one character differences (OCR errors)
        if len(text_upper) >= len(known_class) - 2 and len(text_upper) <= len(known_class) + 2:
            # Simple similarity check
            matches = sum(1 for a, b in zip(text_upper, known_class) if a == b)
            if matches >= len(known_class) - 2 and matches >= len(known_class) * 0.85:
                return normalize_class_name(known_class), False

    return None, False

def combine_split_class(col1, col2):
    """Try to combine split class names from col1 and col2.

    Returns: (combined_class, is_class) tuple
    - combined_class: the full class name if split was detected, else col1
    - is_class: True if this looks like a class row (not a brand)
    """
    col1_upper = col1.upper().strip()
    col2_upper = col2.upper().strip() if col2 else ""

    # Special case: NEUTRAL GRAIN + SPIRIT = NEUTRAL GRAIN SPIRIT
    if col1_upper == 'NEUTRAL GRAIN' and col2_upper == 'SPIRIT':
        return 'NEUTRAL GRAIN SPIRIT', True

    # Case 1: col1 ends with dash, col2 is class suffix
    # Example: "VODKA-CLASSIC-" + "DOM" -> "VODKA-CLASSIC-DOM"
    if col1_upper.endswith('-') and col2 and is_class_suffix(col2):
        combined = (col1 + col2).upper()
        # Verify this matches a known class
        matched, _ = matches_known_class(combined)
        if matched:
            return matched, True
        # If not exact match, still might be valid with OCR error
        return combined, True

    # Case 2: col1 has class prefix but missing suffix, col2 is class suffix
    # Example: "DOM WHSKY-SNGL" + "MALT" -> "DOM WHSKY-SNGL MALT"
    # Example: "BRNDY/CGNC-CGNC" + "VSOP" -> "BRNDY/CGNC-CGNC-VSOP"
    if col2 and is_class_suffix(col2) and has_class_prefix(col1):
        # Try different combination styles - PREFER DASH for cognac suffixes
        # Order matters: try dash first for VS/VSOP/XO to avoid fuzzy matching shorter class
        combinations_to_try = [
            (col1 + '-' + col2).upper(),           # With dash (most common for cognac)
            (col1 + ' ' + col2).strip().upper(),   # With space
            (col1 + col2).upper(),                  # Without separator
        ]

        # FIRST: Try exact matches only (avoid fuzzy matching returning wrong class)
        for combined in combinations_to_try:
            if combined in KNOWN_CLASSES:
                return normalize_class_name(combined), True

        # SECOND: Try matches_known_class (includes fuzzy) only if no exact match
        for combined in combinations_to_try:
            matched, exact = matches_known_class(combined)
            if matched and exact:  # Only accept exact matches here
                return matched, True

        # THIRD: Accept fuzzy match as last resort
        for combined in combinations_to_try:
            matched, _ = matches_known_class(combined)
            if matched:
                return matched, True

    # Case 3: col1 + col2 directly forms a known class (truncated class header)
    # Example: "CRDL-SNPS-BTRS" + "CTCH" -> "CRDL-SNPS-BTRSCTCH"
    if col2 and has_class_prefix(col1):
        combined = (col1 + col2).upper()
        matched, _ = matches_known_class(combined)
        if matched:
            return matched, True

    # Case 4: col1 exactly matches a known class
    matched, _ = matches_known_class(col1_upper)
    if matched and not col2_upper:  # Must have no vendor
        return matched, True

    return col1_upper, False

def is_class_row(text, col2_text):
    """Check if this is a class row (not a brand row).

    STRICT: Only returns True if text matches a known class name.
    """
    text_upper = text.upper().strip()
    col2_upper = col2_text.upper().strip() if col2_text else ""

    # TOTAL rows - skip these (used for validation)
    if text_upper.startswith('TOTAL'):
        return None  # Return None for TOTAL rows

    # Must NOT have vendor text (class rows don't have vendors)
    has_real_vendor = bool(col2_upper) and not is_class_suffix(col2_upper)
    if has_real_vendor:
        return False

    # Check if matches a known class (exact or fuzzy)
    matched, _ = matches_known_class(text_upper)
    if matched:
        return True

    return False

def find_brand_summary_pages(pdf_bytes):
    """Find pages containing Brand Summary table"""
    print("  Searching for Brand Summary pages...")

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        brand_summary_start = None
        brand_summary_end = None

        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            text_upper = text.upper()

            # Find start of Brand Summary
            if brand_summary_start is None:
                if "BRAND SUMMARY" in text_upper and "ALL CONTROL" in text_upper:
                    brand_summary_start = i
                    print(f"    Brand Summary starts at page {i+1}")

            # Find end (look for next major section after Brand Summary)
            if brand_summary_start is not None:
                # Stop at TOP 100 - VENDORS section
                if "TOP 100 - VENDORS" in text_upper:
                    brand_summary_end = i
                    print(f"    Brand Summary ends at page {i+1}")
                    break
                # Also check for VENDOR SUMMARY (page header variant)
                if i > brand_summary_start + 10 and "VENDOR SUMMARY" in text_upper:
                    brand_summary_end = i
                    print(f"    Brand Summary ends at page {i+1}")
                    break

        if brand_summary_start is None:
            print("    WARNING: Brand Summary not found, using default pages 8-370")
            return list(range(7, 370))

        if brand_summary_end is None:
            brand_summary_end = min(brand_summary_start + 400, len(pdf.pages))
            print(f"    WARNING: End not found, using page {brand_summary_end+1}")

        print(f"    Total Brand Summary pages: {brand_summary_end - brand_summary_start}")
        return list(range(brand_summary_start, brand_summary_end))

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

        time.sleep(3)

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

def parse_brand_summary_lines(blocks):
    """Parse LINE blocks to extract brand summary data

    Key insight: TOTAL rows mark the end of a class section.
    After a TOTAL row, the next row with text in col1 and no vendor is the new class.
    """

    # Group lines by page and Y position (row)
    lines_by_page = defaultdict(list)

    for block in blocks:
        if block['BlockType'] == 'LINE':
            page = block.get('Page', 0)
            text = block.get('Text', '')
            geo = block.get('Geometry', {}).get('BoundingBox', {})
            y = geo.get('Top', 0)
            x = geo.get('Left', 0)
            width = geo.get('Width', 0)

            # Skip copyright/footer text in left margin (x < 0.05)
            # This prevents "NABCA", "by", "2024," etc. from being grouped with data rows
            if x < 0.05:
                continue

            lines_by_page[page].append({
                'text': text,
                'y': y,
                'x': x,
                'width': width
            })

    # First pass: identify which pages are actual Brand Summary (not BRAND LEADERS or BY CLASS)
    brand_summary_pages = set()
    for page in lines_by_page.keys():
        page_text = ' '.join([l['text'] for l in lines_by_page[page]]).upper()
        # Must have "BRAND SUMMARY" but NOT "BRAND LEADERS" or "BY CLASS"
        if 'BRAND SUMMARY' in page_text and 'BRAND LEADERS' not in page_text and 'BY CLASS' not in page_text:
            brand_summary_pages.add(page)

    print(f"    Found {len(brand_summary_pages)} actual Brand Summary pages (excluding BRAND LEADERS/BY CLASS)")

    # Process each page
    records = []
    totals = {}  # For validation: class -> {column: total_value}
    class_sums = {}  # Running sum per class: class -> {column: sum}

    # Columns to validate
    VALIDATE_COLUMNS = ['l12m_cases_ty', 'l12m_cases_ly', 'ytd_cases_ty', 'curr_month_cases',
                        'curr_month_175l', 'curr_month_1l', 'curr_month_750ml',
                        'curr_month_750ml_traveler', 'curr_month_375ml', 'curr_month_200ml',
                        'curr_month_100ml', 'curr_month_50ml']

    current_class = None
    expect_new_class = True  # Start by expecting a class (first row after header)

    for page in sorted(lines_by_page.keys()):
        # Skip pages that are not actual Brand Summary
        if page not in brand_summary_pages:
            continue
        lines = lines_by_page[page]

        # Sort by Y, then X
        lines = sorted(lines, key=lambda l: (round(l['y'], 3), l['x']))

        # Group by Y position (same row)
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
            # Skip header rows - use 'CLASS & TYPE' not 'CLASS' to avoid matching VODKA-CLASSIC-DOM
            # Multi-word phrases that uniquely identify header rows
            header_phrases = ['CLASS & TYPE', 'CASE SALES', 'CONTROL STATES',
                              'This Year', 'Last Twelve', 'Current Month', '% OF TYPE']
            # Single words that should match as WHOLE WORDS only (to avoid filtering "CASTLE BRANDS")
            header_words = ['VENDOR', 'PAGE', 'NABCA', 'BOTTLE', 'TRAVELER', 'MONTH', 'YEAR']
            row_text = ' '.join([l['text'] for l in row]).upper()

            # Check multi-word phrases (simple substring match is fine)
            if any(h.upper() in row_text for h in header_phrases):
                continue

            # Check single words with word boundary matching
            # Use regex to match whole words only
            import re
            is_header = False
            for word in header_words:
                # Match whole word only (not part of "BRANDS", "PAGES", etc.)
                if re.search(r'\b' + word.upper() + r'\b', row_text):
                    # Additional check: "VENDOR" and "BRAND" as headers are typically in col1 position
                    # Skip the row only if this looks like a header row (typically has specific patterns)
                    col1_text = ' '.join([l['text'] for l in row if l['x'] < 0.14]).upper().strip()
                    # Header rows have "VENDOR" or "CLASS" in col1, not brand names
                    if col1_text in ['VENDOR', 'CLASS & TYPE', 'BRAND', ''] or 'CLASS' in col1_text:
                        is_header = True
                        break
            if is_header:
                continue

            # Separate columns by X position (based on actual Brand Summary layout)
            # Col1 (Brand/Class): x < 0.14 (class at ~0.067, brand at ~0.061)
            # Col2 (Vendor): 0.14 <= x < 0.27 (vendor at ~0.167)
            # Col3+ (Data): x >= 0.27 (numeric data)

            col1_parts = [l['text'] for l in row if l['x'] < 0.14]
            col2_parts = [l['text'] for l in row if 0.14 <= l['x'] < 0.27]
            col3_parts = [l for l in row if l['x'] >= 0.27]

            col1 = ' '.join(col1_parts).strip()
            col2 = ' '.join(col2_parts).strip()

            if not col1:
                continue

            col1_upper = col1.upper()

            # Check for TOTAL row first - marks end of current class
            if col1_upper.startswith('TOTAL'):
                # Extract full class name from TOTAL row (may be split across col1+col2)
                # e.g., "TOTAL BRNDY/CGN" + "NC-CGNC-VS" → "BRNDY/CGNC-CGNC-VS"
                total_class_text = col1_upper.replace('TOTAL', '').strip()
                if col2:
                    total_class_text = total_class_text + col2.upper()
                # Normalize the class name
                total_class_name = normalize_class_name(total_class_text)
                matched_class, _ = matches_known_class(total_class_name) if total_class_name else (None, 0)

                # Only update class if current_class was ambiguous (no real class detected yet)
                # or if it's an exact prefix match (e.g., "BRNDY/CGNC-CGNC" → "BRNDY/CGNC-CGNC-VS")
                if matched_class and matched_class != current_class:
                    # Only backfill if current_class is a prefix of matched_class
                    # This handles cases like "BRNDY/CGNC-CGNC" → "BRNDY/CGNC-CGNC-VS"
                    if current_class and matched_class.startswith(current_class):
                        for rec in records:
                            if rec.get('class') == current_class:
                                rec['class'] = matched_class
                        if current_class in class_sums:
                            class_sums[matched_class] = class_sums.pop(current_class)
                        current_class = matched_class

                # Extract ALL column values from TOTAL row for validation
                if col3_parts and current_class:
                    # Use same position-based extraction as data rows
                    # Column bounds for TOTAL row validation
                    TOTAL_BOUNDS = [
                        ('l12m_cases_ty', 0.27, 0.335),
                        ('l12m_cases_ly', 0.335, 0.40),
                        ('ytd_cases_ty', 0.44, 0.50),
                        ('curr_month_cases', 0.50, 0.54),
                        ('curr_month_175l', 0.54, 0.60),
                        ('curr_month_1l', 0.60, 0.65),
                        ('curr_month_750ml', 0.65, 0.73),
                        ('curr_month_750ml_traveler', 0.73, 0.75),  # Shifted: x=0.734
                        ('curr_month_375ml', 0.75, 0.82),          # Shifted: x=0.755
                        ('curr_month_200ml', 0.82, 0.86),
                        ('curr_month_100ml', 0.86, 0.905),
                        ('curr_month_50ml', 0.905, 0.96),
                    ]

                    total_values = {}
                    # Sort by x position to process left-to-right
                    sorted_parts = sorted(col3_parts, key=lambda p: p['x'])
                    for part in sorted_parts:
                        text = part['text'].replace(',', '').strip()
                        x_pos = part['x']
                        try:
                            val = int(float(text)) if '.' not in text else None
                            if val is not None:
                                for col_name, x_min, x_max in TOTAL_BOUNDS:
                                    if x_min <= x_pos < x_max:
                                        # Only use first value for each column (don't overwrite)
                                        if col_name not in total_values:
                                            total_values[col_name] = val
                                        break
                        except:
                            pass

                    totals[current_class] = total_values

                    # Validate running sums against TOTAL values (0% tolerance - must match exactly)
                    class_sum_data = class_sums.get(current_class, {})
                    mismatches = []
                    for col in VALIDATE_COLUMNS:
                        calculated = class_sum_data.get(col, 0)
                        expected = total_values.get(col, 0)
                        if expected > 0 and calculated > 0:
                            diff_pct = abs(calculated - expected) / expected * 100
                            if diff_pct > 0.01:  # Allow only 0.01% tolerance for rounding
                                mismatches.append(f"{col}: calc={calculated:,} vs TOTAL={expected:,} ({diff_pct:.1f}%)")

                    if mismatches:
                        print(f"      WARNING: {current_class} column mismatches:")
                        for m in mismatches[:3]:  # Show top 3 mismatches
                            print(f"        - {m}")

                # After TOTAL, expect next data row to be a new class
                expect_new_class = True
                continue

            # Try to combine split class names and detect if this is a class row
            combined_class, detected_as_class = combine_split_class(col1, col2)
            has_vendor = bool(col2) and not is_class_suffix(col2)

            # STRICT class detection:
            # Only accept as class if it matches a known class name
            if detected_as_class:
                # Verify it's a real known class (not a brand that starts with class-like prefix)
                matched, _ = matches_known_class(combined_class)
                if matched:
                    current_class = matched
                    class_sums[current_class] = {col: 0 for col in VALIDATE_COLUMNS}
                    expect_new_class = False
                    continue
                # If col1 ends with dash and combines with col2, trust it
                elif col1_upper.endswith('-'):
                    current_class = combined_class
                    class_sums[current_class] = {col: 0 for col in VALIDATE_COLUMNS}
                    expect_new_class = False
                    continue

            # Also check if col1 alone matches a known class (and no vendor)
            if not has_vendor:
                matched, _ = matches_known_class(col1_upper)
                if matched:
                    current_class = matched
                    class_sums[current_class] = {col: 0 for col in VALIDATE_COLUMNS}
                    expect_new_class = False
                    continue

            # Reset expect_new_class since we didn't find a class
            expect_new_class = False

            # Brand row - must have vendor and current class
            if current_class and has_vendor:
                # Extract numeric data using position-based column mapping
                # Column X position boundaries (based on PDF header analysis)
                # Headers: "Last Twelve" @0.272, "Last Year" @0.337, "%" @0.405
                #          "This Year to Date" @0.399, "Current" @0.499
                #          Bottle sizes: 1.75L @0.554, 1.0L @0.607, 750ml @0.654, 375ml @0.753, 200ml @0.806
                # Column structure (13 numeric columns):
                # L12M: TY, LY, % | YTD: TY only | Current Month: Total + 8 bottle sizes
                # Bottle sizes: 1.75L, 1.0L, 750ml, 750ml Traveler, 375ml, 200ml, 100ml, 50ml
                # Header X positions from Textract: 1.75L@0.554, 1.0L@0.607, 750ml@0.654,
                # 750mlTrav@0.704, 375ml@0.753, 200ml@0.806, 100ml@0.857, 50ml@0.908
                # Column bounds adjusted based on actual TOTAL row value positions
                # Key adjustment: 750ml_traveler/375ml boundary at 0.73 (not 0.75)
                COLUMN_BOUNDS = [
                    ('l12m_cases_ty', 0.27, 0.335),      # L12M This Year
                    ('l12m_cases_ly', 0.335, 0.40),      # L12M Last Year
                    ('l12m_pct_change', 0.40, 0.44),     # L12M % Change
                    ('ytd_cases_ty', 0.44, 0.50),        # YTD This Year (only column in YTD)
                    ('curr_month_cases', 0.50, 0.54),    # Current Month Total
                    ('curr_month_175l', 0.54, 0.60),     # 1.75L (header at 0.554)
                    ('curr_month_1l', 0.60, 0.65),       # 1.0L (header at 0.607)
                    ('curr_month_750ml', 0.65, 0.73),    # 750ml (header at 0.654)
                    ('curr_month_750ml_traveler', 0.73, 0.75),  # Shifted: x=0.734  # 750ml Traveler - narrower!
                    ('curr_month_375ml', 0.75, 0.82),          # Shifted: x=0.755    # 375ml starts at 0.73
                    ('curr_month_200ml', 0.82, 0.86),    # 200ml (header at 0.806)
                    ('curr_month_100ml', 0.86, 0.905),    # 100ml (header at 0.857)
                    ('curr_month_50ml', 0.905, 0.96),     # 50ml (header at 0.908)
                ]

                # Map values to columns based on X position
                col_values = {}
                for part in col3_parts:
                    text = part['text'].replace(',', '').strip()
                    x_pos = part['x']

                    try:
                        if '.' in text:
                            val = float(text)
                        else:
                            val = int(text)

                        # Find which column this value belongs to
                        for col_name, x_min, x_max in COLUMN_BOUNDS:
                            if x_min <= x_pos < x_max:
                                col_values[col_name] = val
                                break
                    except:
                        pass

                l12m_cases = col_values.get('l12m_cases_ty')

                record = {
                    'class': current_class,
                    'brand': col1,
                    'vendor': col2,
                    'l12m_cases_ty': l12m_cases,
                    'l12m_cases_ly': col_values.get('l12m_cases_ly'),
                    'l12m_pct_change': col_values.get('l12m_pct_change'),
                    'ytd_cases_ty': col_values.get('ytd_cases_ty'),
                    'curr_month_cases': col_values.get('curr_month_cases'),
                    # Current Month breakdown by bottle size (8 sizes)
                    'curr_month_175l': col_values.get('curr_month_175l'),
                    'curr_month_1l': col_values.get('curr_month_1l'),
                    'curr_month_750ml': col_values.get('curr_month_750ml'),
                    'curr_month_750ml_traveler': col_values.get('curr_month_750ml_traveler'),
                    'curr_month_375ml': col_values.get('curr_month_375ml'),
                    'curr_month_200ml': col_values.get('curr_month_200ml'),
                    'curr_month_100ml': col_values.get('curr_month_100ml'),
                    'curr_month_50ml': col_values.get('curr_month_50ml'),
                }

                records.append(record)

                # Add to running sums for ALL columns validation
                if current_class in class_sums:
                    for col in VALIDATE_COLUMNS:
                        val = col_values.get(col)
                        if val is not None and isinstance(val, (int, float)):
                            class_sums[current_class][col] += int(val)

    # Print class summary (sorted by l12m_cases_ty)
    print(f"    Classes detected: {len(class_sums)}")
    sorted_classes = sorted(class_sums.items(), key=lambda x: -x[1].get('l12m_cases_ty', 0))
    for cls, sums in sorted_classes[:15]:
        calc_l12m = sums.get('l12m_cases_ty', 0)
        total_data = totals.get(cls, {})
        total_l12m = total_data.get('l12m_cases_ty', 0) if isinstance(total_data, dict) else 0
        print(f"      {cls:40}: {calc_l12m:>10,} (TOTAL: {total_l12m:>10,})")

    return records, totals

def extract_brand_summary(pdf_key, year, month):
    """Extract Brand Summary from a single PDF"""
    print(f"\nProcessing {pdf_key} ({year}-{month:02d})...")

    # Check for cached Textract output
    cache_file = f"cache/textract_{year}_{month:02d}.json"
    if os.path.exists(cache_file):
        print(f"  Loading cached Textract output from {cache_file}...")
        with open(cache_file, 'r') as f:
            blocks = json.load(f)
        print(f"    Total blocks: {len(blocks)}")
    else:
        # Download PDF
        print("  Downloading PDF...")
        response = s3.get_object(Bucket=BUCKET, Key=pdf_key)
        pdf_bytes = response['Body'].read()
        print(f"    Size: {len(pdf_bytes) / 1024 / 1024:.1f} MB")

        # Find Brand Summary pages
        brand_pages = find_brand_summary_pages(pdf_bytes)
        print(f"  Found {len(brand_pages)} Brand Summary pages")

        # Extract only those pages
        print("  Creating subset PDF...")
        subset_pdf = extract_pages_as_pdf(pdf_bytes, brand_pages)
        print(f"    Subset size: {len(subset_pdf) / 1024:.1f} KB")

        # Upload to S3
        subset_key = f"temp/brand_summary_{year}_{month:02d}.pdf"
        s3.put_object(Bucket=BUCKET, Key=subset_key, Body=subset_pdf)
        print(f"  Uploaded to s3://{BUCKET}/{subset_key}")

        # Run Textract
        print("  Starting Textract job...")
        job_id = start_textract_job(subset_key)
        print(f"    Job ID: {job_id}")

        print("  Waiting for Textract...")
        wait_for_job(job_id)
        print("    Completed!")

        # Get results
        print("  Retrieving results...")
        blocks = get_all_results(job_id)
        print(f"    Total blocks: {len(blocks)}")

        # Save to cache
        os.makedirs('cache', exist_ok=True)
        with open(cache_file, 'w') as f:
            json.dump(blocks, f)
        print(f"    Cached to {cache_file}")

        # Cleanup temp file
        try:
            s3.delete_object(Bucket=BUCKET, Key=subset_key)
        except:
            pass

    # Parse data
    print("  Parsing Brand Summary data...")
    records, totals = parse_brand_summary_lines(blocks)

    # Add report date to records
    for record in records:
        record['report_year'] = year
        record['report_month'] = month

    print(f"    Extracted {len(records)} brand records")
    print(f"    Found {len(totals)} total rows for validation")

    return records, totals

def main():
    import sys

    # Parse CLI arguments
    target_months = None  # None means all months
    if len(sys.argv) > 1:
        target_months = set()
        for arg in sys.argv[1:]:
            if '-' in arg:  # Format: YYYY-MM
                parts = arg.split('-')
                if len(parts) == 2:
                    target_months.add((int(parts[0]), int(parts[1])))

    print("=" * 60)
    print("BRAND SUMMARY EXTRACTION - ALL MONTHS")
    print("=" * 60)
    if target_months:
        print(f"Processing specific months: {sorted(target_months)}")

    os.makedirs('output', exist_ok=True)

    all_records = []
    all_totals = {}
    processed_months = []

    fieldnames = ['class', 'brand', 'vendor', 'l12m_cases_ty', 'l12m_cases_ly',
                 'l12m_pct_change', 'ytd_cases_ty', 'curr_month_cases',
                 'curr_month_175l', 'curr_month_1l', 'curr_month_750ml',
                 'curr_month_750ml_traveler', 'curr_month_375ml', 'curr_month_200ml',
                 'curr_month_100ml', 'curr_month_50ml',
                 'report_year', 'report_month']

    # Process each PDF
    for pdf_name, (year, month) in sorted(PDF_DATE_MAP.items(), key=lambda x: (x[1][0], x[1][1])):
        # Skip if not in target months (when filtering)
        if target_months and (year, month) not in target_months:
            continue
        pdf_key = f"raw-pdfs/{pdf_name}"

        try:
            records, totals = extract_brand_summary(pdf_key, year, month)
            all_records.extend(records)
            all_totals.update(totals)
            processed_months.append((year, month, len(records)))

            # Save individual month CSV
            month_csv = f'output/brand_summary_{year}_{month:02d}.csv'
            with open(month_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(records)
            print(f"  Saved {len(records)} records to {month_csv}")

        except Exception as e:
            print(f"  ERROR processing {pdf_name}: {e}")
            import traceback
            traceback.print_exc()

    # Save combined CSV with all months
    combined_csv = 'output/brand_summary_all_months.csv'
    if all_records:
        with open(combined_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(all_records)
        print(f"\n{'='*60}")
        print(f"COMBINED: Saved {len(all_records)} records to {combined_csv}")

    # Summary
    print(f"\n{'='*60}")
    print("EXTRACTION SUMMARY")
    print(f"{'='*60}")
    print(f"Total months processed: {len(processed_months)}")
    print(f"Total records extracted: {len(all_records)}")
    print(f"\nRecords per month:")
    for year, month, count in processed_months:
        print(f"  {year}-{month:02d}: {count:,} records")

    # Show class distribution
    print(f"\nTop 15 classes by record count:")
    class_counts = defaultdict(int)
    for r in all_records:
        class_counts[r['class']] += 1

    for cls, count in sorted(class_counts.items(), key=lambda x: -x[1])[:15]:
        print(f"  {cls[:35]:35}: {count:6,} brands")

    print("\nDone!")

if __name__ == "__main__":
    main()
