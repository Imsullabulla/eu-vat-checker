import streamlit as st
import pandas as pd
import requests
import time
import re
import uuid
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from thefuzz import fuzz

# --- SETUP ---
st.set_page_config(page_title="EU VAT Checker", layout="wide", page_icon="🇪🇺")

# --- CUSTOM CSS THEME ---
st.markdown("""
<style>
    /* ---- Global ---- */
    .block-container { padding-top: 1.5rem !important; max-width: 1100px; }

    /* ---- Header ---- */
    .app-header {
        background: linear-gradient(135deg, #003399 0%, #0055a4 100%);
        color: white;
        padding: 1.8rem 2rem;
        border-radius: 12px;
        margin-bottom: 1.5rem;
        box-shadow: 0 4px 16px rgba(0,51,153,0.15);
    }
    .app-header h1 { color: white !important; margin: 0 0 0.3rem 0; font-size: 1.9rem; }
    .app-header p { color: rgba(255,255,255,0.85); margin: 0; font-size: 0.95rem; }

    /* ---- Step Indicators ---- */
    .step-bar {
        display: flex;
        gap: 0;
        margin: 0.5rem 0 1.5rem 0;
        background: #f0f2f6;
        border-radius: 8px;
        overflow: hidden;
    }
    .step-item {
        flex: 1;
        text-align: center;
        padding: 0.6rem 0.5rem;
        font-size: 0.82rem;
        font-weight: 500;
        color: #888;
        border-right: 1px solid #e0e3e8;
    }
    .step-item:last-child { border-right: none; }
    .step-active {
        background: #003399;
        color: white !important;
        font-weight: 600;
    }
    .step-done {
        background: #d4edda;
        color: #155724 !important;
    }

    /* ---- Metric Cards ---- */
    .metric-row { display: flex; gap: 0.75rem; margin: 1rem 0; flex-wrap: wrap; }
    .metric-card {
        flex: 1;
        min-width: 120px;
        background: white;
        border-radius: 10px;
        padding: 1rem 1.2rem;
        box-shadow: 0 1px 4px rgba(0,0,0,0.07);
        border-left: 4px solid #ccc;
        text-align: center;
    }
    .metric-card .metric-value { font-size: 1.8rem; font-weight: 700; line-height: 1.2; }
    .metric-card .metric-label { font-size: 0.78rem; color: #666; margin-top: 0.2rem; text-transform: uppercase; letter-spacing: 0.03em; }
    .metric-card:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(0,0,0,0.12); transition: all 0.2s; }
    .metric-valid { border-left-color: #28a745; }
    .metric-valid .metric-value { color: #28a745; }
    .metric-invalid { border-left-color: #dc3545; }
    .metric-invalid .metric-value { color: #dc3545; }
    .metric-format { border-left-color: #6f42c1; }
    .metric-format .metric-value { color: #6f42c1; }
    .metric-service { border-left-color: #fd7e14; }
    .metric-service .metric-value { color: #fd7e14; }
    .metric-other { border-left-color: #6c757d; }
    .metric-other .metric-value { color: #6c757d; }
    .metric-verified { border-left-color: #28a745; }
    .metric-verified .metric-value { color: #28a745; }
    .metric-check { border-left-color: #fd7e14; }
    .metric-check .metric-value { color: #fd7e14; }
    .metric-fraud { border-left-color: #dc3545; background: #fff5f5; }
    .metric-fraud .metric-value { color: #dc3545; }

    /* ---- Status Badges ---- */
    .badge {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 12px;
        font-size: 0.78rem;
        font-weight: 600;
        letter-spacing: 0.02em;
        background: #e9ecef;
        color: #495057;
    }
    .badge-success { background: #d4edda; color: #155724; }
    .badge-info { background: #d1ecf1; color: #0c5460; }

    /* ---- Section Headers ---- */
    .section-header {
        font-size: 1.05rem;
        font-weight: 600;
        color: #1a1a2e;
        padding-bottom: 0.4rem;
        border-bottom: 2px solid #003399;
        margin: 1.5rem 0 0.8rem 0;
        display: inline-block;
    }

    /* ---- Info Box ---- */
    .info-box {
        background: #f8f9fa;
        border: 1px solid #e9ecef;
        border-radius: 8px;
        padding: 1rem 1.2rem;
        margin: 0.8rem 0;
    }
    .info-box-blue {
        background: #e8f4fd;
        border-color: #b8daff;
    }

    /* ---- Sidebar ---- */
    section[data-testid="stSidebar"] {
        background: #f8f9fa;
    }
    section[data-testid="stSidebar"] .block-container {
        padding-top: 1rem;
    }
    .sidebar-section {
        background: white;
        border-radius: 8px;
        padding: 1rem;
        margin-bottom: 0.8rem;
        border: 1px solid #e9ecef;
    }
    .sidebar-title {
        font-size: 0.85rem;
        font-weight: 600;
        color: #003399;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 0.5rem;
    }

    /* ---- Download Buttons ---- */
    .download-section {
        background: #f8f9fa;
        border-radius: 10px;
        padding: 1.2rem;
        margin-top: 1rem;
        border: 1px solid #e9ecef;
    }

    /* ---- Summary Box ---- */
    .summary-box {
        background: linear-gradient(135deg, #f0f7ff 0%, #e8f4fd 100%);
        border: 1px solid #b8daff;
        border-radius: 10px;
        padding: 1.2rem 1.5rem;
        margin: 1rem 0;
    }
    .summary-box h4 { margin: 0 0 0.5rem 0; color: #003399; font-size: 1rem; }

    /* ---- Hide default Streamlit branding ---- */
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }

    /* ---- Table improvements ---- */
    .stDataFrame { border-radius: 8px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)

# --- SESSION-BASED ISOLATION FOR MULTI-USER SAFETY ---
# Generate unique session ID for each user to prevent race conditions
if 'session_id' not in st.session_state:
    st.session_state['session_id'] = str(uuid.uuid4())

# Cache directory for checkpoint files
CACHE_DIR = "cache"

def get_cache_filename():
    """Returns the session-specific cache filename"""
    return os.path.join(CACHE_DIR, f"cache_{st.session_state['session_id']}.csv")

def ensure_cache_dir():
    """Creates cache directory if it doesn't exist"""
    if not os.path.exists(CACHE_DIR):
        try:
            os.makedirs(CACHE_DIR)
        except OSError:
            pass  # Directory may have been created by another process

def save_checkpoint(results_dict, tasks_total, completed_indices):
    """
    Saves current progress to a session-specific cache file.

    Args:
        results_dict: Dictionary of {index: result_dict} for completed items
        tasks_total: Total number of tasks to process
        completed_indices: Set of indices that have been completed
    """
    ensure_cache_dir()
    cache_file = get_cache_filename()

    try:
        # Convert results to DataFrame
        if results_dict:
            sorted_results = [results_dict[i] for i in sorted(results_dict.keys())]
            df = pd.DataFrame(sorted_results)
            # Add metadata columns
            df['_checkpoint_total'] = tasks_total
            df['_checkpoint_index'] = list(sorted(results_dict.keys()))
            df.to_csv(cache_file, index=False)
    except Exception:
        pass  # Silently fail to avoid disrupting the main process

def load_checkpoint():
    """
    Loads progress from session-specific cache file if it exists.

    Returns:
        tuple: (results_dict, completed_indices, tasks_total) or (None, None, None) if no checkpoint
    """
    cache_file = get_cache_filename()

    if not os.path.exists(cache_file):
        return None, None, None

    try:
        df = pd.read_csv(cache_file)

        if df.empty or '_checkpoint_total' not in df.columns:
            return None, None, None

        tasks_total = int(df['_checkpoint_total'].iloc[0])
        completed_indices = set(df['_checkpoint_index'].astype(int).tolist())

        # Remove metadata columns and reconstruct results_dict
        result_columns = [col for col in df.columns if not col.startswith('_checkpoint_')]
        results_df = df[result_columns]

        results_dict = {}
        for idx, row in zip(df['_checkpoint_index'].astype(int), results_df.to_dict('records')):
            results_dict[idx] = row

        return results_dict, completed_indices, tasks_total
    except Exception:
        return None, None, None

def cleanup_checkpoint():
    """Deletes the session-specific cache file to free disk space"""
    cache_file = get_cache_filename()
    try:
        if os.path.exists(cache_file):
            os.remove(cache_file)
    except Exception:
        pass  # Silently fail

def has_checkpoint():
    """Checks if a checkpoint file exists for this session"""
    return os.path.exists(get_cache_filename())

API_URL_GET = "https://ec.europa.eu/taxation_customs/vies/rest-api/ms/{}/vat/{}"
API_URL_POST = "https://ec.europa.eu/taxation_customs/vies/rest-api/check-vat-number"
MAX_WORKERS = 8  # Number of concurrent threads
MAX_RETRIES = 3  # Number of retry attempts for failed requests
RETRY_BASE_DELAY = 0.5  # Base delay for exponential backoff (0.5s, 1s, 2s)
RATE_LIMIT_SLEEP = 0.5  # Seconds to wait before each API call

# Thread-safe lock for any shared resources
results_lock = threading.Lock()

# Circuit breaker: Track service unavailable errors per country
# If a country has 3+ errors, reduce retries to 1 for remaining VATs
country_error_count = {}
country_error_lock = threading.Lock()
CIRCUIT_BREAKER_THRESHOLD = 3  # After 3 failures, reduce retries

# --- ERROR CLASSIFICATION ---
# These errors indicate the service is temporarily unavailable (NOT invalid VAT)
SERVICE_UNAVAILABLE_ERRORS = {
    "MS_UNAVAILABLE",
    "MS_MAX_CONCURRENT_REQ",
    "SERVICE_UNAVAILABLE",
    "TIMEOUT",
    "GLOBAL_MAX_CONCURRENT_REQ",
    "VAT_BLOCKED",
    "IP_BLOCKED",
}

# HTTP status codes that indicate service issues (should retry)
RETRY_HTTP_CODES = {500, 502, 503, 504, 429}

# --- EU VAT NUMBER FORMATS ---
# Format patterns and examples for each EU member state
EU_VAT_FORMATS = {
    "AT": {"pattern": r"^U\d{8}$", "format": "ATU12345678", "description": "U + 8 digits"},
    "BE": {"pattern": r"^[01]\d{9}$", "format": "BE0123456789", "description": "0 or 1 + 9 digits"},
    "BG": {"pattern": r"^\d{9,10}$", "format": "BG123456789", "description": "9 or 10 digits"},
    "CY": {"pattern": r"^\d{8}[A-Z]$", "format": "CY12345678X", "description": "8 digits + 1 letter"},
    "CZ": {"pattern": r"^\d{8,10}$", "format": "CZ12345678", "description": "8, 9 or 10 digits"},
    "DE": {"pattern": r"^\d{9}$", "format": "DE123456789", "description": "9 digits"},
    "DK": {"pattern": r"^\d{8}$", "format": "DK12345678", "description": "8 digits"},
    "EE": {"pattern": r"^\d{9}$", "format": "EE123456789", "description": "9 digits"},
    "EL": {"pattern": r"^\d{9}$", "format": "EL123456789", "description": "9 digits (Greece)"},
    "ES": {"pattern": r"^[A-Z0-9]\d{7}[A-Z0-9]$", "format": "ESX1234567X", "description": "Letter/digit + 7 digits + letter/digit"},
    "FI": {"pattern": r"^\d{8}$", "format": "FI12345678", "description": "8 digits"},
    "FR": {"pattern": r"^[A-Z0-9]{2}\d{9}$", "format": "FRXX123456789", "description": "2 characters + 9 digits"},
    "HR": {"pattern": r"^\d{11}$", "format": "HR12345678901", "description": "11 digits"},
    "HU": {"pattern": r"^\d{8}$", "format": "HU12345678", "description": "8 digits"},
    "IE": {"pattern": r"^\d{7}[A-Z]{1,2}$|^\d[A-Z+*]\d{5}[A-Z]$", "format": "IE1234567X or IE1X12345X", "description": "7 digits + 1-2 letters, or special format"},
    "IT": {"pattern": r"^\d{11}$", "format": "IT12345678901", "description": "11 digits"},
    "LT": {"pattern": r"^\d{9}$|^\d{12}$", "format": "LT123456789", "description": "9 or 12 digits"},
    "LU": {"pattern": r"^\d{8}$", "format": "LU12345678", "description": "8 digits"},
    "LV": {"pattern": r"^\d{11}$", "format": "LV12345678901", "description": "11 digits"},
    "MT": {"pattern": r"^\d{8}$", "format": "MT12345678", "description": "8 digits"},
    "NL": {"pattern": r"^\d{9}B\d{2}$", "format": "NL123456789B01", "description": "9 digits + B + 2 digits"},
    "PL": {"pattern": r"^\d{10}$", "format": "PL1234567890", "description": "10 digits"},
    "PT": {"pattern": r"^\d{9}$", "format": "PT123456789", "description": "9 digits"},
    "RO": {"pattern": r"^\d{2,10}$", "format": "RO1234567890", "description": "2 to 10 digits"},
    "SE": {"pattern": r"^\d{12}$", "format": "SE123456789012", "description": "12 digits"},
    "SI": {"pattern": r"^\d{8}$", "format": "SI12345678", "description": "8 digits"},
    "SK": {"pattern": r"^\d{10}$", "format": "SK1234567890", "description": "10 digits"},
    # Non-EU but sometimes used
    "XI": {"pattern": r"^\d{9}$|^\d{12}$|^GD\d{3}$|^HA\d{3}$", "format": "XI123456789", "description": "Northern Ireland: 9 or 12 digits"},
}

def get_vat_format_info(country_code):
    """Returns format information for a given country code"""
    if country_code in EU_VAT_FORMATS:
        info = EU_VAT_FORMATS[country_code]
        return info["format"], info["description"]
    return "Unknown", "Country code not recognized"

def validate_vat_format(country_code, vat_number):
    """Validates if the VAT number matches the expected format for the country"""
    if country_code not in EU_VAT_FORMATS:
        return False, "Unknown country code"

    pattern = EU_VAT_FORMATS[country_code]["pattern"]
    if re.match(pattern, vat_number):
        return True, ""
    return False, EU_VAT_FORMATS[country_code]["description"]

# --- FRAUD DETECTION FUNCTIONS ---

def calculate_name_similarity(name1, name2):
    """
    Calculate similarity score between two company names using fuzzy matching.
    Returns a score from 0-100.

    Uses token_set_ratio as primary (handles extra words like A/S, GmbH, Ltd)
    and partial_ratio as secondary (handles substring matches like LEGO in LEGO System A/S).
    Takes the HIGHEST score to reduce false positives.
    """
    if not name1 or not name2:
        return 0

    # Clean and normalize names
    name1_clean = str(name1).upper().strip()
    name2_clean = str(name2).upper().strip()

    if name1_clean == "---" or name2_clean == "---":
        return 0

    # Primary: token_set_ratio - best for company names with extra words
    # Example: "LEGO" vs "LEGO System A/S" = high score
    # Example: "Google" vs "Google Ireland Ltd" = high score
    token_set_score = fuzz.token_set_ratio(name1_clean, name2_clean)

    # Secondary: partial_ratio - good for substring matches
    # Example: "LEGO" contained in "LEGO System A/S" = high score
    partial_score = fuzz.partial_ratio(name1_clean, name2_clean)

    # Additional: token_sort_ratio - handles word order differences
    # Example: "Company ABC" vs "ABC Company" = high score
    token_sort_score = fuzz.token_sort_ratio(name1_clean, name2_clean)

    # Return the HIGHEST score to reduce false positives
    return max(token_set_score, partial_score, token_sort_score)

def get_identity_risk(score):
    """
    Determine identity risk level based on similarity score.

    Thresholds:
    - > 60: High confidence match (Verified)
    - 20-60: Partial match, needs manual review (Check Manually)
    - < 20: Little to no resemblance (POTENTIAL FRAUD)
    """
    if score > 60:
        return "Verified"
    elif score >= 20:
        return "Check Manually"
    else:
        return "POTENTIAL FRAUD"

# --- FUNCTIONS ---

def normalize_vat_input(value):
    """
    Robustly normalize VAT input from Excel to a clean string.

    Handles:
    - NaN/None: Returns None (skip)
    - Float (e.g., 12345678.0): Removes .0, returns "12345678"
    - Integer (e.g., 12345678): Converts to "12345678"
    - String with .0 (e.g., "12345678.0"): Removes .0
    - Normal strings: Returns as-is after stripping

    Returns:
        tuple: (normalized_string, debug_info)
               debug_info shows what transformation was applied
    """
    import math

    # Handle NaN/None
    if value is None:
        return None, "NULL"

    if isinstance(value, float):
        if math.isnan(value):
            return None, "NaN"
        # Convert float to int first to remove .0, then to string
        # This handles 12345678.0 -> 12345678 -> "12345678"
        try:
            int_value = int(value)
            return str(int_value), f"FLOAT:{value}->{int_value}"
        except (ValueError, OverflowError):
            return str(value), f"FLOAT_ERR:{value}"

    if isinstance(value, int):
        return str(value), f"INT:{value}"

    # It's a string - clean it up
    str_value = str(value).strip()

    # Handle "nan" string (pandas sometimes does this)
    if str_value.lower() == "nan" or str_value == "":
        return None, "EMPTY_STR"

    # Handle string with .0 suffix (e.g., "12345678.0")
    if str_value.endswith(".0"):
        cleaned = str_value[:-2]
        return cleaned, f"STR_DOT0:{str_value}->{cleaned}"

    # Handle scientific notation strings (e.g., "1.23456e+10")
    if "e+" in str_value.lower() or "e-" in str_value.lower():
        try:
            float_val = float(str_value)
            int_val = int(float_val)
            return str(int_val), f"SCI_NOTATION:{str_value}->{int_val}"
        except (ValueError, OverflowError):
            pass

    return str_value, f"STR:{str_value}"


def clean_vat_number(text):
    """
    Strictly cleans VAT number input.

    - Extracts 2-letter country code from the beginning
    - Removes ALL special characters (spaces, dashes, dots, etc.)
    - Keeps only alphanumeric characters in the number portion

    Example: "DK-12 34.56" -> Country="DK", Number="123456"
    Example: "FR AB 123456789" -> Country="FR", Number="AB123456789"
    """
    if text is None:
        return None, None

    # Convert to uppercase and remove ALL non-alphanumeric characters
    clean_text = re.sub(r'[^A-Z0-9]', '', str(text).upper())

    if len(clean_text) < 3:
        return None, None

    # First 2 characters must be letters (country code)
    country = clean_text[:2]
    if not country.isalpha():
        return None, None

    number = clean_text[2:]
    return country, number


def detect_data_format(df, vat_keywords, country_keywords):
    """
    Auto-detect whether VAT data is in Combined or Separate format.

    Returns:
        tuple: (format_string, detected_vat_col, detected_country_col)
        - format_string: "Combined (e.g., DK12345678)" or "Separate Columns (Country + Number)"
        - detected_vat_col: Best guess for VAT column
        - detected_country_col: Best guess for country column (or None if Combined)
    """
    eu_country_codes = set(EU_VAT_FORMATS.keys())

    # Find potential VAT and country columns by keywords
    vat_col_by_keyword = None
    country_col_by_keyword = None

    for col in df.columns:
        col_lower = str(col).lower()
        if vat_col_by_keyword is None and any(kw in col_lower for kw in vat_keywords):
            vat_col_by_keyword = col
        if country_col_by_keyword is None and any(kw in col_lower for kw in country_keywords):
            country_col_by_keyword = col

    # Sample up to 50 non-null values from each column to analyze
    def get_sample_values(column, max_samples=50):
        values = df[column].dropna().head(max_samples)
        return [str(v).strip().upper() for v in values if str(v).strip()]

    def looks_like_country_code(value):
        """Check if value looks like a standalone EU country code"""
        clean = re.sub(r'[^A-Z]', '', value)
        return len(clean) == 2 and clean in eu_country_codes

    def looks_like_combined_vat(value):
        """Check if value looks like a combined VAT number (country code + number)"""
        clean = re.sub(r'[^A-Z0-9]', '', value)
        if len(clean) < 4:
            return False
        prefix = clean[:2]
        return prefix.isalpha() and prefix in eu_country_codes

    # Strategy 1: Check if there's a column with primarily country codes (indicates Separate format)
    best_country_col = None
    best_country_score = 0

    for col in df.columns:
        samples = get_sample_values(col)
        if not samples:
            continue
        country_matches = sum(1 for v in samples if looks_like_country_code(v))
        score = country_matches / len(samples) if samples else 0
        if score > best_country_score and score >= 0.5:  # At least 50% match
            best_country_score = score
            best_country_col = col

    # Strategy 2: Check if VAT column (by keyword) has combined format
    combined_score = 0
    vat_col_to_check = vat_col_by_keyword or df.columns[0]

    samples = get_sample_values(vat_col_to_check)
    if samples:
        combined_matches = sum(1 for v in samples if looks_like_combined_vat(v))
        combined_score = combined_matches / len(samples)

    # Decision logic
    if best_country_col and best_country_score >= 0.5:
        # Found a dedicated country code column - likely Separate format
        return (
            "Separate Columns (Country + Number)",
            vat_col_by_keyword or df.columns[0],
            best_country_col
        )
    elif combined_score >= 0.5:
        # VAT values contain country codes - Combined format
        return (
            "Combined (e.g., DK12345678)",
            vat_col_to_check,
            None
        )
    else:
        # Fallback: check all columns for combined VAT pattern
        for col in df.columns:
            samples = get_sample_values(col)
            if samples:
                matches = sum(1 for v in samples if looks_like_combined_vat(v))
                if matches / len(samples) >= 0.5:
                    return (
                        "Combined (e.g., DK12345678)",
                        col,
                        None
                    )

        # Default to Combined if we can't determine
        return (
            "Combined (e.g., DK12345678)",
            vat_col_by_keyword or df.columns[0],
            None
        )


# Shared session for connection pooling (reuses TCP/SSL connections)
_vies_session = requests.Session()
_vies_session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Content-Type': 'application/json',
})
# Connection pool sized to match worker count
_adapter = requests.adapters.HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
_vies_session.mount('https://', _adapter)


def _vies_request(url, headers, method="GET", payload=None, timeout=30):
    """
    Internal helper to make a single VIES API request.
    Uses shared session for connection pooling.

    Returns:
        tuple: (success, data_or_error, debug_info)
        - success: True if we got a valid response, False otherwise
        - data_or_error: Parsed JSON data or error message
        - debug_info: String with URL/payload for debugging
    """
    debug_info = f"Method: {method}, URL: {url}"
    if payload:
        debug_info += f", Payload: {payload}"

    try:
        if method == "POST":
            response = _vies_session.post(url, json=payload, timeout=timeout)
        else:
            response = _vies_session.get(url, timeout=timeout)

        if response.status_code != 200:
            return False, f"HTTP {response.status_code}", debug_info

        try:
            data = response.json()
            return True, data, debug_info
        except ValueError:
            return False, "Invalid JSON response", debug_info

    except requests.exceptions.Timeout:
        return False, "Connection timeout", debug_info
    except requests.exceptions.ConnectionError:
        return False, "Connection error", debug_info
    except Exception as e:
        return False, f"Error: {str(e)[:50]}", debug_info


def validate_requester_vat(country, number):
    """
    Validates the requester's VAT number by making a quick GET request.

    Returns:
        tuple: (is_valid, company_name, error_message)
        - is_valid: True if VAT is valid, False otherwise
        - company_name: Company name if valid, None otherwise
        - error_message: Error message if invalid, None otherwise
    """
    if not country or not number:
        return False, None, "Country and number are required"

    # Clean the number
    clean_number = re.sub(r'[^A-Z0-9]', '', str(number).upper())

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
    }

    url = API_URL_GET.format(country, clean_number)

    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            return False, None, f"HTTP {response.status_code}"

        data = response.json()

        if data.get("isValid"):
            return True, data.get("name", "Unknown"), None
        else:
            user_error = data.get("userError", "INVALID")
            return False, None, f"VAT not found in VIES ({user_error})"

    except requests.exceptions.Timeout:
        return False, None, "Connection timeout"
    except requests.exceptions.ConnectionError:
        return False, None, "Connection error"
    except Exception as e:
        return False, None, f"Error: {str(e)[:50]}"


def check_vat(country, number, max_retries=None, requester_country=None, requester_number=None):
    """
    Queries EU VIES API for VAT validation using a two-step strategy:

    Step 1 (POST): If requester VAT is provided, try POST for legal Consultation ID
    Step 2 (GET):  Fallback to simple GET if POST fails or returns invalid

    Args:
        country: 2-letter country code
        number: VAT number without country code (alphanumeric only)
        max_retries: Override default MAX_RETRIES (used by circuit breaker)
        requester_country: Requester's 2-letter country code (for POST method)
        requester_number: Requester's VAT number without country code (for POST method)

    Returns:
        dict with keys:
            - valid: True (valid), False (invalid), or None (unknown/service error)
            - name: Company name from VIES
            - address: Company address from VIES
            - request_date: ISO date string
            - request_identifier: Consultation number (legal proof)
            - error_type: 'none', 'invalid', 'service_unavailable', 'format_error'
            - error_detail: Specific error message for debugging
            - debug_info: URL/Payload sent (for troubleshooting)
            - vat_number: The VAT number checked
    """
    if max_retries is None:
        max_retries = MAX_RETRIES

    # Clean requester_number as a safety measure (remove any remaining special chars)
    if requester_number:
        requester_number = re.sub(r'[^A-Z0-9]', '', str(requester_number).upper())

    headers = {}  # Session already has default headers

    # Collect debug info for troubleshooting
    all_debug_info = []

    # --- STEP 1: Try POST with requester info (for Consultation ID) ---
    # Only attempt POST if both requester_country and requester_number are provided
    if requester_country and requester_number:
        payload = {
            "countryCode": country,
            "vatNumber": number,
            "requesterMemberStateCode": requester_country,
            "requesterNumber": requester_number
        }

        for attempt in range(max_retries):
            time.sleep(RATE_LIMIT_SLEEP)  # Rate limiting

            success, data, debug_info = _vies_request(
                API_URL_POST, headers, method="POST", payload=payload
            )
            all_debug_info.append(f"POST attempt {attempt + 1}: {debug_info}")

            if not success:
                # Network/HTTP error - retry with exponential backoff
                if attempt < max_retries - 1:
                    time.sleep(RETRY_BASE_DELAY * (2 ** attempt))
                    continue
                break

            # Check for actionSucceed=false (error response format)
            if data.get("actionSucceed") is False:
                error_wrappers = data.get("errorWrappers", [])
                error_codes = [e.get("error", "") for e in error_wrappers]
                all_debug_info.append(f"POST error: {error_codes}")

                # If requester info is invalid, skip POST and fall back to GET
                if "INVALID_REQUESTER_INFO" in error_codes:
                    all_debug_info.append("Requester VAT invalid - falling back to GET")
                    break

                # For other errors, retry with exponential backoff
                if attempt < max_retries - 1:
                    time.sleep(RETRY_BASE_DELAY * (2 ** attempt))
                    continue
                break

            # Check for service unavailable errors (standard format)
            user_error = data.get("userError", "")
            if user_error in SERVICE_UNAVAILABLE_ERRORS:
                if attempt < max_retries - 1:
                    time.sleep(RETRY_BASE_DELAY * (2 ** attempt))
                    continue
                break

            # We got a definitive answer from POST
            # Note: POST uses "valid" field, not "isValid"
            is_valid = data.get("valid", data.get("isValid", False))

            if is_valid:
                # SUCCESS with POST - return with Consultation ID
                return {
                    "valid": True,
                    "name": data.get("name", "---"),
                    "address": data.get("address", "---"),
                    "request_date": data.get("requestDate", ""),
                    "request_identifier": data.get("requestIdentifier", ""),
                    "error_type": "none",
                    "error_detail": "",
                    "debug_info": " | ".join(all_debug_info),
                    "vat_number": data.get("vatNumber", number)
                }
            else:
                # POST says invalid - save result but continue to GET fallback
                post_result = {
                    "valid": False,
                    "data": data,
                    "user_error": user_error
                }
                break

    # --- STEP 2: Fallback to GET (simple anonymous check) ---
    get_url = API_URL_GET.format(country, number)

    for attempt in range(max_retries):
        time.sleep(RATE_LIMIT_SLEEP)  # Rate limiting

        success, data, debug_info = _vies_request(get_url, headers, method="GET")
        all_debug_info.append(f"GET attempt {attempt + 1}: {debug_info}")

        if not success:
            # Network/HTTP error - retry with exponential backoff
            if attempt < max_retries - 1:
                time.sleep(RETRY_BASE_DELAY * (2 ** attempt))
                continue
            # Max retries exceeded
            return {
                "valid": None,
                "name": "---",
                "address": "---",
                "request_date": "",
                "request_identifier": "",
                "error_type": "service_unavailable",
                "error_detail": f"{data} after {max_retries} attempts",
                "debug_info": " | ".join(all_debug_info),
                "vat_number": number
            }

        # Check for service unavailable errors
        user_error = data.get("userError", "")
        if user_error in SERVICE_UNAVAILABLE_ERRORS:
            if attempt < max_retries - 1:
                time.sleep(RETRY_BASE_DELAY * (2 ** attempt))
                continue
            return {
                "valid": None,
                "name": "---",
                "address": "---",
                "request_date": data.get("requestDate", ""),
                "request_identifier": data.get("requestIdentifier", ""),
                "error_type": "service_unavailable",
                "error_detail": f"{user_error} after {max_retries} attempts",
                "debug_info": " | ".join(all_debug_info),
                "vat_number": data.get("vatNumber", number)
            }

        # We got a definitive answer from GET
        is_valid = data.get("isValid", False)

        if is_valid:
            # VALID via GET (no Consultation ID usually)
            return {
                "valid": True,
                "name": data.get("name", "---"),
                "address": data.get("address", "---"),
                "request_date": data.get("requestDate", ""),
                "request_identifier": data.get("requestIdentifier", ""),
                "error_type": "none",
                "error_detail": "",
                "debug_info": " | ".join(all_debug_info),
                "vat_number": data.get("vatNumber", number)
            }
        else:
            # Both POST and GET say invalid - definitely invalid
            return {
                "valid": False,
                "name": data.get("name", "---"),
                "address": data.get("address", "---"),
                "request_date": data.get("requestDate", ""),
                "request_identifier": data.get("requestIdentifier", ""),
                "error_type": "invalid",
                "error_detail": user_error if user_error else "VAT number not found",
                "debug_info": " | ".join(all_debug_info),
                "vat_number": data.get("vatNumber", number)
            }

    # Fallback (should not reach here)
    return {
        "valid": None,
        "name": "---",
        "address": "---",
        "request_date": "",
        "request_identifier": "",
        "error_type": "service_unavailable",
        "error_detail": "Max retries exceeded",
        "debug_info": " | ".join(all_debug_info) if all_debug_info else "No requests made",
        "vat_number": number
    }


def format_datetime(iso_date):
    """Formats ISO date to readable date + time format in local timezone"""
    if not iso_date:
        # Use current local time if no date provided
        return datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    try:
        dt = datetime.fromisoformat(iso_date.replace('Z', '+00:00'))
        # Convert to local timezone
        local_dt = dt.astimezone()
        return local_dt.strftime("%d-%m-%Y %H:%M:%S")
    except:
        # Fallback to current time
        return datetime.now().strftime("%d-%m-%Y %H:%M:%S")


def format_time_remaining(seconds):
    """Formats seconds into human-readable time string (e.g., '2m 15s' or '45s')"""
    if seconds < 0:
        return "0s"

    seconds = int(seconds)

    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        minutes = seconds // 60
        secs = seconds % 60
        if secs > 0:
            return f"{minutes}m {secs}s"
        return f"{minutes}m"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        if minutes > 0:
            return f"{hours}h {minutes}m"
        return f"{hours}h"


def process_single_vat(index, raw_vat, customer_name=None, requester_country=None, requester_number=None):
    """Process a single VAT number and return result with index for ordering"""
    country, number = clean_vat_number(raw_vat)

    # Base result structure for fraud detection columns
    fraud_columns = {
        "Customer Name (Input)": str(customer_name) if customer_name else "---",
        "Name Match Score": "---",
        "Identity Risk": "---"
    }

    # Case 1: Could not extract country code at all
    if not country or not number:
        return {
            "index": index,
            "result": {
                "No.": index + 1,
                "Name from Output (VIES)": "---",
                "Address from Output (VIES)": "---",
                "Country": "---",
                "VAT Registration No.": str(raw_vat) if raw_vat else "---",
                "VIES Validation Status": "Invalid Format",
                "Validation Result": "Unknown",
                "Validation Date & Time": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "Correct Format": "Must start with 2-letter country code (e.g., DK, DE, FR)",
                "Consultation ID": "N/A",
                **fraud_columns
            }
        }

    # Case 2: Country code not recognized as EU member state
    if country not in EU_VAT_FORMATS:
        return {
            "index": index,
            "result": {
                "No.": index + 1,
                "Name from Output (VIES)": "---",
                "Address from Output (VIES)": "---",
                "Country": country,
                "VAT Registration No.": number,
                "VIES Validation Status": "Invalid Format",
                "Validation Result": "Unknown",
                "Validation Date & Time": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "Correct Format": f"'{country}' is not a valid EU country code",
                "Consultation ID": "N/A",
                **fraud_columns
            }
        }

    # Case 3: Check if VAT number matches expected format for the country
    format_valid, format_description = validate_vat_format(country, number)
    correct_format, format_desc = get_vat_format_info(country)

    if not format_valid:
        return {
            "index": index,
            "result": {
                "No.": index + 1,
                "Name from Output (VIES)": "---",
                "Address from Output (VIES)": "---",
                "Country": country,
                "VAT Registration No.": number,
                "VIES Validation Status": "Invalid Format",
                "Validation Result": "Unknown",
                "Validation Date & Time": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "Correct Format": f"{correct_format} ({format_desc})",
                "Consultation ID": "N/A",
                **fraud_columns
            }
        }

    # Case 4: Format is valid, check with VIES API
    # Circuit breaker: Check if this country has too many service errors
    with country_error_lock:
        country_errors = country_error_count.get(country, 0)

    # If country has 3+ errors, reduce retries to 1
    if country_errors >= CIRCUIT_BREAKER_THRESHOLD:
        response = check_vat(country, number, max_retries=1,
                            requester_country=requester_country, requester_number=requester_number)
    else:
        response = check_vat(country, number,
                            requester_country=requester_country, requester_number=requester_number)

    # Determine display status based on error_type and valid flag
    if response["error_type"] == "none" and response["valid"] is True:
        status = "Valid"
        result = "Valid"

        # Fraud detection: Calculate name similarity for valid VAT numbers
        if customer_name and customer_name != "---" and response["name"] != "---":
            similarity_score = calculate_name_similarity(customer_name, response["name"])
            identity_risk = get_identity_risk(similarity_score)
            fraud_columns = {
                "Customer Name (Input)": str(customer_name),
                "Name Match Score": similarity_score,
                "Identity Risk": identity_risk
            }

    elif response["error_type"] == "invalid" and response["valid"] is False:
        status = "Invalid"
        result = "Invalid"
    elif response["error_type"] == "service_unavailable":
        status = "Service Unavailable"
        result = "Unknown"
        # Circuit breaker: Increment error count for this country
        with country_error_lock:
            country_error_count[country] = country_error_count.get(country, 0) + 1
    else:
        status = "Unknown"
        result = "Unknown"

    # Build result dictionary
    result_dict = {
        "No.": index + 1,
        "Name from Output (VIES)": response["name"],
        "Address from Output (VIES)": response["address"],
        "Country": country,
        "VAT Registration No.": response["vat_number"],
        "VIES Validation Status": status,
        "Validation Result": result,
        "Validation Date & Time": format_datetime(response["request_date"]),
        "Correct Format": "---",
        "Consultation ID": response["request_identifier"] if response["request_identifier"] else "N/A",
    }

    # Add fraud detection columns
    result_dict.update(fraud_columns)

    return {
        "index": index,
        "result": result_dict
    }


# --- HELPER: STEP INDICATOR ---
def render_step_bar(active_step):
    """Renders a step progress bar. Steps: 1=Upload, 2=Configure, 3=Validate, 4=Results"""
    steps = [
        ("1. Upload", "Upload your file"),
        ("2. Configure", "Map columns"),
        ("3. Validate", "Check VAT numbers"),
        ("4. Results", "Download results"),
    ]
    html = '<div class="step-bar">'
    for i, (label, _) in enumerate(steps, 1):
        if i < active_step:
            cls = "step-item step-done"
            icon = "&#10003; "
        elif i == active_step:
            cls = "step-item step-active"
            icon = ""
        else:
            cls = "step-item"
            icon = ""
        html += f'<div class="{cls}">{icon}{label}</div>'
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)

# --- WEBSITE ---

# Header
st.markdown("""
<div class="app-header">
    <h1>EU VAT Bulk Checker</h1>
    <p>Validate EU VAT numbers against the official VIES database &mdash; with fraud detection and legal documentation</p>
</div>
""", unsafe_allow_html=True)

# Sidebar for settings
with st.sidebar:
    st.markdown('<div class="sidebar-title">Settings</div>', unsafe_allow_html=True)

    # --- Legal Documentation Section ---
    with st.expander("Legal Documentation", expanded=True):
        st.caption("Enter your VAT to receive a legally valid Consultation ID as proof of verification. Press Enter to confirm.")

        EU_COUNTRY_CODES = [
            "", "AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "EL", "ES",
            "FI", "FR", "HR", "HU", "IE", "IT", "LT", "LU", "LV", "MT",
            "NL", "PL", "PT", "RO", "SE", "SI", "SK", "XI"
        ]

        col_cc, col_vn = st.columns([2, 3])
        with col_cc:
            requester_country = st.selectbox(
                "Country",
                options=EU_COUNTRY_CODES,
                index=0,
                help="Select your country code"
            )
        with col_vn:
            if 'confirmed_requester_vat' not in st.session_state:
                st.session_state.confirmed_requester_vat = ""

            requester_number_input = st.text_input(
                "VAT Number",
                value="",
                placeholder="e.g. 12345678",
                help="Number only, without country code",
                key="requester_vat_input"
            )

        if requester_number_input and requester_number_input.strip():
            st.session_state.confirmed_requester_vat = requester_number_input.strip()
            st.markdown(
                f'<span class="badge badge-success">Confirmed: {requester_country}{requester_number_input.strip()}</span>',
                unsafe_allow_html=True
            )
        elif st.session_state.confirmed_requester_vat:
            st.session_state.confirmed_requester_vat = ""

    # --- Fraud Detection Section ---
    with st.expander("Fraud Detection", expanded=False):
        st.caption("Compare customer names from your file against official VIES records.")
        enable_fraud_detection = st.checkbox("Enable Name Verification", value=False)

        if enable_fraud_detection:
            st.markdown("""
| Score | Risk Level |
|-------|-----------|
| > 60% | Verified |
| 20-60% | Check Manually |
| < 20% | POTENTIAL FRAUD |
""")

    # --- About Section ---
    with st.expander("About", expanded=False):
        st.caption("Validates EU VAT numbers via the official VIES API from the European Commission. Supports all 27 EU member states + Northern Ireland.")
        st.caption(f"Concurrent workers: {MAX_WORKERS} | Retries: {MAX_RETRIES}")

# --- Determine current step for step bar ---
has_results = 'validation_results' in st.session_state and st.session_state['validation_results']
current_step = 4 if has_results else 1
# (will be updated as we go)

uploaded_file = st.file_uploader(
    "Upload your Excel or CSV file with VAT numbers",
    type=["xlsx", "xls", "xlsm", "xlsb", "ods", "csv"],
    help="Supported: .xlsx, .xls, .xlsm, .xlsb, .ods, .csv"
)

if not uploaded_file and not has_results:
    render_step_bar(1)
    st.markdown("""
<div class="info-box info-box-blue">
    <strong>How to use:</strong><br>
    1. Upload a file containing VAT numbers (with country codes like DK, DE, FR)<br>
    2. Configure which columns contain the data<br>
    3. Click <em>Start Validation</em> to check all numbers against VIES<br>
    4. Download the results as CSV or Excel
</div>
""", unsafe_allow_html=True)
    st.stop()

if uploaded_file:
    file_name = uploaded_file.name.lower()

    # Clear previous results when a new file is uploaded
    if 'current_file' not in st.session_state or st.session_state['current_file'] != file_name:
        st.session_state['current_file'] = file_name
        if 'validation_results' in st.session_state:
            del st.session_state['validation_results']
        if 'duplicate_data' in st.session_state:
            del st.session_state['duplicate_data']
        if 'selected_sheet' in st.session_state:
            del st.session_state['selected_sheet']

    # --- Detect sheets and read data ---
    sheet_names = None
    selected_sheet = 0  # default: first sheet
    xls = None

    try:
        if file_name.endswith('.csv'):
            sheet_names = None
        elif file_name.endswith('.xlsb'):
            xls = pd.ExcelFile(uploaded_file, engine='pyxlsb')
            sheet_names = xls.sheet_names
        elif file_name.endswith('.ods'):
            xls = pd.ExcelFile(uploaded_file, engine='odf')
            sheet_names = xls.sheet_names
        else:
            xls = pd.ExcelFile(uploaded_file)
            sheet_names = xls.sheet_names
    except ImportError as e:
        if 'pyxlsb' in str(e):
            st.error("To read .xlsb files, please install pyxlsb: `pip install pyxlsb`")
        elif 'odfpy' in str(e):
            st.error("To read .ods files, please install odfpy: `pip install odfpy`")
        else:
            st.error(f"Missing library: {e}")
        st.stop()
    except Exception as e:
        st.error(f"Could not read file. It may be corrupted or password-protected. ({type(e).__name__}: {str(e)[:100]})")
        st.stop()

    # Show sheet selector if multiple sheets exist
    if sheet_names and len(sheet_names) > 1:
        selected_sheet = st.selectbox(
            "Select sheet",
            options=list(range(len(sheet_names))),
            format_func=lambda i: sheet_names[i],
            index=0,
            help=f"This file has {len(sheet_names)} sheets"
        )

    # Read the data from the selected sheet (reuse ExcelFile to avoid double-read)
    try:
        if file_name.endswith('.csv'):
            try:
                df = pd.read_csv(uploaded_file)
            except UnicodeDecodeError:
                uploaded_file.seek(0)
                df = pd.read_csv(uploaded_file, encoding='latin-1')
        elif xls is not None:
            df = xls.parse(sheet_name=selected_sheet)
        else:
            df = pd.read_excel(uploaded_file, sheet_name=selected_sheet)
    except UnicodeDecodeError:
        st.error("Could not read file: encoding not supported. Try saving the file as UTF-8 CSV.")
        st.stop()
    except Exception as e:
        st.error(f"Error reading data: {type(e).__name__}: {str(e)[:100]}")
        st.stop()

    # Guard against empty files
    if df.empty:
        st.warning("This file (or sheet) contains no data rows. Please upload a file with data.")
        st.stop()

    has_results = 'validation_results' in st.session_state and st.session_state['validation_results']
    if not has_results:
        render_step_bar(2)

    # File summary
    sheet_info = ""
    if sheet_names and len(sheet_names) > 1:
        sheet_info = f' &mdash; Sheet: <strong>{sheet_names[selected_sheet]}</strong>'
    st.markdown(f"""
<div class="info-box">
    <strong>{uploaded_file.name}</strong> &mdash; {len(df):,} rows, {len(df.columns)} columns{sheet_info}
</div>
""", unsafe_allow_html=True)

    with st.expander("Data Preview", expanded=False):
        st.dataframe(df.head(10), use_container_width=True)

    # --- COLUMN FORMAT SELECTION ---
    if not has_results:
        st.markdown('<div class="section-header">Column Mapping</div>', unsafe_allow_html=True)

    vat_keywords = ["vat no.", "vat no", "vat", "momsnummer", "moms", "tax code", "taxcode", "cvr", "nummer"]
    country_keywords = ["country code", "country", "landekode", "land"]

    detected_format, detected_vat_col, detected_country_col = detect_data_format(
        df, vat_keywords, country_keywords
    )

    format_options = ["Combined (e.g., DK12345678)", "Separate Columns (Country + Number)"]
    default_index = format_options.index(detected_format) if detected_format in format_options else 0

    data_format = st.radio(
        "VAT data format",
        options=format_options,
        index=default_index,
        horizontal=True,
        help="Auto-detected from your data. Change if incorrect."
    )

    # Show auto-detect badge
    st.markdown(f'<span class="badge badge-info">Auto-detected: {detected_format.split("(")[0].strip()}</span>', unsafe_allow_html=True)

    default_vat_col = detected_vat_col
    if default_vat_col is None:
        for col in df.columns:
            if any(keyword in str(col).lower() for keyword in vat_keywords):
                default_vat_col = col
                break
    if default_vat_col is None:
        default_vat_col = df.columns[0]

    default_country_col = detected_country_col
    if default_country_col is None:
        for col in df.columns:
            if any(keyword in str(col).lower() for keyword in country_keywords):
                default_country_col = col
                break

    if data_format == "Combined (e.g., DK12345678)":
        vat_column = st.selectbox(
            "VAT Column (with country code)",
            options=df.columns.tolist(),
            index=df.columns.tolist().index(default_vat_col) if default_vat_col in df.columns.tolist() else 0
        )
        country_column = None
        number_column = None
    else:
        col1, col2 = st.columns(2)
        with col1:
            country_column = st.selectbox(
                "Country Code Column",
                options=df.columns.tolist(),
                index=df.columns.tolist().index(default_country_col) if default_country_col and default_country_col in df.columns.tolist() else 0
            )
        with col2:
            number_column = st.selectbox(
                "VAT Number Column",
                options=df.columns.tolist(),
                index=df.columns.tolist().index(default_vat_col) if default_vat_col in df.columns.tolist() else 0
            )
        vat_column = None

    # Column selector for customer name (for fraud detection)
    name_column = None
    if enable_fraud_detection:
        st.markdown('<div class="section-header">Fraud Detection</div>', unsafe_allow_html=True)
        name_keywords = ["name", "company", "firma", "kunde", "customer", "navn"]
        default_name_col = None
        for col in df.columns:
            if any(keyword in str(col).lower() for keyword in name_keywords):
                default_name_col = col
                break

        name_column = st.selectbox(
            "Customer/Company Name Column",
            options=df.columns.tolist(),
            index=df.columns.tolist().index(default_name_col) if default_name_col else 0
        )

    # --- CHECKPOINT/RESUME DETECTION ---
    checkpoint_exists = has_checkpoint()
    resume_mode = False

    if checkpoint_exists:
        st.markdown("""
<div class="info-box" style="border-left: 4px solid #fd7e14;">
    <strong>Previous validation interrupted</strong> &mdash; You can resume where you left off or start fresh.
</div>
""", unsafe_allow_html=True)
        col_resume, col_restart, _ = st.columns([1, 1, 2])
        with col_resume:
            if st.button("Resume", type="primary", use_container_width=True):
                resume_mode = True
                st.session_state['resume_requested'] = True
        with col_restart:
            if st.button("Start Fresh", use_container_width=True):
                cleanup_checkpoint()
                st.session_state['resume_requested'] = False
                st.rerun()

    if st.session_state.get('resume_requested', False):
        resume_mode = True

    st.markdown("")  # spacer
    if st.button("Start Validation", type="primary", use_container_width=True) or resume_mode:

        # Clean and validate requester VAT for legal documentation (Consultation ID)
        # requester_country comes directly from the selectbox
        # requester_number_input needs to be cleaned (remove spaces, dots, dashes)
        cleaned_requester_number = None
        requester_valid = False

        if requester_country and requester_number_input and requester_number_input.strip():
            # Clean the number: remove all non-alphanumeric characters
            cleaned_requester_number = re.sub(r'[^A-Z0-9]', '', requester_number_input.upper())

            if cleaned_requester_number:
                # Validate the requester VAT with VIES before proceeding
                with st.spinner(f"Validating your VAT number ({requester_country}{cleaned_requester_number})..."):
                    is_valid, company_name, error_msg = validate_requester_vat(requester_country, cleaned_requester_number)

                if is_valid:
                    st.success(f"Your VAT verified: {requester_country}{cleaned_requester_number} ({company_name}). Consultation IDs will be issued.")
                    requester_valid = True
                else:
                    st.error(f"Your VAT ({requester_country}{cleaned_requester_number}) is NOT valid: {error_msg}")
                    st.warning("Proceeding without legal documentation. Consultation ID will be N/A.")
                    cleaned_requester_number = None  # Don't use invalid requester
            else:
                st.warning("Invalid requester VAT number format. Proceeding without legal documentation.")
        else:
            st.info("No requester VAT provided. Consultation ID will be N/A (anonymous validation).")

        # Reset circuit breaker counters for new validation run
        country_error_count.clear()

        render_step_bar(3)

        turbo_mode_placeholder = st.empty()
        turbo_mode_placeholder.markdown(f"""
<div class="info-box info-box-blue">
    Validating with <strong>{MAX_WORKERS} concurrent workers</strong> for faster processing.
</div>
""", unsafe_allow_html=True)

        progress_bar_placeholder = st.empty()
        progress_bar = progress_bar_placeholder.progress(0)
        status_text = st.empty()

        # --- CONSTRUCT VAT NUMBERS BASED ON FORMAT ---
        # Apply robust normalization to handle Excel data types (floats, integers, NaN)
        normalized_data = []  # List of (normalized_vat, customer_name)

        for idx, row in df.iterrows():
            if data_format == "Combined (e.g., DK12345678)":
                # Use the selected combined column directly
                raw_value = row[vat_column]
                normalized, _ = normalize_vat_input(raw_value)
            else:
                # Normalize both country and number columns separately, then combine
                raw_country = row[country_column]
                raw_number = row[number_column]

                country_normalized, _ = normalize_vat_input(raw_country)
                number_normalized, _ = normalize_vat_input(raw_number)

                # Handle cases where either is None
                if country_normalized is None and number_normalized is None:
                    normalized = None
                elif country_normalized is None:
                    normalized = number_normalized
                elif number_normalized is None:
                    normalized = country_normalized
                else:
                    normalized = country_normalized.strip() + number_normalized.strip()

            # Get customer name if fraud detection is enabled
            customer_name = None
            if enable_fraud_detection and name_column:
                raw_name = row[name_column]
                name_normalized, _ = normalize_vat_input(raw_name)
                customer_name = name_normalized

            # Skip None values (NaN/empty cells)
            if normalized is not None:
                normalized_data.append((normalized, customer_name))

        # Remove duplicates while tracking them for download (keep first occurrence)
        original_count = len(normalized_data)
        seen_vats = {}
        unique_data = []
        duplicate_data = []  # Track duplicates for download
        for vat, name in normalized_data:
            if vat not in seen_vats:
                seen_vats[vat] = True
                unique_data.append((vat, name))
            else:
                # This is a duplicate - track it
                duplicate_data.append({"VAT Number": vat, "Customer Name": name if name else "---"})

        unique_count = len(unique_data)
        duplicates_removed = original_count - unique_count
        skipped_count = len(df) - original_count

        if skipped_count > 0:
            st.info(f"Skipped {skipped_count} empty/invalid rows.")

        if duplicates_removed > 0:
            st.warning(f"Removed {duplicates_removed} duplicate VAT numbers. Processing {unique_count} unique numbers.")

        total = unique_count

        if total == 0:
            st.warning("No valid VAT numbers found to process. Check that your column mapping is correct.")
            st.stop()

        # Prepare list of tasks: (index, vat_number, customer_name)
        all_tasks = [(i, vat, name) for i, (vat, name) in enumerate(unique_data)]

        # --- RESUME LOGIC: Load checkpoint if resuming ---
        results_dict = {}
        completed_indices = set()

        if resume_mode:
            cached_results, cached_indices, cached_total = load_checkpoint()
            if cached_results and cached_indices:
                results_dict = cached_results
                completed_indices = cached_indices
                st.info(f"Resuming from checkpoint: {len(completed_indices)}/{total} already completed.")

        # Filter out already-completed tasks
        tasks = [(idx, vat, name) for idx, vat, name in all_tasks if idx not in completed_indices]
        completed_count = len(completed_indices)

        # Checkpoint saving interval (save every N completions)
        CHECKPOINT_INTERVAL = 10
        last_checkpoint_count = completed_count

        # Start timer for time estimation
        start_time = time.time()

        # Process with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_index = {
                executor.submit(process_single_vat, idx, vat, name, requester_country, cleaned_requester_number): idx
                for idx, vat, name in tasks
            }

            for future in as_completed(future_to_index):
                try:
                    result_data = future.result()
                    index = result_data["index"]

                    with results_lock:
                        results_dict[index] = result_data["result"]
                        completed_indices.add(index)
                        completed_count += 1

                    # Update progress bar
                    progress_bar.progress(completed_count / total)

                    # --- CHECKPOINT SAVING ---
                    # Save checkpoint every CHECKPOINT_INTERVAL completions
                    if completed_count - last_checkpoint_count >= CHECKPOINT_INTERVAL:
                        save_checkpoint(results_dict, total, completed_indices)
                        last_checkpoint_count = completed_count

                    # Calculate time estimation
                    elapsed_time = time.time() - start_time
                    if completed_count > len(completed_indices - {index}):  # At least 1 new completion
                        # Calculate based on newly processed items only
                        newly_processed = completed_count - (len(completed_indices) - len(tasks))
                        if newly_processed > 0:
                            avg_time_per_item = elapsed_time / newly_processed
                            items_remaining = total - completed_count
                            estimated_seconds_left = avg_time_per_item * items_remaining
                            time_remaining_str = format_time_remaining(estimated_seconds_left)

                            if items_remaining > 0:
                                status_text.text(f"Processing {completed_count}/{total} (Est. time remaining: {time_remaining_str})")
                            else:
                                status_text.text(f"Processing {completed_count}/{total}...")
                        else:
                            status_text.text(f"Processing {completed_count}/{total}...")
                    else:
                        status_text.text(f"Processing {completed_count}/{total}...")

                except Exception as e:
                    idx = future_to_index[future]
                    with results_lock:
                        results_dict[idx] = {
                            "No.": idx + 1,
                            "Name from Output (VIES)": "---",
                            "Address from Output (VIES)": "---",
                            "Country": "---",
                            "VAT Registration No.": "---",
                            "VIES Validation Status": "Error",
                            "Validation Result": "Unknown",
                            "Validation Date & Time": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                            "Correct Format": "---",
                            "Consultation ID": "N/A",
                            "Customer Name (Input)": "---",
                            "Name Match Score": "---",
                            "Identity Risk": "---"
                        }
                        completed_indices.add(idx)
                        completed_count += 1
                    progress_bar.progress(completed_count / total)

                    # Save checkpoint on error too
                    if completed_count - last_checkpoint_count >= CHECKPOINT_INTERVAL:
                        save_checkpoint(results_dict, total, completed_indices)
                        last_checkpoint_count = completed_count

        # Sort results by original index
        results = [results_dict[i] for i in sorted(results_dict.keys())]

        # Calculate total time taken
        total_time = time.time() - start_time
        total_time_str = format_time_remaining(total_time)

        # Clear the turbo mode notification and progress bar
        turbo_mode_placeholder.empty()
        progress_bar_placeholder.empty()

        # --- CLEANUP: Delete checkpoint file after successful completion ---
        cleanup_checkpoint()
        if 'resume_requested' in st.session_state:
            del st.session_state['resume_requested']

        # Store results in session state so they persist after download
        st.session_state['validation_results'] = results
        st.session_state['duplicate_data'] = duplicate_data
        st.session_state['total_time_str'] = total_time_str
        st.session_state['total_count'] = total
        st.session_state['fraud_detection_enabled'] = enable_fraud_detection

    # Display results from session state (persists after download clicks)
    if 'validation_results' in st.session_state and st.session_state['validation_results']:
        results = st.session_state['validation_results']
        duplicate_data = st.session_state.get('duplicate_data', [])
        total_time_str = st.session_state.get('total_time_str', '')
        total = st.session_state.get('total_count', len(results))
        fraud_detection_enabled = st.session_state.get('fraud_detection_enabled', False)

        render_step_bar(4)

        # Calculate summary statistics
        valid_count = sum(1 for r in results if r["VIES Validation Status"] == "Valid")
        invalid_count = sum(1 for r in results if r["VIES Validation Status"] == "Invalid")
        invalid_format_count = sum(1 for r in results if r["VIES Validation Status"] == "Invalid Format")
        service_error_count = sum(1 for r in results if r["VIES Validation Status"] == "Service Unavailable")
        other_error_count = total - valid_count - invalid_count - invalid_format_count - service_error_count

        # Completion summary
        st.markdown(f"""
<div class="summary-box">
    <h4>Validation Complete</h4>
    Processed <strong>{total:,}</strong> VAT numbers in <strong>{total_time_str}</strong>
</div>
""", unsafe_allow_html=True)

        # Custom metric cards (color-coded)
        st.markdown(f"""
<div class="metric-row">
    <div class="metric-card metric-valid">
        <div class="metric-value">{valid_count}</div>
        <div class="metric-label">Valid</div>
    </div>
    <div class="metric-card metric-invalid">
        <div class="metric-value">{invalid_count}</div>
        <div class="metric-label">Invalid</div>
    </div>
    <div class="metric-card metric-format">
        <div class="metric-value">{invalid_format_count}</div>
        <div class="metric-label">Invalid Format</div>
    </div>
    <div class="metric-card metric-service">
        <div class="metric-value">{service_error_count}</div>
        <div class="metric-label">Service Error</div>
    </div>
    <div class="metric-card metric-other">
        <div class="metric-value">{other_error_count}</div>
        <div class="metric-label">Other</div>
    </div>
</div>
""", unsafe_allow_html=True)

        # Fraud detection summary (if enabled)
        if fraud_detection_enabled:
            fraud_results = [r for r in results if r["Identity Risk"] != "---"]
            if fraud_results:
                verified_count = sum(1 for r in fraud_results if r["Identity Risk"] == "Verified")
                check_count = sum(1 for r in fraud_results if r["Identity Risk"] == "Check Manually")
                fraud_count = sum(1 for r in fraud_results if r["Identity Risk"] == "POTENTIAL FRAUD")

                st.markdown('<div class="section-header">Fraud Detection</div>', unsafe_allow_html=True)
                st.markdown(f"""
<div class="metric-row">
    <div class="metric-card metric-verified">
        <div class="metric-value">{verified_count}</div>
        <div class="metric-label">Verified</div>
    </div>
    <div class="metric-card metric-check">
        <div class="metric-value">{check_count}</div>
        <div class="metric-label">Check Manually</div>
    </div>
    <div class="metric-card metric-fraud">
        <div class="metric-value">{fraud_count}</div>
        <div class="metric-label">Potential Fraud</div>
    </div>
</div>
""", unsafe_allow_html=True)

        result_df = pd.DataFrame(results)

        # Ensure Consultation ID is treated as string (prevents truncation of long numbers)
        if "Consultation ID" in result_df.columns:
            result_df["Consultation ID"] = result_df["Consultation ID"].astype(str)

        # Define explicit column order
        base_columns = [
            "No.",
            "Name from Output (VIES)",
            "Address from Output (VIES)",
            "Country",
            "VAT Registration No.",
            "VIES Validation Status",
            "Validation Result",
            "Validation Date & Time",
            "Correct Format",
            "Consultation ID"
        ]
        fraud_columns_list = ["Customer Name (Input)", "Name Match Score", "Identity Risk"]

        final_columns = [col for col in base_columns if col in result_df.columns]
        if fraud_detection_enabled:
            final_columns.extend([col for col in fraud_columns_list if col in result_df.columns])
        result_df = result_df[final_columns]

        if not fraud_detection_enabled:
            result_df = result_df.drop(columns=["Customer Name (Input)", "Name Match Score", "Identity Risk"], errors='ignore')

        # Style the validation status and identity risk columns
        def highlight_status(val):
            if val == "Valid":
                return "color: #155724; font-weight: bold; background-color: #d4edda"
            elif val == "Invalid":
                return "color: #721c24; font-weight: bold; background-color: #f8d7da"
            elif val == "Invalid Format":
                return "color: #4a1a7a; font-weight: bold; background-color: #e8d5f5"
            elif val == "Service Unavailable":
                return "color: #856404; font-weight: bold; background-color: #fff3cd"
            elif val in ["Unknown", "Error"]:
                return "color: #6c757d; font-weight: bold; background-color: #f0f0f0"
            return ""

        def highlight_risk(val):
            if val == "Verified":
                return "color: #155724; font-weight: bold; background-color: #d4edda"
            elif val == "Check Manually":
                return "color: #856404; font-weight: bold; background-color: #fff3cd"
            elif val == "POTENTIAL FRAUD":
                return "color: #721c24; font-weight: bold; background-color: #f8d7da"
            return ""

        st.markdown('<div class="section-header">Detailed Results</div>', unsafe_allow_html=True)

        if fraud_detection_enabled and "Identity Risk" in result_df.columns:
            styled_df = result_df.style.map(highlight_status, subset=["VIES Validation Status"]).map(highlight_risk, subset=["Identity Risk"])
        else:
            styled_df = result_df.style.map(highlight_status, subset=["VIES Validation Status"])

        st.dataframe(styled_df, use_container_width=True, height=500)

        # Download section
        st.markdown('<div class="section-header">Export Results</div>', unsafe_allow_html=True)

        # Prepare file data upfront
        csv = result_df.to_csv(index=False).encode('utf-8')
        temp_excel_file = os.path.join(CACHE_DIR, f"temp_{st.session_state['session_id']}.xlsx")
        ensure_cache_dir()
        excel_buffer = pd.ExcelWriter(temp_excel_file, engine='openpyxl')
        result_df.to_excel(excel_buffer, index=False)
        excel_buffer.close()
        with open(temp_excel_file, "rb") as f:
            excel_data = f.read()
        try:
            os.remove(temp_excel_file)
        except Exception:
            pass

        with st.container(border=True):
            dl_col1, dl_col2 = st.columns(2)
            with dl_col1:
                st.download_button(
                    "Download CSV",
                    csv,
                    "vat_results.csv",
                    "text/csv",
                    key="download_csv",
                    use_container_width=True
                )
            with dl_col2:
                st.download_button(
                    "Download Excel",
                    excel_data,
                    "vat_results.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_excel",
                    use_container_width=True
                )

            if duplicate_data:
                duplicates_df = pd.DataFrame(duplicate_data)
                duplicates_csv = duplicates_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    f"Download Duplicates ({len(duplicate_data)})",
                    duplicates_csv,
                    "vat_duplicates.csv",
                    "text/csv",
                    key="download_duplicates",
                    use_container_width=True
                )

            st.divider()
            if st.button("Start Over", key="start_over", use_container_width=True):
                cleanup_checkpoint()
                for key in ['validation_results', 'duplicate_data', 'total_time_str',
                           'total_count', 'fraud_detection_enabled', 'resume_requested', 'current_file']:
                    if key in st.session_state:
                        del st.session_state[key]
                st.rerun()
