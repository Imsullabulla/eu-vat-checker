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
st.set_page_config(page_title="EU VAT Checker", layout="wide")

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

API_URL = "https://ec.europa.eu/taxation_customs/vies/rest-api/ms/{}/vat/{}"
MAX_WORKERS = 5  # Number of concurrent threads
MAX_RETRIES = 3  # Number of retry attempts for failed requests
RETRY_DELAY = 2  # Seconds to wait between retries

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
    - > 80: High confidence match (Verified)
    - 20-80: Partial match, needs manual review (Check Manually)
    - < 20: Little to no resemblance (POTENTIAL FRAUD)
    """
    if score > 80:
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
    """Removes spaces and extracts country code + number"""
    clean_text = re.sub(r'[^A-Z0-9]', '', str(text).upper())

    if len(clean_text) < 3:
        return None, None

    country = clean_text[:2]
    number = clean_text[2:]
    return country, number


def check_vat(country, number, max_retries=None):
    """
    Queries EU VIES API for VAT validation with smart retry logic.

    Args:
        country: 2-letter country code
        number: VAT number without country code
        max_retries: Override default MAX_RETRIES (used by circuit breaker)

    Returns:
        dict with keys:
            - valid: True (valid), False (invalid), or None (unknown/service error)
            - name: Company name from VIES
            - address: Company address from VIES
            - request_date: ISO date string
            - request_identifier: Consultation number
            - error_type: 'none', 'invalid', 'service_unavailable', 'format_error'
            - error_detail: Specific error message for debugging
            - vat_number: The VAT number checked
    """
    if max_retries is None:
        max_retries = MAX_RETRIES

    full_url = API_URL.format(country, number)

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    last_error = None

    for attempt in range(max_retries):
        try:
            # Rate limiting - polite delay per thread
            time.sleep(0.5)

            response = requests.get(full_url, headers=headers, timeout=30)

            # --- Handle HTTP error codes ---
            if response.status_code in RETRY_HTTP_CODES:
                last_error = f"HTTP_{response.status_code}"
                if attempt < max_retries - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                # Max retries exceeded - return as service unavailable
                return {
                    "valid": None,  # Unknown - NOT False!
                    "name": "---",
                    "address": "---",
                    "request_date": "",
                    "request_identifier": "",
                    "error_type": "service_unavailable",
                    "error_detail": f"HTTP {response.status_code} after {max_retries} attempts",
                    "vat_number": number
                }

            # --- Handle non-200 responses ---
            if response.status_code != 200:
                return {
                    "valid": None,
                    "name": "---",
                    "address": "---",
                    "request_date": "",
                    "request_identifier": "",
                    "error_type": "service_unavailable",
                    "error_detail": f"Unexpected HTTP {response.status_code}",
                    "vat_number": number
                }

            # --- Parse JSON response ---
            try:
                data = response.json()
            except ValueError:
                return {
                    "valid": None,
                    "name": "---",
                    "address": "---",
                    "request_date": "",
                    "request_identifier": "",
                    "error_type": "service_unavailable",
                    "error_detail": "Invalid JSON response",
                    "vat_number": number
                }

            # --- Check for JSON-level errors (HTTP 200 but error in body) ---
            user_error = data.get("userError", "")

            # Check if this is a service unavailable error
            if user_error in SERVICE_UNAVAILABLE_ERRORS:
                last_error = user_error
                if attempt < max_retries - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                # Max retries exceeded
                return {
                    "valid": None,  # Unknown - NOT False!
                    "name": "---",
                    "address": "---",
                    "request_date": data.get("requestDate", ""),
                    "request_identifier": data.get("requestIdentifier", ""),
                    "error_type": "service_unavailable",
                    "error_detail": f"{user_error} after {max_retries} attempts",
                    "vat_number": data.get("vatNumber", number)
                }

            # --- Check for explicit "error" field in JSON ---
            if "error" in data:
                error_msg = data.get("error", "Unknown error")
                if any(err in str(error_msg).upper() for err in SERVICE_UNAVAILABLE_ERRORS):
                    if attempt < max_retries - 1:
                        time.sleep(RETRY_DELAY)
                        continue
                    return {
                        "valid": None,
                        "name": "---",
                        "address": "---",
                        "request_date": "",
                        "request_identifier": "",
                        "error_type": "service_unavailable",
                        "error_detail": str(error_msg),
                        "vat_number": number
                    }

            # --- Success case: We got a definitive answer ---
            is_valid = data.get("isValid", False)

            if is_valid:
                # Definitively VALID
                return {
                    "valid": True,
                    "name": data.get("name", "---"),
                    "address": data.get("address", "---"),
                    "request_date": data.get("requestDate", ""),
                    "request_identifier": data.get("requestIdentifier", ""),
                    "error_type": "none",
                    "error_detail": "",
                    "vat_number": data.get("vatNumber", number)
                }
            else:
                # Definitively INVALID (userError should be "INVALID" or empty with isValid=false)
                return {
                    "valid": False,
                    "name": data.get("name", "---"),
                    "address": data.get("address", "---"),
                    "request_date": data.get("requestDate", ""),
                    "request_identifier": data.get("requestIdentifier", ""),
                    "error_type": "invalid",
                    "error_detail": user_error if user_error else "VAT number not found",
                    "vat_number": data.get("vatNumber", number)
                }

        except requests.exceptions.Timeout:
            last_error = "TIMEOUT"
            if attempt < max_retries - 1:
                time.sleep(RETRY_DELAY)
                continue
            return {
                "valid": None,
                "name": "---",
                "address": "---",
                "request_date": "",
                "request_identifier": "",
                "error_type": "service_unavailable",
                "error_detail": f"Connection timeout after {max_retries} attempts",
                "vat_number": number
            }

        except requests.exceptions.ConnectionError as e:
            last_error = "CONNECTION_ERROR"
            if attempt < max_retries - 1:
                time.sleep(RETRY_DELAY)
                continue
            return {
                "valid": None,
                "name": "---",
                "address": "---",
                "request_date": "",
                "request_identifier": "",
                "error_type": "service_unavailable",
                "error_detail": f"Connection error after {max_retries} attempts",
                "vat_number": number
            }

        except Exception as e:
            last_error = str(e)[:50]
            if attempt < max_retries - 1:
                time.sleep(RETRY_DELAY)
                continue
            return {
                "valid": None,
                "name": "---",
                "address": "---",
                "request_date": "",
                "request_identifier": "",
                "error_type": "service_unavailable",
                "error_detail": f"Error: {str(e)[:50]}",
                "vat_number": number
            }

    # Fallback (should not reach here)
    return {
        "valid": None,
        "name": "---",
        "address": "---",
        "request_date": "",
        "request_identifier": "",
        "error_type": "service_unavailable",
        "error_detail": f"Max retries exceeded: {last_error}",
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


def process_single_vat(index, raw_vat, customer_name=None):
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
                **fraud_columns
            }
        }

    # Case 4: Format is valid, check with VIES API
    # Circuit breaker: Check if this country has too many service errors
    with country_error_lock:
        country_errors = country_error_count.get(country, 0)

    # If country has 3+ errors, reduce retries to 1
    if country_errors >= CIRCUIT_BREAKER_THRESHOLD:
        response = check_vat(country, number, max_retries=1)
    else:
        response = check_vat(country, number)

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

    return {
        "index": index,
        "result": {
            "No.": index + 1,
            "Name from Output (VIES)": response["name"],
            "Address from Output (VIES)": response["address"],
            "Country": country,
            "VAT Registration No.": response["vat_number"],
            "VIES Validation Status": status,
            "Validation Result": result,
            "Validation Date & Time": format_datetime(response["request_date"]),
            "Correct Format": "---",
            **fraud_columns
        }
    }


# --- WEBSITE ---

st.title("EU VAT Bulk Checker")
st.markdown("Upload an Excel file with VAT numbers (include country code, e.g. DK12345678).")

# Sidebar for fraud detection settings
with st.sidebar:
    st.header("Fraud Detection")
    st.markdown("Compare customer names from your file with official VIES records.")
    enable_fraud_detection = st.checkbox("Enable Name Verification", value=False)

    st.markdown("---")
    st.markdown("**Risk Levels:**")
    st.markdown("- **Verified**: Score > 80%")
    st.markdown("- **Check Manually**: Score 20-80%")
    st.markdown("- **POTENTIAL FRAUD**: Score < 20%")

uploaded_file = st.file_uploader(
    "Choose your file",
    type=["xlsx", "xls", "xlsm", "xlsb", "ods", "csv", "numbers"]
)

if uploaded_file:
    # Determine file type and read accordingly
    file_name = uploaded_file.name.lower()

    try:
        if file_name.endswith('.csv'):
            # CSV files
            df = pd.read_csv(uploaded_file)
        elif file_name.endswith('.xlsb'):
            # Excel Binary Workbook (requires pyxlsb)
            df = pd.read_excel(uploaded_file, engine='pyxlsb')
        elif file_name.endswith('.ods'):
            # OpenDocument Spreadsheet (requires odfpy)
            df = pd.read_excel(uploaded_file, engine='odf')
        elif file_name.endswith('.numbers'):
            # Apple Numbers (requires numbers-parser)
            from numbers_parser import Document
            import tempfile
            # Save uploaded file to temp location (numbers-parser needs a file path)
            with tempfile.NamedTemporaryFile(delete=False, suffix='.numbers') as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name
            doc = Document(tmp_path)
            # Read first table from first sheet
            sheets = doc.sheets
            tables = sheets[0].tables
            table = tables[0]
            # Convert to DataFrame
            data = table.rows(values_only=True)
            df = pd.DataFrame(data[1:], columns=data[0])
            # Clean up temp file
            os.unlink(tmp_path)
        else:
            # Standard Excel formats (xlsx, xls, xlsm)
            df = pd.read_excel(uploaded_file)
    except ImportError as e:
        if 'pyxlsb' in str(e):
            st.error("To read .xlsb files, please install pyxlsb: `pip install pyxlsb`")
            st.stop()
        elif 'odfpy' in str(e):
            st.error("To read .ods files, please install odfpy: `pip install odfpy`")
            st.stop()
        elif 'numbers_parser' in str(e):
            st.error("To read .numbers files, please install numbers-parser: `pip install numbers-parser`")
            st.stop()
        else:
            raise e

    # Clear previous results when a new file is uploaded
    if 'current_file' not in st.session_state or st.session_state['current_file'] != file_name:
        st.session_state['current_file'] = file_name
        if 'validation_results' in st.session_state:
            del st.session_state['validation_results']
        if 'duplicate_data' in st.session_state:
            del st.session_state['duplicate_data']

    st.write("Preview:")
    st.dataframe(df.head())

    # --- COLUMN FORMAT SELECTION ---
    st.markdown("### Data Format Configuration")

    data_format = st.radio(
        "How is the VAT data formatted?",
        options=["Combined (e.g., DK12345678)", "Separate Columns (Country + Number)"],
        index=0,
        horizontal=True
    )

    # Auto-detect columns based on keywords
    vat_keywords = ["vat no.", "vat no", "vat", "momsnummer", "moms", "tax code", "taxcode", "cvr", "nummer"]
    country_keywords = ["country code", "country", "landekode", "land"]

    # Find default VAT column
    default_vat_col = None
    for col in df.columns:
        if any(keyword in str(col).lower() for keyword in vat_keywords):
            default_vat_col = col
            break
    if default_vat_col is None:
        default_vat_col = df.columns[0]

    # Find default country column
    default_country_col = None
    for col in df.columns:
        if any(keyword in str(col).lower() for keyword in country_keywords):
            default_country_col = col
            break

    # Conditional column selectors
    if data_format == "Combined (e.g., DK12345678)":
        vat_column = st.selectbox(
            "Select VAT Column (with country code):",
            options=df.columns.tolist(),
            index=df.columns.tolist().index(default_vat_col) if default_vat_col in df.columns.tolist() else 0
        )
        country_column = None
        number_column = None
        st.info(f"Using combined VAT column: **{vat_column}**")
    else:
        col1, col2 = st.columns(2)
        with col1:
            country_column = st.selectbox(
                "Select Country Code Column:",
                options=df.columns.tolist(),
                index=df.columns.tolist().index(default_country_col) if default_country_col and default_country_col in df.columns.tolist() else 0
            )
        with col2:
            number_column = st.selectbox(
                "Select VAT Number Column:",
                options=df.columns.tolist(),
                index=df.columns.tolist().index(default_vat_col) if default_vat_col in df.columns.tolist() else 0
            )
        vat_column = None
        st.info(f"Will combine: **{country_column}** + **{number_column}**")

    # Column selector for customer name (for fraud detection)
    name_column = None
    if enable_fraud_detection:
        st.markdown("### Fraud Detection Setup")
        name_keywords = ["name", "company", "firma", "kunde", "customer", "navn"]
        default_name_col = None
        for col in df.columns:
            if any(keyword in str(col).lower() for keyword in name_keywords):
                default_name_col = col
                break

        name_column = st.selectbox(
            "Select column with Customer/Company Names:",
            options=df.columns.tolist(),
            index=df.columns.tolist().index(default_name_col) if default_name_col else 0
        )
        st.success(f"Will compare names from **{name_column}** with VIES records.")

    # --- CHECKPOINT/RESUME DETECTION ---
    # Check if there's a saved checkpoint for this session
    checkpoint_exists = has_checkpoint()
    resume_mode = False

    if checkpoint_exists:
        st.warning("A previous validation was interrupted. Would you like to resume?")
        col_resume, col_restart = st.columns(2)
        with col_resume:
            if st.button("Resume Previous Validation", type="primary"):
                resume_mode = True
                st.session_state['resume_requested'] = True
        with col_restart:
            if st.button("Start Fresh (Delete Checkpoint)"):
                cleanup_checkpoint()
                st.session_state['resume_requested'] = False
                st.rerun()

    # Handle resume request from previous interaction
    if st.session_state.get('resume_requested', False):
        resume_mode = True

    if st.button("Start Validation") or resume_mode:

        # Reset circuit breaker counters for new validation run
        country_error_count.clear()

        # Turbo Mode notification (using empty placeholder so it can be cleared)
        turbo_mode_placeholder = st.empty()
        turbo_mode_placeholder.success(f"Turbo Mode Active: Checking {MAX_WORKERS} numbers at once.")

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
                executor.submit(process_single_vat, idx, vat, name): idx
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

        # Calculate summary statistics
        valid_count = sum(1 for r in results if r["VIES Validation Status"] == "Valid")
        invalid_count = sum(1 for r in results if r["VIES Validation Status"] == "Invalid")
        invalid_format_count = sum(1 for r in results if r["VIES Validation Status"] == "Invalid Format")
        service_error_count = sum(1 for r in results if r["VIES Validation Status"] == "Service Unavailable")
        other_error_count = total - valid_count - invalid_count - invalid_format_count - service_error_count

        st.success(f"Complete! Processed {total} VAT numbers in {total_time_str}.")

        # Summary metrics
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Valid", valid_count, delta=None)
        col2.metric("Invalid", invalid_count, delta=None)
        col3.metric("Invalid Format", invalid_format_count, delta=None)
        col4.metric("Service Unavailable", service_error_count, delta=None)
        col5.metric("Other Errors", other_error_count, delta=None)

        # Fraud detection summary (if enabled)
        if fraud_detection_enabled:
            st.markdown("### Fraud Detection Summary")
            fraud_results = [r for r in results if r["Identity Risk"] != "---"]
            if fraud_results:
                verified_count = sum(1 for r in fraud_results if r["Identity Risk"] == "Verified")
                check_count = sum(1 for r in fraud_results if r["Identity Risk"] == "Check Manually")
                fraud_count = sum(1 for r in fraud_results if r["Identity Risk"] == "POTENTIAL FRAUD")

                fcol1, fcol2, fcol3 = st.columns(3)
                fcol1.metric("Verified", verified_count)
                fcol2.metric("Check Manually", check_count)
                fcol3.metric("POTENTIAL FRAUD", fraud_count)

        result_df = pd.DataFrame(results)

        # Remove fraud detection columns if not enabled
        if not fraud_detection_enabled:
            result_df = result_df.drop(columns=["Customer Name (Input)", "Name Match Score", "Identity Risk"], errors='ignore')

        # Style the validation status and identity risk columns
        def highlight_status(val):
            if val == "Valid":
                return "color: green; font-weight: bold"
            elif val == "Invalid":
                return "color: red; font-weight: bold"
            elif val == "Invalid Format":
                return "color: purple; font-weight: bold"
            elif val == "Service Unavailable":
                return "color: orange; font-weight: bold"
            elif val in ["Unknown", "Error"]:
                return "color: gray; font-weight: bold"
            return ""

        def highlight_risk(val):
            if val == "Verified":
                return "color: green; font-weight: bold"
            elif val == "Check Manually":
                return "color: orange; font-weight: bold"
            elif val == "POTENTIAL FRAUD":
                return "color: red; font-weight: bold; background-color: #ffcccc"
            return ""

        if fraud_detection_enabled and "Identity Risk" in result_df.columns:
            styled_df = result_df.style.applymap(highlight_status, subset=["VIES Validation Status"]).applymap(highlight_risk, subset=["Identity Risk"])
        else:
            styled_df = result_df.style.applymap(highlight_status, subset=["VIES Validation Status"])

        st.dataframe(styled_df, use_container_width=True)

        # Download buttons
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            csv = result_df.to_csv(index=False).encode('utf-8')
            st.download_button("Download CSV", csv, "vat_results.csv", "text/csv", key="download_csv")
        with col2:
            # Use session-specific temp file to avoid race conditions
            temp_excel_file = os.path.join(CACHE_DIR, f"temp_{st.session_state['session_id']}.xlsx")
            ensure_cache_dir()
            excel_buffer = pd.ExcelWriter(temp_excel_file, engine='openpyxl')
            result_df.to_excel(excel_buffer, index=False)
            excel_buffer.close()
            with open(temp_excel_file, "rb") as f:
                excel_data = f.read()
            # Clean up temp excel file after reading
            try:
                os.remove(temp_excel_file)
            except Exception:
                pass
            st.download_button("Download Excel", excel_data, "vat_results.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_excel")
        with col3:
            if duplicate_data:
                duplicates_df = pd.DataFrame(duplicate_data)
                duplicates_csv = duplicates_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    f"Download Duplicates ({len(duplicate_data)})",
                    duplicates_csv,
                    "vat_duplicates.csv",
                    "text/csv",
                    key="download_duplicates"
                )
            else:
                st.info("No duplicates found")
        with col4:
            if st.button("Start Over", key="start_over"):
                # Clean up any session files and reset state
                cleanup_checkpoint()
                for key in ['validation_results', 'duplicate_data', 'total_time_str',
                           'total_count', 'fraud_detection_enabled', 'resume_requested', 'current_file']:
                    if key in st.session_state:
                        del st.session_state[key]
                st.rerun()
