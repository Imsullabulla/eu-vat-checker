import streamlit as st
import pandas as pd
import requests
import time
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# --- SETUP ---
st.set_page_config(page_title="EU VAT Checker", layout="wide")

API_URL = "https://ec.europa.eu/taxation_customs/vies/rest-api/ms/{}/vat/{}"
MAX_WORKERS = 5  # Number of concurrent threads
MAX_RETRIES = 3  # Number of retry attempts for failed requests
RETRY_DELAY = 2  # Seconds to wait between retries

# Thread-safe lock for any shared resources
results_lock = threading.Lock()

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

# --- FUNCTIONS ---

def clean_vat_number(text):
    """Removes spaces and extracts country code + number"""
    clean_text = re.sub(r'[^A-Z0-9]', '', str(text).upper())

    if len(clean_text) < 3:
        return None, None

    country = clean_text[:2]
    number = clean_text[2:]
    return country, number


def check_vat(country, number):
    """
    Queries EU VIES API for VAT validation with smart retry logic.

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
    full_url = API_URL.format(country, number)

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    last_error = None

    for attempt in range(MAX_RETRIES):
        try:
            # Rate limiting - polite delay per thread
            time.sleep(0.5)

            response = requests.get(full_url, headers=headers, timeout=30)

            # --- Handle HTTP error codes ---
            if response.status_code in RETRY_HTTP_CODES:
                last_error = f"HTTP_{response.status_code}"
                if attempt < MAX_RETRIES - 1:
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
                    "error_detail": f"HTTP {response.status_code} after {MAX_RETRIES} attempts",
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
                if attempt < MAX_RETRIES - 1:
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
                    "error_detail": f"{user_error} after {MAX_RETRIES} attempts",
                    "vat_number": data.get("vatNumber", number)
                }

            # --- Check for explicit "error" field in JSON ---
            if "error" in data:
                error_msg = data.get("error", "Unknown error")
                if any(err in str(error_msg).upper() for err in SERVICE_UNAVAILABLE_ERRORS):
                    if attempt < MAX_RETRIES - 1:
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
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                continue
            return {
                "valid": None,
                "name": "---",
                "address": "---",
                "request_date": "",
                "request_identifier": "",
                "error_type": "service_unavailable",
                "error_detail": f"Connection timeout after {MAX_RETRIES} attempts",
                "vat_number": number
            }

        except requests.exceptions.ConnectionError as e:
            last_error = "CONNECTION_ERROR"
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                continue
            return {
                "valid": None,
                "name": "---",
                "address": "---",
                "request_date": "",
                "request_identifier": "",
                "error_type": "service_unavailable",
                "error_detail": f"Connection error after {MAX_RETRIES} attempts",
                "vat_number": number
            }

        except Exception as e:
            last_error = str(e)[:50]
            if attempt < MAX_RETRIES - 1:
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


def process_single_vat(index, raw_vat):
    """Process a single VAT number and return result with index for ordering"""
    country, number = clean_vat_number(raw_vat)

    if not country or not number:
        return {
            "index": index,
            "result": {
                "No.": index + 1,
                "Name from Output (VIES)": "---",
                "Address from Output (VIES)": "---",
                "Country": "---",
                "VAT Registration No.": str(raw_vat),
                "VIES Validation Status": "Format Error",
                "Validation Result": "Unknown",
                "Validation Date & Time": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "Error Details": "Missing or invalid country code"
            }
        }

    response = check_vat(country, number)

    # Determine display status based on error_type and valid flag
    if response["error_type"] == "none" and response["valid"] is True:
        status = "Valid"
        result = "Valid"
    elif response["error_type"] == "invalid" and response["valid"] is False:
        status = "Invalid"
        result = "Invalid"
    elif response["error_type"] == "service_unavailable":
        status = "Service Unavailable"
        result = "Unknown"
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
            "Error Details": response["error_detail"] if response["error_detail"] else "---"
        }
    }


# --- WEBSITE ---

st.title("EU VAT Bulk Checker")
st.markdown("Upload an Excel file with VAT numbers (include country code, e.g. DK12345678).")

uploaded_file = st.file_uploader("Choose your Excel file", type=["xlsx", "xls"])

if uploaded_file:
    df = pd.read_excel(uploaded_file)

    st.write("Preview:")
    st.dataframe(df.head())

    # Auto-detect VAT column: look for common names or use first column
    vat_column = None
    vat_keywords = ["vat", "moms", "cvr", "tax", "number", "nummer", "no"]
    for col in df.columns:
        if any(keyword in str(col).lower() for keyword in vat_keywords):
            vat_column = col
            break
    if vat_column is None:
        vat_column = df.columns[0]

    st.info(f"Using column: **{vat_column}**")

    if st.button("Start Validation"):

        # Turbo Mode notification (using empty placeholder so it can be cleared)
        turbo_mode_placeholder = st.empty()
        turbo_mode_placeholder.success(f"Turbo Mode Active: Checking {MAX_WORKERS} numbers at once.")

        progress_bar_placeholder = st.empty()
        progress_bar = progress_bar_placeholder.progress(0)
        status_text = st.empty()

        # Remove duplicate VAT numbers
        original_count = len(df)
        unique_vat_numbers = df[vat_column].drop_duplicates().tolist()
        unique_count = len(unique_vat_numbers)
        duplicates_removed = original_count - unique_count

        if duplicates_removed > 0:
            st.warning(f"Removed {duplicates_removed} duplicate VAT numbers. Processing {unique_count} unique numbers.")

        total = unique_count
        completed_count = 0

        # Prepare list of tasks with unique VAT numbers: (index, vat_number)
        tasks = [(i, vat) for i, vat in enumerate(unique_vat_numbers)]

        # Results dictionary to maintain order
        results_dict = {}

        # Start timer for time estimation
        start_time = time.time()

        # Process with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_index = {
                executor.submit(process_single_vat, idx, vat): idx
                for idx, vat in tasks
            }

            for future in as_completed(future_to_index):
                try:
                    result_data = future.result()
                    index = result_data["index"]

                    with results_lock:
                        results_dict[index] = result_data["result"]
                        completed_count += 1

                    # Update progress bar
                    progress_bar.progress(completed_count / total)

                    # Calculate time estimation
                    elapsed_time = time.time() - start_time
                    if completed_count > 0:
                        avg_time_per_item = elapsed_time / completed_count
                        items_remaining = total - completed_count
                        estimated_seconds_left = avg_time_per_item * items_remaining
                        time_remaining_str = format_time_remaining(estimated_seconds_left)

                        if items_remaining > 0:
                            status_text.text(f"Processing {completed_count}/{total} (Est. time remaining: {time_remaining_str})")
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
                            "Error Details": str(e)[:100]
                        }
                        completed_count += 1
                    progress_bar.progress(completed_count / total)

        # Sort results by original index
        results = [results_dict[i] for i in sorted(results_dict.keys())]

        # Calculate total time taken
        total_time = time.time() - start_time
        total_time_str = format_time_remaining(total_time)

        # Clear the turbo mode notification and progress bar
        turbo_mode_placeholder.empty()
        progress_bar_placeholder.empty()

        # Calculate summary statistics
        valid_count = sum(1 for r in results if r["VIES Validation Status"] == "Valid")
        invalid_count = sum(1 for r in results if r["VIES Validation Status"] == "Invalid")
        service_error_count = sum(1 for r in results if r["VIES Validation Status"] == "Service Unavailable")
        other_error_count = total - valid_count - invalid_count - service_error_count

        status_text.success(f"Complete! Processed {total} VAT numbers in {total_time_str}.")

        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Valid", valid_count, delta=None)
        col2.metric("Invalid", invalid_count, delta=None)
        col3.metric("Service Unavailable", service_error_count, delta=None)
        col4.metric("Other Errors", other_error_count, delta=None)

        result_df = pd.DataFrame(results)

        # Style the validation status column
        def highlight_status(val):
            if val == "Valid":
                return "color: green; font-weight: bold"
            elif val == "Invalid":
                return "color: red; font-weight: bold"
            elif val == "Service Unavailable":
                return "color: orange; font-weight: bold"
            elif val in ["Format Error", "Unknown", "Error"]:
                return "color: gray; font-weight: bold"
            return ""

        styled_df = result_df.style.applymap(highlight_status, subset=["VIES Validation Status"])
        st.dataframe(styled_df, use_container_width=True)

        # Download buttons
        col1, col2 = st.columns(2)
        with col1:
            csv = result_df.to_csv(index=False).encode('utf-8')
            st.download_button("Download CSV", csv, "vat_results.csv", "text/csv")
        with col2:
            excel_buffer = pd.ExcelWriter("temp.xlsx", engine='openpyxl')
            result_df.to_excel(excel_buffer, index=False)
            excel_buffer.close()
            with open("temp.xlsx", "rb") as f:
                excel_data = f.read()
            st.download_button("Download Excel", excel_data, "vat_results.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
