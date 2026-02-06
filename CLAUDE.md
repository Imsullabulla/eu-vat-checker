# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands
- **Run app:** `streamlit run app.py`
- **Install dependencies:** `pip install -r requirements.txt`
- **Generate test files:** `python lav_test_fil.py` (creates moms_test.xlsx with Danish/Swedish VAT examples)

## Architecture

Single-file Streamlit application (`app.py`, ~1470 lines) for bulk EU VAT validation against the VIES API.

### VIES API Integration

Two endpoints with different capabilities:

| Endpoint | Method | Purpose | Returns Consultation ID |
|----------|--------|---------|------------------------|
| `/rest-api/ms/{country}/vat/{number}` | GET | Anonymous validation | No |
| `/rest-api/check-vat-number` | POST | Authenticated validation | Yes (legal proof) |

**Two-step validation strategy in `check_vat()`:**
1. If requester VAT provided → try POST first (gets Consultation ID)
2. Fallback to GET if POST fails or no requester VAT

**Response field differences:**
- GET uses `isValid` field
- POST uses `valid` field (check both: `data.get("valid", data.get("isValid", False))`)

### Constants (lines 106-119)
```python
MAX_WORKERS = 5          # Concurrent threads
MAX_RETRIES = 3          # Per-request retry attempts
RETRY_DELAY = 2          # Seconds between retries
CIRCUIT_BREAKER_THRESHOLD = 3  # Failures before reducing retries
```

### Service Error Classification (lines 123-134)
These VIES `userError` values indicate temporary issues (should retry, not mark invalid):
- `MS_UNAVAILABLE`, `MS_MAX_CONCURRENT_REQ`, `SERVICE_UNAVAILABLE`
- `TIMEOUT`, `GLOBAL_MAX_CONCURRENT_REQ`, `VAT_BLOCKED`, `IP_BLOCKED`

### Validation Pipeline

```
Input → normalize_vat_input() → clean_vat_number() → validate_vat_format() → check_vat()
```

1. **`normalize_vat_input()`** (line 242): Handles Excel quirks
   - Floats: `12345678.0` → `"12345678"`
   - Scientific notation: `1.23e+10` → `"12300000000"`
   - NaN/None → skip row

2. **`clean_vat_number()`** (line 301): Extracts country + number
   - Strips all non-alphanumeric: `"DK-12 34.56"` → `("DK", "123456")`
   - First 2 chars must be letters (country code)

3. **`validate_vat_format()`** (line 177): Regex validation per country
   - Uses `EU_VAT_FORMATS` dict (lines 138-168)
   - Each country has specific pattern (e.g., DK = 8 digits, DE = 9 digits)

4. **`check_vat()`** (line 511): VIES API call with retry logic

### Concurrency Pattern

```python
# Thread-safe result collection (lines 113, 831-865)
with results_lock:
    results_dict[index] = result_data["result"]
    completed_indices.add(index)

# Circuit breaker per country (lines 115-119)
with country_error_lock:
    if country_error_count.get(country, 0) >= 3:
        max_retries = 1  # Reduce retries for failing countries
```

### Session State Keys (Streamlit)
- `session_id`: UUID for multi-user isolation
- `validation_results`: List of result dicts after processing
- `duplicate_data`: Tracked duplicates for download
- `resume_requested`: Flag for checkpoint resume
- `current_file`: Tracks uploaded file to clear results on new upload
- `confirmed_requester_vat`: User's own VAT for Consultation ID

### Checkpointing System (lines 24-104)
- Saves to `cache/cache_{session_id}.csv` every 10 completions
- Metadata columns: `_checkpoint_total`, `_checkpoint_index`
- Allows resume after browser refresh or interruption

### Fraud Detection (lines 187-238)
Uses thefuzz library for name matching:
```python
score = max(
    fuzz.token_set_ratio(name1, name2),   # Handles "LEGO" vs "LEGO System A/S"
    fuzz.partial_ratio(name1, name2),      # Substring matching
    fuzz.token_sort_ratio(name1, name2)    # Word order differences
)
```

**Risk thresholds (line 224-238):**
- `> 60`: Verified
- `20-60`: Check Manually
- `< 20`: POTENTIAL FRAUD

### Auto-Detection Logic (lines 330-427)
`detect_data_format()` samples 50 values to determine:
- **Combined format**: Values start with EU country code (e.g., "DK12345678")
- **Separate columns**: One column has standalone country codes

## Coding Patterns

### Error Handling
Always wrap VIES calls in try/except. Return structured dict with `error_type`:
- `"none"`: Success
- `"invalid"`: VAT definitively invalid
- `"service_unavailable"`: Temporary error, `valid` should be `None`
- `"format_error"`: Failed regex validation

### Adding New EU Countries
Add entry to `EU_VAT_FORMATS` dict (line 138):
```python
"XX": {"pattern": r"^\d{8}$", "format": "XX12345678", "description": "8 digits"}
```

### Streamlit UI Patterns
- Use `st.empty()` for clearable status messages
- Store results in `st.session_state` to persist after button clicks
- Use `st.rerun()` after state changes that need UI refresh

### Rate Limiting
- 0.5s `time.sleep()` before each API call (lines 565, 635)
- 2s delay on retry (`RETRY_DELAY`)

### User-Agent Header
Always use browser-like header to avoid VIES blocking:
```python
'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36...'
```
