# Commands
- run: streamlit run app.py
- install: pip install pandas streamlit requests openpyxl beautifulsoup4 thefuzz python-levenshtein

# Build Rules
- Language: Python 3
- UI Framework: Streamlit
- Data Handling: Pandas

# Coding Guidelines
- **Error Handling:** Never let the app crash. Use try/except blocks, especially when calling the VIES API.
- **Performance:** Always use `ThreadPoolExecutor` (max_workers=5) for bulk processing.
- **User Feedback:** Use `st.empty()` for status messages and clear them when processing is done.
- **Excel:** Handle inputs robustly. Always convert VAT numbers to strings and strip `.0` (floats) before processing.
- **Privacy:** Do not print sensitive VAT data to the terminal console; show it in the UI dataframe only.
- **Identity:** Always use a browser-like 'User-Agent' header for API requests to avoid blocking.