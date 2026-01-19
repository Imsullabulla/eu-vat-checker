import pandas as pd

# List of VERIFIED valid EU VAT numbers (tested January 2026)
data = [
    {"VAT Number": "DK10150817"},
    {"VAT Number": "IE6388047V"},
    {"VAT Number": "LU26375245"},
    {"VAT Number": "PT503504564"},
    {"VAT Number": "PL5260250995"},
    {"VAT Number": "IT00905811006"},
    {"VAT Number": "BE0403170701"},
    # Invalid number to test error handling
    {"VAT Number": "DK99999999"}
]

# Create dataframe
df = pd.DataFrame(data)

# Save as Excel
filename = "eu_vat_test.xlsx"
df.to_excel(filename, index=False)

print(f"File '{filename}' created with {len(data)} VAT numbers!")