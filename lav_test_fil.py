import pandas as pd

# Vi laver nogle test-data
data = {
    "Virksomhed": ["Tivoli A/S", "Volvo (Sverige)", "Ugyldigt Test Nummer", "LEGO (Med mellemrum)"],
    "Momsnummer": [
        "DK13598562",       # Gyldigt
        "SE556056625801",   # Gyldigt svensk
        "DK99999999",       # Ugyldigt
        "DK 12 89 08 00"    # Gyldigt (Lego), men med mellemrum (tester vores rense-funktion)
    ]
}

# Lav om til en dataframe (tabel)
df = pd.DataFrame(data)

# Gem som Excel-fil
df.to_excel("moms_test.xlsx", index=False)

print("✅ Filen 'moms_test.xlsx' er oprettet!")