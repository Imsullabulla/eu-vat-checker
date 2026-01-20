import pandas as pd
import random

# --- 1. DE GYLDIGE (40 stk - 80%) ---
# Vi bruger en liste af rigtige store firmaer i EU
real_companies = [
    # Danmark
    {"Land": "DK", "Momsnummer": "DK13598562", "Note": "Tivoli"},
    {"Land": "DK", "Momsnummer": "DK47458714", "Note": "LEGO"},
    {"Land": "DK", "Momsnummer": "DK24256790", "Note": "Novo Nordisk"},
    {"Land": "DK", "Momsnummer": "DK53139655", "Note": "A.P. Møller - Mærsk"},
    {"Land": "DK", "Momsnummer": "DK10093789", "Note": "Carlsberg"},
    # Sverige
    {"Land": "SE", "Momsnummer": "SE556074308901", "Note": "Volvo Cars"},
    {"Land": "SE", "Momsnummer": "SE556042722001", "Note": "IKEA AB"},
    {"Land": "SE", "Momsnummer": "SE556242437201", "Note": "H&M"},
    {"Land": "SE", "Momsnummer": "SE556703748501", "Note": "Spotify"},
    # Tyskland
    {"Land": "DE", "Momsnummer": "DE129274202", "Note": "Siemens"},
    {"Land": "DE", "Momsnummer": "DE143586880", "Note": "BMW"},
    {"Land": "DE", "Momsnummer": "DE118517536", "Note": "Deutsche Post"},
    {"Land": "DE", "Momsnummer": "DE114109369", "Note": "Adidas"},
    # Irland (Tech giganter)
    {"Land": "IE", "Momsnummer": "IE6388047V", "Note": "Google Ireland"},
    {"Land": "IE", "Momsnummer": "IE9692928F", "Note": "Facebook Ireland"},
    {"Land": "IE", "Momsnummer": "IE6364992H", "Note": "Microsoft Ireland"},
    # Frankrig
    {"Land": "FR", "Momsnummer": "FR10632012100", "Note": "L'Oreal"},
    {"Land": "FR", "Momsnummer": "FR40302476106", "Note": "Carrefour"},
    {"Land": "FR", "Momsnummer": "FR54303923986", "Note": "Danone"},
    # Italien
    {"Land": "IT", "Momsnummer": "IT00159560366", "Note": "Ferrari"},
    {"Land": "IT", "Momsnummer": "IT00224140368", "Note": "Barilla"},
]

# Vi dublerer listen for at nå op på ca 40 gyldige (det gør ikke noget at de går igen i testen)
valid_data = real_companies + real_companies
valid_data = valid_data[:40] # Sørg for vi har præcis 40

# --- 2. DE UGYLDIGE (10 stk - 20%) ---
invalid_data = [
    {"Land": "DK", "Momsnummer": "DK00000000", "Note": "Ugyldigt nummer"},
    {"Land": "DK", "Momsnummer": "DK123", "Note": "For kort"},
    {"Land": "DE", "Momsnummer": "DE12345ABC", "Note": "Bogstaver i tal"},
    {"Land": "SE", "Momsnummer": "SE1234567890", "Note": "Forkert længde"},
    {"Land": "IT", "Momsnummer": "IT99999999999", "Note": "Findes ikke"},
    {"Land": "FR", "Momsnummer": "", "Note": "Tomt felt"},
    {"Land": "US", "Momsnummer": "US123456789", "Note": "Ikke EU land"},
    {"Land": "DK", "Momsnummer": "DK 99 99 99 99", "Note": "Med mellemrum (Test af rens)"},
    {"Land": "NO", "Momsnummer": "NO123456789", "Note": "Norge er ikke i EU VIES"},
    {"Land": "??", "Momsnummer": "HeltForkert", "Note": "Nonsens"},
]

# --- 3. SAML OG BLAND ---
full_list = valid_data + invalid_data
random.shuffle(full_list) # Bland dem godt

# --- 4. GEM ---
df = pd.DataFrame(full_list)
filnavn = "test_80_20.xlsx"
df.to_excel(filnavn, index=False)

print(f"✅ Filen '{filnavn}' er oprettet!")
print(f"   - Total: {len(full_list)} rækker")
print(f"   - Gyldige: {len(valid_data)} (80%)")
print(f"   - Ugyldige: {len(invalid_data)} (20%)")