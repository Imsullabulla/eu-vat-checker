import pandas as pd
import random

# 1. Liste med KENDTE, GYLDIGE numre (Sikre hits)
known_valid = [
    {"Land": "DK", "Momsnummer": "DK13598562"}, # Tivoli
    {"Land": "DK", "Momsnummer": "DK47458714"}, # LEGO
    {"Land": "SE", "Momsnummer": "SE556074308901"}, # Volvo
    {"Land": "DE", "Momsnummer": "DE129274202"}, # Siemens
    {"Land": "IE", "Momsnummer": "IE6388047V"}, # Google
    {"Land": "FR", "Momsnummer": "FR10632012100"}, # L'Oreal
    {"Land": "IT", "Momsnummer": "IT00159560366"}, # Ferrari
    {"Land": "NL", "Momsnummer": "NL001007727B01"}, # Heineken
    {"Land": "BE", "Momsnummer": "BE0417497106"}, # AB InBev
    {"Land": "AT", "Momsnummer": "ATU33864707"}, # Red Bull
    {"Land": "FI", "Momsnummer": "FI01120389"}, # Nokia
    {"Land": "PT", "Momsnummer": "PT500278188"}, # TAP Air
    {"Land": "LU", "Momsnummer": "LU26375245"}, # Amazon
    {"Land": "CZ", "Momsnummer": "CZ00177041"}, # Skoda
    {"Land": "PL", "Momsnummer": "PL7361701370"}, # CD Projekt
]

# 2. Funktion til at lave "Fake" numre der ser rigtige ud (tilfældige tal)
def generate_random_vat(country, length):
    # Laver en streng af tilfældige tal i den ønskede længde
    random_digits = ''.join([str(random.randint(0, 9)) for _ in range(length)])
    return f"{country}{random_digits}"

# Vi genererer en masse tilfældige (sandsynligvis ugyldige) numre
random_data = []
for _ in range(80):
    choice = random.choice([
        ("DK", 8),   # Danske er 8 cifre
        ("SE", 12),  # Svenske er 12 cifre
        ("DE", 9),   # Tyske er 9 cifre
        ("IT", 11),  # Italienske er 11 cifre
        ("FR", 11),  # Franske er 11 cifre
    ])
    vat = generate_random_vat(choice[0], choice[1])
    random_data.append({"Land": f"Random {choice[0]}", "Momsnummer": vat})

# 3. Nogle helt forkerte formater (til fejl-test)
bad_data = [
    {"Land": "Fejl", "Momsnummer": "DK123"},       # For kort
    {"Land": "Fejl", "Momsnummer": "TEST-1234"},   # Bogstaver
    {"Land": "Fejl", "Momsnummer": ""},            # Tom
    {"Land": "Fejl", "Momsnummer": "DK 99 99 99"}, # Med mellemrum (skal koden kunne klare)
    {"Land": "Fejl", "Momsnummer": "DE000000000"}  # Tysk format, men ugyldigt
]

# 4. Saml det hele og bland kortene
full_list = known_valid + random_data + bad_data
random.shuffle(full_list) # Bland listen så de gyldige ligger spredt

# Lav til DataFrame og gem
df = pd.DataFrame(full_list)
filnavn = "stor_test_100.xlsx"
df.to_excel(filnavn, index=False)

print(f"✅ Filen '{filnavn}' er oprettet med {len(full_list)} linjer.")
print("   - Indeholder både gyldige firmaer, tilfældige tal og fejl-data.")