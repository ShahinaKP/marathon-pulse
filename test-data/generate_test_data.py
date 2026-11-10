# test-data/generate_test_data.py
import random
from pathlib import Path
import openpyxl

# Target output directory relative to this script
SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = SCRIPT_DIR / "sample_10k.xlsx"

wb = openpyxl.Workbook()
ws = wb.active
ws.title = "Participants"

# 1. Header matching the marathon schema
ws.append(["First Name", "Last Name", "Email", "Bib Number", "Category", "Registration Fee"])

# Valid ENUM categories matching race_category_enum
VALID_CATEGORIES = ["FULL_MARATHON", "HALF_MARATHON", "10K", "5K"]

# 2. Populate 10,000 rows with valid data and deliberate failure cases
for i in range(1, 10001):
    bib_number = f"BIB-{1000 + i}"
    
    if i == 500:
        # Deliberate invalid email
        ws.append(["Bad", "EmailRow", "not-an-email", bib_number, "FULL_MARATHON", 120.50])
    elif i == 1000:
        # Deliberate invalid registration fee (string instead of numeric)
        ws.append(["Bad", "FeeRow", "jane@example.com", bib_number, "HALF_MARATHON", "NOT_A_NUMBER"])
    elif i == 1500:
        # Deliberate invalid category (violates ENUM validation)
        ws.append(["Bad", "CategoryRow", "john@example.com", bib_number, "ULTRA_MARATHON", 150.00])
    else:
        # Valid participant record
        category = random.choice(VALID_CATEGORIES)
        fee = 100.00 if category == "FULL_MARATHON" else (75.00 if category == "HALF_MARATHON" else 45.00)
        ws.append([f"Runner{i}", f"Pulse{i}", f"runner{i}@marathon.com", bib_number, category, fee])

wb.save(OUTPUT_FILE)
print(f"Generated {OUTPUT_FILE} with 10,000 rows.")