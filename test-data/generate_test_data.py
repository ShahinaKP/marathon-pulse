# test-data/generate_test_data.py
"""Generate Marathon Pulse test workbooks.

With no arguments it produces sample_10k.xlsx exactly as before (10,000 rows, 3 planted
invalid rows). Options make the other files the demo needs:

  python generate_test_data.py                                     # sample_10k.xlsx
  python generate_test_data.py --rows 50000 --out sample_50k.xlsx
  python generate_test_data.py --rows 10000 --tag a --out demo_a.xlsx   # distinct people ...
  python generate_test_data.py --rows 10000 --tag b --out demo_b.xlsx   # ... so 3 files can load at once
  python generate_test_data.py --rows 5000 --dupes 200 --tag d --out demo_dupes.xlsx
  python generate_test_data.py --corrupt --out demo_corrupt.xlsx

Two files only collide (become duplicates of each other) if they use the same --tag.
"""
import argparse
import os
import random
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
HEADER = ["First Name", "Last Name", "Email", "Bib Number", "Category", "Registration Fee"]
VALID_CATEGORIES = ["FULL_MARATHON", "HALF_MARATHON", "10K", "5K"]
FEES = {"FULL_MARATHON": 100.00, "HALF_MARATHON": 75.00, "10K": 45.00, "5K": 45.00}


def make_rows(rows: int, tag: str, planted_errors: bool, dupes: int, rng: random.Random):
    """Yield data rows (not the header). `tag` makes emails and bibs unique to a file."""
    suffix = f".{tag}" if tag else ""
    bib_tag = f"{tag.upper()}-" if tag else ""
    valid = []  # remember valid rows so duplicates can copy them

    for i in range(1, rows + 1):
        bib = f"BIB-{bib_tag}{1000 + i}"
        if planted_errors and i == 500:
            yield ["Bad", "EmailRow", "not-an-email", bib, "FULL_MARATHON", 120.50]
        elif planted_errors and i == 1000:
            yield ["Bad", "FeeRow", f"jane{suffix}@marathon.com", bib, "HALF_MARATHON", "NOT_A_NUMBER"]
        elif planted_errors and i == 1500:
            yield ["Bad", "CategoryRow", f"john{suffix}@marathon.com", bib, "ULTRA_MARATHON", 150.00]
        else:
            category = rng.choice(VALID_CATEGORIES)
            row = [f"Runner{i}", f"Pulse{i}", f"runner{i}{suffix}@marathon.com", bib, category, FEES[category]]
            valid.append(row)
            yield row

    # Duplicates: same email + bib as an earlier row (different name), so the database
    # unique constraint (email, bib_number) rejects them.
    for n in range(dupes):
        src = rng.choice(valid)
        yield [f"Dup{n}", "Copy", src[2], src[3], src[4], src[5]]


def write_workbook(path: Path, rows_iter) -> int:
    import openpyxl  # imported here so --corrupt works without it

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Participants"
    ws.append(HEADER)
    count = 0
    for row in rows_iter:
        ws.append(row)
        count += 1
    wb.save(path)
    return count


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rows", type=int, default=10000, help="data rows (default 10000)")
    ap.add_argument("--out", default="sample_10k.xlsx", help="output file name, written next to this script")
    ap.add_argument("--tag", default="", help="makes emails/bibs unique to this file (default: none)")
    ap.add_argument("--dupes", type=int, default=0, help="extra rows that duplicate earlier rows")
    ap.add_argument("--no-errors", action="store_true", help="do not plant the 3 invalid rows")
    ap.add_argument("--corrupt", action="store_true",
                    help="write a file that passes the API's .xlsx check but cannot be opened (tests FAILED handling)")
    ap.add_argument("--seed", type=int, default=None, help="random seed for repeatable files")
    args = ap.parse_args()

    out = Path(args.out)
    out = out if out.is_absolute() else SCRIPT_DIR / out

    if args.corrupt:
        out.write_bytes(b"PK\x03\x04" + os.urandom(4096))
        print(f"Generated {out} (corrupt workbook: valid .xlsx header, unreadable content).")
        return

    planted = not args.no_errors and args.rows >= 1500
    rng = random.Random(args.seed)
    count = write_workbook(out, make_rows(args.rows, args.tag, planted, args.dupes, rng))
    expected_invalid = 3 if planted else 0
    print(f"Generated {out} with {count:,} rows "
          f"({expected_invalid} invalid, {args.dupes} duplicate rows planted).")


if __name__ == "__main__":
    main()
