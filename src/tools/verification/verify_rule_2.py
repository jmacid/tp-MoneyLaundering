import argparse
import csv
import os
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path
"""
    Example:

    python tools/verification/verify_minor_transactions.py \
    --input-file tests/data/transactions.csv \
    --expected-file tests/data/expected_minor_transactions.csv \
    --actual-file tests/data/tp_output_minor_transactions.csv

    python3 verify_minor_transactions.py   --input-file "/home/franco/Desktop/Sistemas Distribuidos I/tp-final/test/data/HI-Medium_Trans.csv"   --expected-file "./RULE_1_HI-Medium_Trans.csv" --actual-file ./RULE_1_expected.csv
    
    or

    INPUT_FILE=tests/data/transactions.csv \
    EXPECTED_FILE=tests/data/expected_minor_transactions.csv \
    ACTUAL_FILE=tests/data/tp_output_minor_transactions.csv \
    python tools/verification/verify_minor_transactions.py
"""

THRESHOLD = Decimal(os.getenv("MINOR_TRANSACTION_THRESHOLD", "50"))
TARGET_CURRENCY = os.getenv("TARGET_CURRENCY", "US Dollar")


# CSV column indexes.
# The CSV contains duplicated column names such as "Account",
# so using indexes is safer than using DictReader.
FROM_BANK = 1
FROM_ACCOUNT = 2
AMOUNT_PAID = 7
PAYMENT_CURRENCY = 8


def normalize_row(row: list[str]) -> tuple[str, ...]:
    """
    Normalize a CSV row before comparing it.

    This avoids false differences caused by leading/trailing spaces
    or line ending differences.
    """
    return tuple(value.strip() for value in row)


def is_minor_transaction(row: list[str]) -> bool:
    """
    Rule: transaction paid in the target currency with an amount below
    the configured threshold.
    """
    try:
        amount_paid = Decimal(row[AMOUNT_PAID].strip())
        payment_currency = row[PAYMENT_CURRENCY].strip()
    except (InvalidOperation, IndexError):
        return False

    return payment_currency == TARGET_CURRENCY and amount_paid < THRESHOLD

def project_max_transaction_by_bank(row: list[str]) -> list[str]:
    """
    Project the output row for the max USD transaction by source bank.

    Output format without header:
    - source bank ID
    - source account
    - paid amount
    """
    return [
        row[FROM_BANK].strip(),
        row[FROM_ACCOUNT].strip(),
        row[AMOUNT_PAID].strip(),
    ]


def generate_expected_max_usd_transaction_by_bank(
    input_file: str,
    expected_file: str,
) -> None:
    """
    Generate the expected output file with the max USD transaction for each source bank.

    The generated file has no header and only includes:
    source bank ID, source account and paid amount.
    """
    total_bytes = Path(input_file).stat().st_size
    max_by_bank: dict[str, list[str]] = {}

    with open(input_file, "r", newline="", encoding="utf-8") as infile:
        # Skip input header.
        header_line = infile.readline()
        if not header_line:
            return

        last_printed_percentage = -1

        while True:
            line = infile.readline()

            if not line:
                break

            row = next(csv.reader([line]))

            if not row:
                continue

            try:
                bank_id = row[FROM_BANK].strip()
                payment_currency = row[PAYMENT_CURRENCY].strip()
                amount_paid = Decimal(row[AMOUNT_PAID].strip())
            except (InvalidOperation, IndexError):
                continue

            if payment_currency != TARGET_CURRENCY:
                continue

            current_max_row = max_by_bank.get(bank_id)

            if current_max_row is None:
                max_by_bank[bank_id] = row
            else:
                current_max_amount = Decimal(
                    current_max_row[AMOUNT_PAID].strip()
                )

                if amount_paid > current_max_amount:
                    max_by_bank[bank_id] = row

            current_bytes = infile.tell()
            percentage = int((current_bytes / total_bytes) * 100)

            if percentage != last_printed_percentage:
                print_progress(
                    current_bytes,
                    total_bytes,
                    "Calculating max USD transaction by bank",
                )
                last_printed_percentage = percentage

    print()

    with open(expected_file, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)

        for bank_id in sorted(max_by_bank):
            writer.writerow(
                project_max_transaction_by_bank(max_by_bank[bank_id])
            )

def read_csv_as_counter(
    file_path: str,
    skip_header: bool = True,
    progress_label: str | None = None,
) -> Counter:
    """
    Read a CSV file as a Counter.

    This allows comparing files regardless of row order while still
    preserving duplicated transactions.

    Progress is estimated using bytes read from the file.
    """
    counter = Counter()
    total_bytes = Path(file_path).stat().st_size
    last_printed_percentage = -1

    with open(file_path, "r", newline="", encoding="utf-8") as file:
        if skip_header:
            file.readline()

        while True:
            line = file.readline()

            if not line:
                break

            row = next(csv.reader([line]))

            if row:
                counter[normalize_row(row)] += 1

            if progress_label:
                current_bytes = file.tell()
                percentage = int((current_bytes / total_bytes) * 100)

                if percentage != last_printed_percentage:
                    print_progress(
                        current_bytes,
                        total_bytes,
                        progress_label,
                    )
                    last_printed_percentage = percentage

    if progress_label:
        print()

    return counter

def compare_outputs(expected_file: str, actual_file: str) -> None:
    """
    Compare expected and actual output files without relying on row order.
    """
    expected = read_csv_as_counter(
        expected_file,
        skip_header=False,
        progress_label="Reading expected file",
    )

    actual = read_csv_as_counter(
        actual_file,
        skip_header=False,
        progress_label="Reading actual file",
    )

    missing = expected - actual
    unexpected = actual - expected

    if not missing and not unexpected:
        print("OK: actual output matches expected output.")
        return

    print("ERROR: actual output does not match expected output.")

    if missing:
        print("\nExpected transactions missing from actual output:")
        for row, count in missing.items():
            print(f"{count}x {row}")

    if unexpected:
        print("\nUnexpected transactions found in actual output:")
        for row, count in unexpected.items():
            print(f"{count}x {row}")

def parse_args() -> argparse.Namespace:
    """
    Parse CLI arguments.

    CLI arguments have priority over environment variables.
    Environment variables have priority over default values.
    """
    parser = argparse.ArgumentParser(
        description="Generate and compare expected output for minor transactions."
    )

    parser.add_argument(
        "--input-file",
        default=os.getenv("INPUT_FILE", "transactions.csv"),
        help="Original transaction CSV file.",
    )

    parser.add_argument(
        "--expected-file",
        default=os.getenv("EXPECTED_FILE", "expected_minor_transactions.csv"),
        help="Expected output CSV file generated by this script.",
    )

    parser.add_argument(
        "--actual-file",
        default=os.getenv("ACTUAL_FILE", "tp_output_minor_transactions.csv"),
        help="Actual output CSV file generated by the TP.",
    )

    return parser.parse_args()

def print_progress(current_bytes: int, total_bytes: int, label: str) -> None:
    if total_bytes == 0:
        return

    percentage = (current_bytes / total_bytes) * 100
    print(f"\r{label}: {percentage:.2f}%", end="", flush=True)


def main() -> None:
    args = parse_args()

    input_file = Path(args.input_file)
    expected_file = Path(args.expected_file)
    actual_file = Path(args.actual_file)

    if not input_file.exists():
        print(f"Input file not found. Generation expected file aborted")
    else:
        generate_expected_max_usd_transaction_by_bank(
            str(input_file),
            str(expected_file),
        )

    if not actual_file.exists():
        print(f"Actual output file not found. Comparison aborted")
    else:
        compare_outputs(str(expected_file), str(actual_file))


if __name__ == "__main__":
    main()