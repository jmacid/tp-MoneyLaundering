import argparse
import csv
import os
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path


TARGET_CURRENCY = os.getenv("TARGET_CURRENCY", "US Dollar")

# CSV column indexes.
TIMESTAMP = 0
FROM_ACCOUNT = 2
AMOUNT_PAID = 7
PAYMENT_CURRENCY = 8
PAYMENT_FORMAT = 9

BASE_START_DATE = "2022/09/01"
BASE_END_DATE = "2022/09/05"

TARGET_START_DATE = "2022/09/06"
TARGET_END_DATE = "2022/09/15"

CENTESIMAL_FACTOR = Decimal("100")

def is_date_between(value: str, start: str, end: str) -> bool:
    return start <= value <= end

def print_progress(current_bytes: int, total_bytes: int, label: str) -> None:
    if total_bytes == 0:
        return

    percentage = (current_bytes / total_bytes) * 100
    print(f"\r{label}: {percentage:.2f}%", end="", flush=True)


def calculate_average_by_payment_format(input_file: str) -> dict[str, Decimal]:
    """
    Calculate the average USD paid amount by payment format for the base period.

    Base period:
    [2022-09-01, 2022-09-05]
    """
    total_bytes = Path(input_file).stat().st_size

    totals_by_format: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    counts_by_format: dict[str, int] = defaultdict(int)

    progress_label = "Calculating base averages by payment format"

    with open(input_file, "r", newline="", encoding="utf-8") as infile:
        # Skip input header.
        header_line = infile.readline()
        if not header_line:
            return {}

        last_printed_percentage = -1

        while True:
            line = infile.readline()

            if not line:
                break

            current_bytes = infile.tell()
            percentage = int((current_bytes / total_bytes) * 100)

            if percentage != last_printed_percentage:
                print_progress(
                    current_bytes,
                    total_bytes,
                    progress_label,
                )
                last_printed_percentage = percentage

            try:
                row = next(csv.reader([line]))

                # Cheap filter first: currency.
                payment_currency = row[PAYMENT_CURRENCY].strip()
                if payment_currency != TARGET_CURRENCY:
                    continue

                # Cheap filter second: date string comparison.
                transaction_date = row[TIMESTAMP].strip()[:10]
                if not is_date_between(
                    transaction_date,
                    BASE_START_DATE,
                    BASE_END_DATE,
                ):
                    continue

                # More expensive operations are only executed after filtering.
                payment_format = row[PAYMENT_FORMAT].strip()
                amount_paid = Decimal(row[AMOUNT_PAID].strip())

            except (InvalidOperation, IndexError, StopIteration):
                continue

            totals_by_format[payment_format] += amount_paid
            counts_by_format[payment_format] += 1

    # Force final progress print because the last rows may have been skipped.
    print_progress(total_bytes, total_bytes, progress_label)
    print()

    averages_by_format: dict[str, Decimal] = {}

    for payment_format, total in totals_by_format.items():
        count = counts_by_format[payment_format]

        if count > 0:
            averages_by_format[payment_format] = total / Decimal(count)

    return averages_by_format

def project_rule_3_transaction(row: list[str]) -> list[str]:
    """
    Project the output row for rule 3.

    Output format without header:
    - source account
    - paid amount
    """
    return [
        row[FROM_ACCOUNT].strip(),
        row[AMOUNT_PAID].strip(),
    ]


def generate_expected_rule_3(input_file: str, expected_file: str) -> None:
    """
    Generate expected output for rule 3.

    Rule:
    Source account and amount of USD transactions in period [2022-09-06, 2022-09-15]
    with amount lower than one hundredth of the average found for the same
    payment format in period [2022-09-01, 2022-09-05].

    The generated file has no header.
    """
    averages_by_format = calculate_average_by_payment_format(input_file)

    total_bytes = Path(input_file).stat().st_size
    progress_label = "Generating expected rule 3 output"

    with open(input_file, "r", newline="", encoding="utf-8") as infile, \
         open(expected_file, "w", newline="", encoding="utf-8") as outfile:

        writer = csv.writer(outfile)

        # Skip input header.
        header_line = infile.readline()
        if not header_line:
            return

        last_printed_percentage = -1

        while True:
            line = infile.readline()

            if not line:
                break

            current_bytes = infile.tell()
            percentage = int((current_bytes / total_bytes) * 100)

            if percentage != last_printed_percentage:
                print_progress(
                    current_bytes,
                    total_bytes,
                    progress_label,
                )
                last_printed_percentage = percentage

            try:
                row = next(csv.reader([line]))

                # Cheap filter first: currency.
                payment_currency = row[PAYMENT_CURRENCY].strip()
                if payment_currency != TARGET_CURRENCY:
                    continue

                # Cheap filter second: target date range.
                transaction_date = row[TIMESTAMP].strip()[:10]
                if not is_date_between(
                    transaction_date,
                    TARGET_START_DATE,
                    TARGET_END_DATE,
                ):
                    continue

                payment_format = row[PAYMENT_FORMAT].strip()

                average_for_format = averages_by_format.get(payment_format)
                if average_for_format is None:
                    continue

                amount_paid = Decimal(row[AMOUNT_PAID].strip())

            except (InvalidOperation, IndexError, StopIteration):
                continue

            threshold = average_for_format / CENTESIMAL_FACTOR

            if amount_paid < threshold:
                writer.writerow(project_rule_3_transaction(row))

    # Force final progress print because the last rows may have been skipped.
    print_progress(total_bytes, total_bytes, progress_label)
    print()

def normalize_row(row: list[str]) -> tuple[str, ...]:
    """
    Normalize a CSV row before comparing it.

    This avoids false differences caused by leading/trailing spaces.
    """
    return tuple(value.strip() for value in row)


def read_csv_as_counter(
    file_path: str,
    skip_header: bool = False,
    progress_label: str | None = None,
) -> Counter:
    """
    Read a CSV file as a Counter.

    This allows comparing files regardless of row order while preserving
    duplicated rows.
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
        print_progress(total_bytes, total_bytes, progress_label)
        print()

    return counter


def compare_outputs(expected_file: str, actual_file: str) -> None:
    """
    Compare expected and actual output files without relying on row order.

    Rule 3 output files have no header.
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
        print("\nExpected rows missing from actual output:")
        for row, count in missing.items():
            print(f"{count}x {row}")

    if unexpected:
        print("\nUnexpected rows found in actual output:")
        for row, count in unexpected.items():
            print(f"{count}x {row}")

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate and compare expected output for rule 3."
    )

    parser.add_argument(
        "--input-file",
        default=os.getenv("INPUT_FILE", "transactions.csv"),
        help="Original transaction CSV file.",
    )

    parser.add_argument(
        "--expected-file",
        default=os.getenv("EXPECTED_FILE", "RULE_3_expected.csv"),
        help="Expected output CSV file generated by this script.",
    )

    parser.add_argument(
        "--actual-file",
        default=os.getenv("ACTUAL_FILE"),
        help="Actual output CSV file generated by the TP.",
    )

    return parser.parse_args()

def main() -> None:
    args = parse_args()

    input_file = Path(args.input_file)
    expected_file = Path(args.expected_file)

    if not input_file.exists():
        print("Input file not found. Expected file generation aborted.")
        return

    print("Starting rule 3 expected output generation...")

    generate_expected_rule_3(
        str(input_file),
        str(expected_file),
    )

    print(f"Expected output generated: {expected_file}")

    if not args.actual_file:
        print("Actual output file not provided. Comparison skipped.")
        return

    actual_file = Path(args.actual_file)

    if not actual_file.exists():
        print("Actual output file not found. Comparison aborted.")
        return

    print("Starting comparison...")

    compare_outputs(
        str(expected_file),
        str(actual_file),
    )


if __name__ == "__main__":
    main()