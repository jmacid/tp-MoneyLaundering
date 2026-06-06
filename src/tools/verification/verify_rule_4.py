import argparse
import csv
import os
from collections import Counter, defaultdict
from pathlib import Path


TARGET_CURRENCY = os.getenv("TARGET_CURRENCY", "US Dollar")

# CSV column indexes.
TIMESTAMP = 0
FROM_ACCOUNT = 2
TO_ACCOUNT = 4
PAYMENT_CURRENCY = 8

START_DATE = "2022/09/01"
END_DATE = "2022/09/05"

MIN_DISTINCT_ACCOUNTS = int(os.getenv("SCATTER_GATHER_MIN_ACCOUNTS", "5"))


def is_date_between(value: str, start: str, end: str) -> bool:
    return start <= value <= end


def print_progress(current_bytes: int, total_bytes: int, label: str) -> None:
    if total_bytes == 0:
        return

    percentage = (current_bytes / total_bytes) * 100
    print(f"\r{label}: {percentage:.2f}%", end="", flush=True)


def is_valid_rule_4_transaction(row: list[str]) -> bool:
    """
    Validate whether the transaction belongs to the rule 4 analysis scope.

    Scope:
    - USD transactions
    - Period [2022/09/01, 2022/09/05]
    """
    try:
        payment_currency = row[PAYMENT_CURRENCY].strip()
        if payment_currency != TARGET_CURRENCY:
            return False

        transaction_date = row[TIMESTAMP].strip()[:10]
        if not is_date_between(transaction_date, START_DATE, END_DATE):
            return False

        return True

    except IndexError:
        return False


def generate_expected_rule_4(input_file: str, expected_file: str) -> None:
    """
    Generate expected output for rule 4.

    Rule:
    Accounts that match the scatter-gather pattern with one intermediate
    account level, for accounts that transferred USD to at least 5 distinct
    accounts within period [2022/09/01, 2022/09/05].

    Pattern:
    source account -> at least 5 distinct intermediate accounts -> same final account

    Output format without header:
    - source account
    """
    total_bytes = Path(input_file).stat().st_size
    progress_label = "Generating expected rule 4 output"

    # source_account -> set(intermediate_accounts)
    scatter_by_source: dict[str, set[str]] = defaultdict(set)

    # intermediate_account -> set(final_accounts)
    gather_by_intermediate: dict[str, set[str]] = defaultdict(set)

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

                if not is_valid_rule_4_transaction(row):
                    continue

                source_account = row[FROM_ACCOUNT].strip()
                destination_account = row[TO_ACCOUNT].strip()

                if not source_account or not destination_account:
                    continue

            except (IndexError, StopIteration):
                continue

            scatter_by_source[source_account].add(destination_account)
            gather_by_intermediate[source_account].add(destination_account)

    print_progress(total_bytes, total_bytes, progress_label)
    print()

    matching_accounts: set[str] = set()

    for source_account, intermediate_accounts in scatter_by_source.items():
        if len(intermediate_accounts) < MIN_DISTINCT_ACCOUNTS:
            continue

        # final_account -> number of intermediate accounts that send to that final account
        final_account_counter: Counter[str] = Counter()

        for intermediate_account in intermediate_accounts:
            final_accounts = gather_by_intermediate.get(intermediate_account, set())

            for final_account in final_accounts:
                # Avoid counting direct loops back to the same source as gather target if needed.
                # Remove this condition if the TP allows source == final account.
                if final_account == source_account:
                    continue

                final_account_counter[final_account] += 1

        if any(count >= MIN_DISTINCT_ACCOUNTS for count in final_account_counter.values()):
            matching_accounts.add(source_account)

    with open(expected_file, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)

        for account in sorted(matching_accounts):
            writer.writerow([account])


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

    Rule 4 output files have no header.
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
        description="Generate and compare expected output for rule 4."
    )

    parser.add_argument(
        "--input-file",
        default=os.getenv("INPUT_FILE", "transactions.csv"),
        help="Original transaction CSV file.",
    )

    parser.add_argument(
        "--expected-file",
        default=os.getenv("EXPECTED_FILE", "RULE_4_expected.csv"),
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

    print("Starting rule 4 expected output generation...")

    generate_expected_rule_4(
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