import argparse
import csv
import json
import os
import urllib.parse
import urllib.request
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path


API_BASE_URL = os.getenv(
    "EXCHANGE_RATE_API_BASE_URL",
    "https://api.frankfurter.dev/v2",
)

TARGET_CURRENCY = os.getenv("TARGET_CURRENCY", "US Dollar")

PAYMENT_FORMATS = {
    value.strip()
    for value in os.getenv("RULE_5_PAYMENT_FORMATS", "Wire,ACH").split(",")
    if value.strip()
}

THRESHOLD_USD = Decimal(os.getenv("RULE_5_THRESHOLD_USD", "1"))

# CSV column indexes.
TIMESTAMP = 0
AMOUNT_PAID = 7
PAYMENT_CURRENCY = 8
PAYMENT_FORMAT = 9

START_DATE = "2022/09/01"
END_DATE = "2022/09/05"

API_START_DATE = "2022-09-01"
API_END_DATE = "2022-09-05"


CURRENCY_NAME_TO_CODE = {
    "US Dollar": "USD",
    "Euro": "EUR",
    "Yuan": "CNY",
    "Chinese Yuan": "CNY",
    "Yen": "JPY",
    "Japanese Yen": "JPY",
    "Rupee": "INR",
    "Indian Rupee": "INR",
    "Ruble": "RUB",
    "Russian Ruble": "RUB",
    "Mexican Peso": "MXN",
    "Brazil Real": "BRL",
    "Australian Dollar": "AUD",
    "Canadian Dollar": "CAD",
    "Swiss Franc": "CHF",
    "Pound Sterling": "GBP",
    "UK Pound": "GBP",
    "British Pound": "GBP",
}


def is_date_between(value: str, start: str, end: str) -> bool:
    return start <= value <= end


def print_progress(current_bytes: int, total_bytes: int, label: str) -> None:
    if total_bytes == 0:
        return

    percentage = (current_bytes / total_bytes) * 100
    print(f"\r{label}: {percentage:.2f}%", end="", flush=True)


def get_currency_code(currency_name: str) -> str:
    """
    Convert dataset currency name to ISO currency code.
    """
    currency_name = currency_name.strip()

    code = CURRENCY_NAME_TO_CODE.get(currency_name)

    if not code:
        raise ValueError(f"Unsupported currency: {currency_name}")

    return code


def get_transaction_date(row: list[str]) -> str:
    """
    Return dataset transaction date as YYYY/MM/DD.
    """
    return row[TIMESTAMP].strip()[:10]


def get_transaction_date_for_api(row: list[str]) -> str:
    """
    Convert dataset date format YYYY/MM/DD HH:MM to API date format YYYY-MM-DD.
    """
    return row[TIMESTAMP].strip()[:10].replace("/", "-")


def fetch_usd_based_rates_for_period(
    start_date: str,
    end_date: str,
) -> dict[tuple[str, str], Decimal]:
    """
    Fetch exchange rates for the full period using USD as base currency.

    Returned structure:
    {
        ("2022-09-01", "EUR"): Decimal("1.001"),
        ("2022-09-01", "GBP"): Decimal("0.865"),
        ...
    }

    Meaning:
    1 USD = rate quote currency.

    Therefore, to convert quote currency to USD:
    amount_usd = amount / rate
    """
    query_params = urllib.parse.urlencode(
        {
            "from": start_date,
            "to": end_date,
            "base": "USD",
        }
    )

    url = f"{API_BASE_URL}/rates?{query_params}"

    print(f"Fetching exchange rates once: {url}")

    request = urllib.request.Request(
    url,
    headers={
        "User-Agent": "tp-final-verification/1.0",
        "Accept": "application/json",
    },
)

    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))

    rates_by_date_and_currency: dict[tuple[str, str], Decimal] = {}

    for rate_item in payload:
        rate_date = rate_item["date"]
        quote_currency = rate_item["quote"]
        rate = Decimal(str(rate_item["rate"]))

        rates_by_date_and_currency[(rate_date, quote_currency)] = rate

    print(f"Exchange rates loaded: {len(rates_by_date_and_currency)}")

    return rates_by_date_and_currency


def find_rate_for_date_or_previous_available(
    api_date: str,
    currency_code: str,
    rates_by_date_and_currency: dict[tuple[str, str], Decimal],
) -> Decimal:
    """
    Find the exchange rate for the exact date.

    If the API does not return a rate for that exact date, for example because
    it is a weekend or non-publishing day, use the latest previous available
    rate within the already fetched range.
    """
    exact_rate = rates_by_date_and_currency.get((api_date, currency_code))

    if exact_rate is not None:
        return exact_rate

    available_dates = sorted(
        rate_date
        for rate_date, quote_currency in rates_by_date_and_currency
        if quote_currency == currency_code and rate_date <= api_date
    )

    if not available_dates:
        raise ValueError(
            f"Missing exchange rate for date={api_date}, currency={currency_code}"
        )

    previous_date = available_dates[-1]
    return rates_by_date_and_currency[(previous_date, currency_code)]


def convert_amount_to_usd(
    amount: Decimal,
    currency_name: str,
    api_date: str,
    rates_by_date_and_currency: dict[tuple[str, str], Decimal],
) -> Decimal:
    """
    Convert an amount to USD using preloaded exchange rates.

    Rates are USD-based:
    1 USD = rate quote currency.

    Therefore:
    amount_in_quote_currency / rate = amount_in_usd.
    """
    currency_code = get_currency_code(currency_name)

    if currency_code == "USD":
        return amount

    rate = find_rate_for_date_or_previous_available(
        api_date,
        currency_code,
        rates_by_date_and_currency,
    )

    return amount / rate


def generate_expected_rule_5(input_file: str, expected_file: str) -> None:
    """
    Generate expected output for rule 5.

    Rule:
    Count transactions in period [2022/09/01, 2022/09/05]
    with payment format Wire or ACH whose amount converted to USD is lower than 1.

    Output format without header:
    - count
    """
    rates_by_date_and_currency = fetch_usd_based_rates_for_period(
        API_START_DATE,
        API_END_DATE,
    )

    total_bytes = Path(input_file).stat().st_size
    progress_label = "Generating expected rule 5 output"

    count = 0
    skipped_rows = 0

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

            try:
                row = next(csv.reader([line]))

                transaction_date = get_transaction_date(row)

                if not is_date_between(transaction_date, START_DATE, END_DATE):
                    continue

                payment_format = row[PAYMENT_FORMAT].strip()

                if payment_format not in PAYMENT_FORMATS:
                    continue

                amount_paid = Decimal(row[AMOUNT_PAID].strip())
                payment_currency = row[PAYMENT_CURRENCY].strip()
                api_date = get_transaction_date_for_api(row)

                amount_usd = convert_amount_to_usd(
                    amount_paid,
                    payment_currency,
                    api_date,
                    rates_by_date_and_currency,
                )

            except (
                InvalidOperation,
                IndexError,
                StopIteration,
                ValueError,
            ):
                skipped_rows += 1
                continue

            if amount_usd < THRESHOLD_USD:
                count += 1

            current_bytes = infile.tell()
            percentage = int((current_bytes / total_bytes) * 100)

            if percentage != last_printed_percentage:
                print_progress(
                    current_bytes,
                    total_bytes,
                    progress_label,
                )
                last_printed_percentage = percentage

    print_progress(total_bytes, total_bytes, progress_label)
    print()

    with open(expected_file, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerow([count])

    print(f"Rule 5 count: {count}")

    if skipped_rows > 0:
        print(f"Skipped rows: {skipped_rows}")


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

            row = next(csv.reader([line]))

            if row:
                counter[normalize_row(row)] += 1

    if progress_label:
        print_progress(total_bytes, total_bytes, progress_label)
        print()

    return counter


def compare_outputs(expected_file: str, actual_file: str) -> None:
    """
    Compare expected and actual output files without relying on row order.

    Rule 5 output files have no header.
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
        for row, row_count in missing.items():
            print(f"{row_count}x {row}")

    if unexpected:
        print("\nUnexpected rows found in actual output:")
        for row, row_count in unexpected.items():
            print(f"{row_count}x {row}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate and compare expected output for rule 5."
    )

    parser.add_argument(
        "--input-file",
        default=os.getenv("INPUT_FILE", "transactions.csv"),
        help="Original transaction CSV file.",
    )

    parser.add_argument(
        "--expected-file",
        default=os.getenv("EXPECTED_FILE", "RULE_5_expected.csv"),
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

    print("Starting rule 5 expected output generation...")

    generate_expected_rule_5(
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