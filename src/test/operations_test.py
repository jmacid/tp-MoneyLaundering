from datetime import datetime, date
from decimal import Decimal
from domain.exchange_rate import ExchangeRate
from domain.transaction import Transaction
from operations.normalizers.currency_normalizer import CurrencyNormalizer
from operations.projectors.field_projector import FieldProjector

def main():
    transaction = Transaction(
        timestamp=datetime(2022, 9, 1, 10, 30),
        from_bank="Bank A",
        from_account="ACC-001",
        to_bank="Bank B",
        to_account="ACC-999",
        amount_received=Decimal("1000"),
        receiving_currency="EUR",
        amount_paid=Decimal("1080"),
        payment_currency="USD",
        payment_format="ACH",
        is_laundering=False,
    )

    eur_to_usd = ExchangeRate(
        from_currency="EUR",
        to_currency="USD",
        rate=Decimal("1.08"),
        rate_date=date(2022, 9, 1),
    )

    normalizer = CurrencyNormalizer(target_currency="USD")
    normalizer.load_exchange_rate(eur_to_usd)

    normalized_transaction = normalizer.normalize(transaction)

    projector = FieldProjector([
        "from_account",
        "to_account",
        "amount_received",
        "receiving_currency",
        "normalized_amount_received",
        "normalized_amount_paid",
        "normalized_currency",
    ])

    result = projector.project(normalized_transaction)

    print("Original transaction:")
    print(transaction)

    print("\nNormalized transaction:")
    print(normalized_transaction)

    print("\nProjected fields:")
    print(result)


if __name__ == "__main__":
    main()
