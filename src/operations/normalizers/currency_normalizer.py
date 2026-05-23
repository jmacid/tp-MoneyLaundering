from decimal import Decimal
from domain.exchange_rate import ExchangeRate
from domain.transaction import Transaction

class CurrencyNormalizer:

    def __init__(self, target_currency: str):
        self.target_currency = target_currency

        self.exchange_rates: dict[tuple[str, str, object], ExchangeRate] = {}

    def load_exchange_rate(self, exchange_rate: ExchangeRate) -> None:

        key = (
            exchange_rate.from_currency,
            exchange_rate.to_currency,
            exchange_rate.rate_date,
        )

        self.exchange_rates[key] = exchange_rate

    def normalize_amount(self,
        amount: Decimal, from_currency: str, rate_date) -> Decimal:

        if from_currency == self.target_currency:
            return amount

        key = (
            from_currency,
            self.target_currency,
            rate_date,
        )

        exchange_rate = self.exchange_rates.get(key)

        if not exchange_rate:
            raise ValueError(
                f"Exchange rate not found for "
                f"{from_currency} -> "
                f"{self.target_currency} "
                f"({rate_date})"
            )

        return amount * exchange_rate.rate

    def normalize(self, transaction: Transaction) -> Transaction:
            normalized_paid_amount = self.normalize_amount(
                amount=transaction.amount_paid,
                from_currency=transaction.payment_currency,
                rate_date=transaction.timestamp.date(),
            )

            normalized_received_amount = self.normalize_amount(
                amount=transaction.amount_received,
                from_currency=transaction.receiving_currency,
                rate_date=transaction.timestamp.date(),
            )

            return Transaction(
                timestamp=transaction.timestamp,
                from_bank=transaction.from_bank,
                from_account=transaction.from_account,
                to_bank=transaction.to_bank,
                to_account=transaction.to_account,

                amount_received=transaction.amount_received,
                receiving_currency=transaction.receiving_currency,

                amount_paid=transaction.amount_paid,
                payment_currency=transaction.payment_currency,

                payment_format=transaction.payment_format,
                is_laundering=transaction.is_laundering,

                normalized_amount_received=normalized_received_amount,
                normalized_amount_paid=normalized_paid_amount,
                normalized_currency=self.target_currency,
            )
