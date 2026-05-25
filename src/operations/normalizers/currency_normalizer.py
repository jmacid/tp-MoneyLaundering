from decimal import Decimal
import os
from typing import Any
from domain.exchange_rate import ExchangeRate
from operations.core.operation_strategy import OperationStrategy


class CurrencyNormalizer(OperationStrategy):

    REQUIRED_FIELDS = {
        "timestamp",
        "amount_paid",
        "payment_currency",
        "amount_received",
        "receiving_currency",
    }

    def __init__(self, target_currency: str | None = None):
        self.target_currency = target_currency or os.getenv("TARGET_CURRENCY")

        if self.target_currency is None:
            raise ValueError("Missing TARGET_CURRENCY")

        self.exchange_rates: dict[tuple[str, str, object], ExchangeRate] = {}

    def load_exchange_rate(self, exchange_rate: ExchangeRate) -> None:
        key = (
            exchange_rate.from_currency,
            exchange_rate.to_currency,
            exchange_rate.rate_date,
        )

        self.exchange_rates[key] = exchange_rate

    def normalize_amount(self, amount: Decimal, from_currency: str, rate_date) -> Decimal:

        return amount # for DEMO
        if from_currency == self.target_currency:
            return amount

        key = (
            from_currency,
            self.target_currency,
            rate_date,
        )

        exchange_rate = self.exchange_rates.get(key)

        if exchange_rate is None:
            raise ValueError(
                f"Exchange rate not found for "
                f"{from_currency} -> {self.target_currency} ({rate_date})"
            )

        return amount * exchange_rate.rate

    def process(self, transaction: dict[str, Any]) -> dict[str, Any] | None:
        #for DEMO
        return {
            **transaction,
            "normalized_amount_paid": 1000,
            "normalized_amount_received": 1000,
            "normalized_currency": self.target_currency,
        }
    
        self._validate_payload(transaction)

        rate_date = transaction["timestamp"].date()

        normalized_paid_amount = self.normalize_amount(
            amount=transaction["amount_paid"],
            from_currency=transaction["payment_currency"],
            rate_date=rate_date,
        )

        normalized_received_amount = self.normalize_amount(
            amount=transaction["amount_received"],
            from_currency=transaction["receiving_currency"],
            rate_date=rate_date,
        )

        return {
            **transaction,
            "normalized_amount_paid": normalized_paid_amount,
            "normalized_amount_received": normalized_received_amount,
            "normalized_currency": self.target_currency,
        }

    def _validate_payload(self, transaction: dict) -> None:
        missing_fields = self.REQUIRED_FIELDS - transaction.keys()

        if missing_fields:
            raise ValueError(
                f"Missing required fields for CurrencyNormalizer: {missing_fields}"
            )