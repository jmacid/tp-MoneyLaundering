from decimal import Decimal
from datetime import datetime, date
import os
import requests
import logging
from typing import Any
from domain.exchange_rate import ExchangeRate
from operations.core.operation_strategy import OperationStrategy
from shared.validators.transaction_validator import TransactionValidator

mapped_tag_currencies = {
    "US Dollar": "USD",
    "Euro": "EUR"
}
class CurrencyNormalizer(OperationStrategy):

    def __init__(self, target_currency: str | None = None):
        self.target_currency = target_currency or os.getenv("TARGET_CURRENCY")

        if self.target_currency is None:
            raise ValueError("Missing TARGET_CURRENCY")

        self.exchange_rates: dict[tuple[str, str, object], ExchangeRate] = {}
        self.required_fields = {"client_id", "timestamp","amount_paid","payment_currency","amount_received","receiving_currency"}

    def load_exchange_rate(self, exchange_rate: ExchangeRate) -> None:
        key = (
            exchange_rate.from_currency,
            exchange_rate.to_currency,
            exchange_rate.rate_date,
        )

        self.exchange_rates[key] = exchange_rate

    def normalize_amount(self, amount: Decimal, from_currency: str, rate_date) -> Decimal:

        currency_tag = mapped_tag_currencies.get(from_currency, from_currency)
        if currency_tag == self.target_currency:
            return amount

        logging.info(f"###############  MONEDA DIFERENTE {currency_tag}")
        key = (currency_tag, self.target_currency, rate_date)

        exchange_rate = self.exchange_rates.get(key)

        if key not in self.exchange_rates:
            date_str = rate_date.strftime('%Y-%m-%d')
            # url = f"https://api.frankfurter.app/{rate_date.strftime('%Y-%m-%d')}?from={from_currency}&to={self.target_currency}"
            url = f"https://api.frankfurter.dev/v2/rates?base={currency_tag}&quotes=USD&from={date_str}&to={rate_date.strftime('%Y-%m-%d')}"
            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                logging.info(f"Data: {data}")

                rate = Decimal(str(data[0]["rate"]))
                logging.info(f"Exchange rate from {currency_tag} to {self.target_currency}: {rate}")

                self.exchange_rates[key] = ExchangeRate(currency_tag, self.target_currency, rate, rate_date)
            except Exception as e:
                raise ValueError(f"Fallo al obtener exchange rate para {currency_tag} -> {self.target_currency} en {rate_date}: {e}")

        exchange_rate = self.exchange_rates[key]
        return amount * exchange_rate.rate

    def process(self, transaction: dict[str, Any]) -> dict[str, Any] | None:

        TransactionValidator.validate_required_fields(transaction, self.required_fields)

        try:
            timestamp = datetime.strptime(transaction["timestamp"], "%Y/%m/%d %H:%M")
            rate_date = timestamp.date()
        except ValueError:
            logging.error(f"Date conversion failed: {timestamp}")
            rate_date = datetime.fromisoformat(transaction["timestamp"]).date()

        normalized_paid_amount = self.normalize_amount(
            amount=Decimal(str(transaction["amount_paid"])),
            from_currency=transaction["payment_currency"],
            rate_date=rate_date,
        )

        normalized_received_amount = self.normalize_amount(
            amount=Decimal(str(transaction["amount_received"])),
            from_currency=transaction["receiving_currency"],
            rate_date=rate_date,
        )

        return {
            **transaction,
            "normalized_amount_paid": float(normalized_paid_amount), 
            "normalized_amount_received": float(normalized_received_amount),
            "normalized_currency": self.target_currency,
        }
