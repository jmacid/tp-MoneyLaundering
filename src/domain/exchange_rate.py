from dataclasses import dataclass
from datetime import date
from decimal import Decimal

@dataclass(slots=True, frozen=True)
class ExchangeRate:
    from_currency: str
    to_currency: str
    rate: Decimal
    rate_date: date