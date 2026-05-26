from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime


@dataclass(slots=True)
class Transaction:
    timestamp: datetime

    from_bank: str
    from_account: str

    to_bank: str
    to_account: str

    amount_received: Decimal
    receiving_currency: str

    amount_paid: Decimal
    payment_currency: str

    payment_format: str

    is_laundering: bool

    normalized_amount_paid: Decimal | None = None
    normalized_amount_received: Decimal | None = None
    normalized_currency: str | None = None
