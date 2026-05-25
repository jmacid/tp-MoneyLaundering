from typing import Any

class TransactionValidator:

    @staticmethod
    def validate_required_fields(transaction: dict[str, Any], required_fields: set[str]) -> None:
        missing_fields = required_fields - transaction.keys()

        if missing_fields:
            raise ValueError(
                f"Missing required fields: {missing_fields}"
            )