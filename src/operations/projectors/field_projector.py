from domain.transaction_fields import TRANSACTION_FIELDS

class FieldProjector:

    def __init__(self, fields: list[str]):
        invalid_fields = set(fields) - TRANSACTION_FIELDS

        if invalid_fields:
            raise ValueError(f"Invalid projection fields: {invalid_fields}")

        self.fields = fields

    def project(self, transaction) -> dict:
        return {
            field: getattr(transaction, field)
            for field in self.fields
        }