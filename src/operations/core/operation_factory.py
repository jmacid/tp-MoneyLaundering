from operations.filters.amount_filter import AmountFilter
from operations.filters.currency_fIlter import CurrencyFilter
from operations.filters.date_range_filter import DateRangeFilter
from operations.filters.payment_method_filter import PaymentMethodFilter

class OperationFactory:

    @staticmethod
    def create(operation_type: str, **kwargs):

        operations = {
            "currency": CurrencyFilter,
            "date_range": DateRangeFilter,
            "amount": AmountFilter,
            "payment_method": PaymentMethodFilter,
        }

        operation_class = operations.get(operation_type)

        if not operation_class:
            raise ValueError(f"Unknown operation type: {operation_type}")

        return operation_class(**kwargs)
