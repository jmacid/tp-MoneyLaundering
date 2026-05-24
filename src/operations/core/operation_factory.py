from operations.counters.payment_method_counter import PaymentMethodCounter
from operations.filters.amount_filter import AmountFilter
from operations.filters.currency_fIlter import CurrencyFilter
from operations.filters.date_range_filter import DateRangeFilter
from operations.filters.payment_method_filter import PaymentMethodFilter
from operations.normalizers.currency_normalizer import CurrencyNormalizer

class OperationFactory:

    @staticmethod
    def create(operation_type: str, **kwargs):

        operations = {
            "currency_filter": CurrencyFilter,
            "date_range_filter": DateRangeFilter,
            "amount_filter": AmountFilter,
            "payment_method_filter": PaymentMethodFilter,
            "payment_method_counter": PaymentMethodCounter,
            "currency_normalizer": CurrencyNormalizer,
            #"projector_dispatcher": PaymentMethodFilter,
        }

        operation_class = operations.get(operation_type)

        if not operation_class:
            raise ValueError(f"Unknown operation type: {operation_type}")

        return operation_class(**kwargs)
