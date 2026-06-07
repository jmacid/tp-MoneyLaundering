from common import message_protocol

class MessageHandler:

    def __init__(self, client_id: str):
        self.client_id = client_id
    
    def serialize_data_message(self, line: str) -> bytes:
        fields = line.strip().split(',')
        [timestamp, from_bank, from_account, to_bank, to_account,
        amount_received, receiving_currency, amount_paid,
        payment_currency, payment_format, is_laundering] = fields

        transaction_dict = {
            "client_id": self.client_id,
            "timestamp": timestamp,
            "from_bank": from_bank,
            "from_account": from_account,
            "to_bank": to_bank,
            "to_account": to_account,
            "amount_received": float(amount_received),
            "receiving_currency": receiving_currency,
            "amount_paid": float(amount_paid),
            "payment_currency": payment_currency,
            "payment_format": payment_format,
            "is_laundering": is_laundering == 'True'
        }
        return message_protocol.internal.serialize(transaction_dict)

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client_id])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        return fields