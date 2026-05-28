import json
from domain.message_type import MessageType

class MessageHandler:
    def __init__(self, client_id: str):
        self.client_id = client_id

    def serialize_data_message(self, message):
        [timestamp, from_bank, from_account, to_bank, to_account,
         amount_received, receiving_currency, amount_paid,
         payment_currency, payment_format, is_laundering] = message
        return json.dumps({
            "type": MessageType.TRANSACTION,
            "client_id": self.client_id,
            "timestamp": timestamp,
            "from_bank": from_bank,
            "from_account": from_account,
            "to_bank": to_bank,
            "to_account": to_account,
            "amount_received": amount_received,
            "receiving_currency": receiving_currency,
            "amount_paid": amount_paid,
            "payment_currency": payment_currency,
            "payment_format": payment_format,
            "is_laundering": is_laundering,
        }).encode()

    def serialize_eof_message(self, _):
        return json.dumps({
            "type": MessageType.EOF,
            "client_id": self.client_id,
        }).encode()

    def deserialize_result_message(self, message):
        return json.loads(message.decode())