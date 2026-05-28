import os
import logging
import signal
import hashlib
from common import middleware, message_protocol

ID = int(os.environ.get("ID", "0"))
MOM_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
INPUT_EXCHANGE = os.environ.get("INPUT_EXCHANGE", "usd_transactions")
OUTPUT_EXCHANGE = os.environ.get("OUTPUT_EXCHANGE", "bank_shards")
AGGREGATORS_COUNT = int(os.environ.get("AGGREGATORS_COUNT", "1"))

class BankDispatcher:
    def __init__(self):
        self.data_output_exchanges = []
        for i in range(AGGREGATORS_COUNT):
            exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, OUTPUT_EXCHANGE, [f"{OUTPUT_EXCHANGE}{i}"]
            )
            self.data_output_exchanges.append(exchange)

    def _process_data(self, transaction):
        bank = transaction.get("to_bank", "")
        hash_val = int(hashlib.md5(bank.encode('utf-8')).hexdigest(), 16)
        aggregator_index = hash_val % AGGREGATORS_COUNT
        
        self.data_output_exchanges[aggregator_index].send(
            message_protocol.internal.serialize(transaction)
        )

    def _process_eof(self, client_id):
        logging.info(f"EOF received in BankDispatcher for client {client_id[:8]}. Replicating downstream")
        for exchange in self.data_output_exchanges:
            exchange.send(message_protocol.internal.serialize([client_id]))

    def process(self, transaction):
        try:
            if isinstance(transaction, dict) and "to_bank" in transaction:
                self._process_data(transaction)
            elif isinstance(transaction, list) and len(transaction) == 1:
                self._process_eof(transaction[0])
        except Exception as e:
            logging.error(f"Error processing message in BankDispatcher: {e}")
            raise e

        return None

    def __del__(self):
        for exchange in self.data_output_exchanges:
            try:
                exchange.close()
            except:
                pass