import os
import logging
import signal
import hashlib
from common import middleware, message_protocol

ID = int(os.environ.get("ID", "0"))
MOM_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
INPUT_EXCHANGE = os.environ.get("INPUT_EXCHANGE", "bank_shards")
OUTPUT_EXCHANGE = os.environ.get("OUTPUT_EXCHANGE", "max_bank_transactions")
DISPATCHERS_AMOUNT = int(os.environ.get("DISPATCHERS_AMOUNT", "1"))
RESOLVERS_COUNT = int(os.environ.get("RESOLVERS_COUNT", "1"))

class LocalBankMaxAggregator:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [f"{INPUT_EXCHANGE}{ID}"]
        )
        self.data_output_exchanges = []
        for i in range(RESOLVERS_COUNT):
            exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, OUTPUT_EXCHANGE, [f"{OUTPUT_EXCHANGE}{i}"]
            )
            self.data_output_exchanges.append(exchange)
        
        self.max_by_bank_by_client = {}
        self.eof_count_by_client = {}

    def _process_data(self, client_id, to_bank, from_account, amount_paid):
        if client_id not in self.max_by_bank_by_client:
            self.max_by_bank_by_client[client_id] = {}
        
        client_max = self.max_by_bank_by_client[client_id]
        
        if to_bank not in client_max or amount_paid > client_max[to_bank]["amount_paid"]:
            logging.info(f"Nuevo maximo parcial para banco {to_bank}: {amount_paid}")
            client_max[to_bank] = {
                "from_account": from_account,
                "amount_paid": amount_paid
            }

    def _process_eof(self, client_id):
        if client_id not in self.eof_count_by_client:
            self.eof_count_by_client[client_id] = 0
        self.eof_count_by_client[client_id] += 1

        if self.eof_count_by_client[client_id] < DISPATCHERS_AMOUNT:
            return

        logging.info(f"All EOF received for aggregator {ID} for client {client_id[:8]}")
        
        local_max = self.max_by_bank_by_client.get(client_id, {})
        result_message = {
            "client_id": client_id,
            "local_max": local_max,
            "shard_id": ID
        }
        
        hash_val = int(hashlib.md5(client_id.encode('utf-8')).hexdigest(), 16)
        resolver_index = hash_val % RESOLVERS_COUNT
        
        self.data_output_exchanges[resolver_index].send(
            message_protocol.internal.serialize(result_message)
        )
        
        eof_control = [client_id, ID] 
        self.data_output_exchanges[resolver_index].send(
            message_protocol.internal.serialize(eof_control)
        )

        if client_id in self.max_by_bank_by_client:
            del self.max_by_bank_by_client[client_id]
        del self.eof_count_by_client[client_id]

    def process_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            if isinstance(fields, dict) and "client_id" in fields and "to_bank" in fields:
                self._process_data(
                    fields["client_id"], 
                    fields["to_bank"], 
                    fields["from_account"], 
                    float(fields["amount_paid"])
                )
            elif isinstance(fields, list) and len(fields) == 1:
                self._process_eof(fields[0])
            ack()
        except Exception as e:
            logging.error(f"Error in LocalBankMaxAggregator: {e}")
            nack()

    def handle_sigterm(self, signum, frame):
        self.input_exchange.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        logging.info(f"LocalBankMaxAggregator {ID} ready.")
        self.input_exchange.start_consuming(self.process_message)
        self.input_exchange.close()
        for exchange in self.data_output_exchanges:
            exchange.close()

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    aggregator = LocalBankMaxAggregator()
    aggregator.start()
    return 0

if __name__ == "__main__":
    main()