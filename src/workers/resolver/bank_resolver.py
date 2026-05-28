import os
import logging
import signal
from common import middleware, message_protocol

ID = int(os.environ.get("ID", "0"))
MOM_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
INPUT_EXCHANGE = os.environ.get("INPUT_EXCHANGE", "max_bank_transactions")
OUTPUT_QUEUE = os.environ.get("OUTPUT_QUEUE", "bank_max_transactions_results")
AGGREGATORS_COUNT = int(os.environ.get("AGGREGATORS_COUNT", "1"))

class BankResolver:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [f"{INPUT_EXCHANGE}{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        
        self.bank_names_map = {}
        
        self.global_max_by_client = {}
        self.eof_shards_tracked = {}

    def _process_bank_mapping(self, mapping_data):
        self.bank_names_map.update(mapping_data)

    def _process_local_max(self, client_id, local_max):
        if client_id not in self.global_max_by_client:
            self.global_max_by_client[client_id] = {}
            
        global_maxima = self.global_max_by_client[client_id]
        
        for bank_id, data in local_max.items():
            if bank_id not in global_maxima or data["amount_paid"] > global_maxima[bank_id]["amount_paid"]:
                global_maxima[bank_id] = data

    def _process_eof(self, client_id, shard_id):
        if client_id not in self.eof_shards_tracked:
            self.eof_shards_tracked[client_id] = set()
            
        self.eof_shards_tracked[client_id].add(shard_id)

        if len(self.eof_shards_tracked[client_id]) < AGGREGATORS_COUNT:
            return
        
        id_results = self.global_max_by_client.get(client_id, {})
        final_formatted_results = []
        
        for bank_id, data in id_results.items():
            name_bank = self.bank_names_map.get(bank_id, f"Unknown Bank ({bank_id})")
            
            final_formatted_results.append({
                "name_bank": name_bank,
                "client": data["from_account"],
                "amount": data["amount_paid"]
            })
        
        gateway_payload = {
            "client_id": client_id,
            "query": "query_2",
            "results": final_formatted_results
        }
        
        self.output_queue.send(message_protocol.internal.serialize(gateway_payload))
        
        if client_id in self.global_max_by_client:
            del self.global_max_by_client[client_id]
        del self.eof_shards_tracked[client_id]

    def process_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            
            if isinstance(fields, dict):
                if "bank_mapping" in fields:
                    self._process_bank_mapping(fields["bank_mapping"])
                elif "local_max" in fields:
                    self._process_local_max(fields["client_id"], fields["local_max"])
                    
            elif isinstance(fields, list) and len(fields) == 2:
                self._process_eof(fields[0], fields[1])
                
            ack()
        except Exception as e:
            logging.error(f"Error processing message in BankResolver: {e}")
            nack()

    def handle_sigterm(self, signum, frame):
        self.input_exchange.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        logging.info(f"BankResolver {ID} listening")
        self.input_exchange.start_consuming(self.process_message)
        self.input_exchange.close()
        self.output_queue.close()

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    resolver = BankResolver()
    resolver.start()
    return 0

if __name__ == "__main__":
    main()