import os
import json
import logging
from collections import defaultdict
from common import middleware

logging.basicConfig(level=logging.INFO)
EOF_CONTROL_QUEUE = os.getenv("EOF_CONTROL_QUEUE", "eof_control_queue")
FINAL_QUEUE = os.getenv("FINAL_QUEUE", "minor_transactions")

def get_topology():
    topology_env = os.environ["TOPOLOGY"]
    topology_nodes = [node.strip() for node in topology_env.split(",") if node.strip()]
    topology = {}
    for i in range(len(topology_nodes) - 1):
        topology[topology_nodes[i]] = topology_nodes[i + 1]
    if topology_nodes:
        topology[topology_nodes[-1]] = "END"
    return topology

topology = get_topology()


def main():
    control_queue = middleware.MessageMiddlewareQueueRabbitMQ("rabbitmq", EOF_CONTROL_QUEUE)
    
    final_gateway_queue = middleware.MessageMiddlewareQueueRabbitMQ("rabbitmq", FINAL_QUEUE)
    
    state = defaultdict(lambda: defaultdict(lambda: {"processed": 0, "emitted": 0}))

    def check_eof(client_id):
        if state[client_id]["gateway"]["emitted"] == 0:
            return 

        expected_next = state[client_id]["gateway"]["emitted"]
        current_node = topology["gateway"]

        while current_node != "END":
            if state[client_id][current_node]["processed"] < expected_next:
                return 
            
            expected_next = state[client_id][current_node]["emitted"]
            current_node = topology[current_node]

        logging.info(f"EOF Alcanzado para el cliente {client_id}. Enviando a Gateway...")
        eof_message = json.dumps([client_id])
        final_gateway_queue.send(eof_message.encode('utf-8'))
        
        del state[client_id]

    def on_message(body, ack, nack):
        try:
            msg = json.loads(body)
            client_id = msg["client_id"]
            node = msg["node"]
            
            if node == "gateway":
                state[client_id][node]["emitted"] = msg["emitted"]
            else:
                state[client_id][node]["processed"] += msg["processed"]
                state[client_id][node]["emitted"] += msg["emitted"]

            check_eof(client_id)
            ack()
        except Exception as e:
            logging.error(f"Error en tracker: {e}")
            nack()

    logging.info("Tracker iniciado y esperando métricas...")
    control_queue.start_consuming(on_message)

if __name__ == "__main__":
    main()