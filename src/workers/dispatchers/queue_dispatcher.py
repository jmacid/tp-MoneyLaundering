import os
from common import middleware
from common.message_protocol.batch import Batch
from common.message_protocol.internal import serialize


class QueueDispatcher:

    def __init__(self):
        outputs = os.getenv("OUTPUTS", "")

        if not outputs:
            raise ValueError("Missing OUTPUTS")

        self.output_queues = [
            queue.strip()
            for queue in outputs.split(",")
            if queue.strip()
        ]

        self.expected_transactions = len(self.output_queues)

        self.middlewares = {
            queue: middleware.MessageMiddlewareQueueRabbitMQ(
                host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
                queue_name=queue,
            )
            for queue in self.output_queues
        }

    def process(self, batch_per_queue: list[list[dict]], original: Batch) -> None:
        for queue, lines in zip(self.output_queues, batch_per_queue):
            
            if not lines and not original.is_last:
                continue

            tb = Batch(
                sequence_number=original.sequence_number, 
                lines=lines, 
                is_last=original.is_last, 
                client_id=original.client_id
            )

            self.middlewares[queue].send(serialize(tb.to_dict()))