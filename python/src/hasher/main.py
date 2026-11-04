import logging
import os
from common.base_worker import BaseWorker
from common.eof_coordinator.queue_eof_coordinator import QueueEofCoordinator
from common.utils import fnv1a_32


HOST = os.environ.get("RABBITMQ_HOST")
INPUT_QUEUE_NAME = os.environ.get("INPUT_QUEUE_NAME")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 10))
CONTROL_EXCHANGE_NAME = os.environ.get("CONTROL_EXCHANGE_NAME")
CONTROL_ROUTING_KEY = os.environ.get("CONTROL_ROUTING_KEY", "")
OUTPUT_EXCHANGE_NAME = os.environ.get("OUTPUT_EXCHANGE_NAME")
OUTPUT_ROUTING_KEY_PREFIX = os.environ.get("OUTPUT_ROUTING_KEY_PREFIX")
REPLICA_ID = os.environ.get("REPLICA_ID", "default_replica")
PREVIOUS_STAGE_REPLICAS = int(os.environ.get("PREVIOUS_STAGE_REPLICAS", "1"))
NEXT_STAGE_REPLICAS = int(os.environ.get("NEXT_STAGE_REPLICAS", "1"))

class HasherWorker(BaseWorker):
    def __init__(self, batch_size, host, input_queue_name, control_routing_key, control_exchange_name, output_exchange_name, output_routing_key_prefix, replica_id, previous_stage_replicas, next_stage_replicas):
        super().__init__(batch_size, host)
        self.next_stage_replicas = next_stage_replicas
        output_routing_keys = []
        for i in range(self.next_stage_replicas):
            output_routing_keys.append(f"{output_routing_key_prefix}_{i}")
        self._create_output_exchange(output_exchange_name, output_routing_keys)
        self.output_exchange_name = output_exchange_name
        self.output_routing_key_prefix = output_routing_key_prefix
        self.eof_coordinator = QueueEofCoordinator(
            replica_id=replica_id,
            previous_stage_replicas=previous_stage_replicas,
            on_barrier=self._flush_all,
            publish_control=self._publish_control_message
        )
        self._create_queue_input_handler(input_queue_name, control_exchange_name, control_routing_key)


    def process_data(self, client_id, message):
        # Base worker llama a este metodo sobre cada articulo dentro del batch
        # message es una lista con los parametros necesarios para esta etapa del articulo
        words, in_language = message
        for word in words:
            index = fnv1a_32(word.encode("utf-8")) % self.next_stage_replicas
            routing_key = f"{self.output_routing_key_prefix}_{index}"
            self.emit(client_id, self.output_exchange_name, [word, in_language], routing_keys=[routing_key])

def main():
    logging.basicConfig(level=logging.INFO)

    hasher_worker = HasherWorker(
        BATCH_SIZE, HOST, INPUT_QUEUE_NAME, CONTROL_ROUTING_KEY,
        CONTROL_EXCHANGE_NAME, OUTPUT_EXCHANGE_NAME, OUTPUT_ROUTING_KEY_PREFIX,
        REPLICA_ID, PREVIOUS_STAGE_REPLICAS, NEXT_STAGE_REPLICAS
    )
    hasher_worker.start()
    return 0

if __name__ == "__main__":
    main()