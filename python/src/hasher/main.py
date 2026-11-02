import logging
import os
from common.base_worker import BaseWorker
from common.eof_coordinator.queue_eof_coordinator import QueueEofCoordinator

FNV32_OFFSET, FNV32_PRIME = 0x811C9DC5, 0x01000193

class HasherWorker(BaseWorker):
    def __init__(self, batch_size, host, input_queue_name, control_routing_key, control_exchange_name, output_exchange_name, output_routing_key_prefix):
        super().__init__(batch_size, host)
        self.next_stage_replicas = int(os.environ.get("NEXT_STAGE_REPLICAS", "1"))
        output_routing_keys = []
        for i in range(self.next_stage_replicas):
            output_routing_keys.append(f"{output_routing_key_prefix}_{i}")
        self._create_output_exchange(output_exchange_name, output_routing_keys)
        self.output_exchange_name = output_exchange_name
        self.output_routing_key_prefix = output_routing_key_prefix
        self.previous_stage_replicas = int(os.environ.get("PREVIOUS_STAGE_REPLICAS", "1"))
        self.eof_coordinator = QueueEofCoordinator(
            replica_id=os.environ.get("REPLICA_ID", "default_replica"),
            previous_stage_replicas=self.previous_stage_replicas,
            on_barrier=self._flush_all,
            publish_control=self._publish_control_message
        )
        self.input_queue_name = input_queue_name
        self._create_queue_input_handler(input_queue_name, control_exchange_name, control_routing_key)


    def process_data(self, client_id, message):
        # Base worker llama a este metodo sobre cada articulo dentro del batch
        # message es una lista con los parametros necesarios para esta etapa del articulo
        words, in_language = message
        for word in words:
            index = self.fnv1a_32(word.encode("utf-8")) % self.next_stage_replicas
            routing_key = f"{self.output_routing_key_prefix}_{index}"
            self.emit(client_id, self.output_exchange_name, [word, in_language], routing_keys=[routing_key])

    @staticmethod
    def fnv1a_32(data: bytes) -> int:           
        h = FNV32_OFFSET
        for byte in data:
            h ^= byte
            h = (h * FNV32_PRIME) & 0xFFFFFFFF
        return h

def main():
    logging.basicConfig(level=logging.INFO)
    host = os.environ["RABBITMQ_HOST"]
    input_queue_name = os.environ["INPUT_QUEUE_NAME"]
    batch_size = int(os.environ.get("BATCH_SIZE", 10))
    control_exchange_name = os.environ["CONTROL_EXCHANGE_NAME"]
    control_routing_key = os.environ["CONTROL_ROUTING_KEY"]
    output_exchange_name = os.environ["OUTPUT_EXCHANGE_NAME"]
    output_routing_key_prefix = os.environ["OUTPUT_ROUTING_KEY_PREFIX"]

    hasher_worker = HasherWorker(batch_size, host, input_queue_name, control_routing_key, control_exchange_name, output_exchange_name, output_routing_key_prefix)
    hasher_worker.start()
    return 0

if __name__ == "__main__":
    main()