import logging
import os
from common.base_worker import BaseWorker
from common.eof_coordinator.queue_eof_coordinator import QueueEofCoordinator

HOST = os.environ.get("RABBITMQ_HOST")
INPUT_QUEUE_NAME = os.environ.get("INPUT_QUEUE_NAME")
Q3_4_OUTPUT_QUEUE_NAME = os.environ.get("Q3_4_OUTPUT_QUEUE_NAME")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 100))
CONTROL_EXCHANGE_NAME = os.environ.get("CONTROL_EXCHANGE_NAME")
CONTROL_ROUTING_KEY = os.environ.get("CONTROL_ROUTING_KEY", "")
REPLICA_ID = os.environ.get("REPLICA_ID", "default_replica")
PREVIOUS_STAGE_REPLICAS = int(os.environ.get("PREVIOUS_STAGE_REPLICAS", "1"))

class UniqueWordsFilterWorker(BaseWorker):
    def __init__(self, batch_size, host, input_queue_name, control_routing_key, control_exchange_name, q3_4_output_queue_name, replica_id, previous_stage_replicas):
        super().__init__(batch_size, host)
        self._create_output_queue(q3_4_output_queue_name)
        self.q3_4_output_queue_name = q3_4_output_queue_name
        self.eof_coordinator = QueueEofCoordinator(
            replica_id=replica_id,
            previous_stage_replicas=previous_stage_replicas,
            on_barrier=self._flush_all,
            publish_control=self._publish_control_message
        )
        self._create_queue_input_handler(input_queue_name, control_exchange_name, control_routing_key)

    def process_data(self, client_id, message):
        # Base worker llama a este metodo sobre cada elemento recibido dentro del batch
        # message: [abstract_words_list, in_language]
        abstract_words_list, in_language = message
        if not abstract_words_list:
            return
        single_words = set(abstract_words_list)
        self.emit(client_id, self.q3_4_output_queue_name, [list(single_words), in_language])

def main():
    logging.basicConfig(level=logging.INFO)

    unique_words_filter_worker = UniqueWordsFilterWorker(
        BATCH_SIZE, HOST, INPUT_QUEUE_NAME, CONTROL_ROUTING_KEY,
        CONTROL_EXCHANGE_NAME, Q3_4_OUTPUT_QUEUE_NAME, REPLICA_ID, PREVIOUS_STAGE_REPLICAS
    )
    unique_words_filter_worker.start()
    return 0

if __name__ == "__main__":
    main()

