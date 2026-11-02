import logging
import os
from common.base_worker import BaseWorker
from common.eof_coordinator.queue_eof_coordinator import QueueEofCoordinator

class UniqueWordsFilterWorker(BaseWorker):
    def __init__(self, batch_size, host, input_queue_name, control_routing_key, control_exchange_name, q3_4_output_queue_name):
        super().__init__(batch_size, host)
        self._create_output_queue(q3_4_output_queue_name)
        self.q3_4_output_queue_name = q3_4_output_queue_name
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
        # Base worker llama a este metodo sobre cada elemento recibido dentro del batch
        # message: [abstract_words_list, in_language]
        abstract_words_list, in_language = message
        if not abstract_words_list:
            return
        single_words = set(abstract_words_list)
        self.emit(client_id, self.q3_4_output_queue_name, [list(single_words), in_language])

def main():
    logging.basicConfig(level=logging.INFO)
    host = os.environ["RABBITMQ_HOST"]
    input_queue_name = os.environ["INPUT_QUEUE_NAME"]
    q3_4_output_queue_name = os.environ["Q3_4_OUTPUT_QUEUE_NAME"]
    batch_size = int(os.environ.get("BATCH_SIZE", 100))
    control_exchange_name = os.environ["CONTROL_EXCHANGE_NAME"]
    control_routing_key = os.environ["CONTROL_ROUTING_KEY"]

    unique_words_filter_worker = UniqueWordsFilterWorker(
        batch_size, host, input_queue_name, control_routing_key,
        control_exchange_name, q3_4_output_queue_name
    )
    unique_words_filter_worker.start()
    return 0

if __name__ == "__main__":
    main()

