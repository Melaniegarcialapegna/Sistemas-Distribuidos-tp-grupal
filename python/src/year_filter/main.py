import logging
import os
from common.base_worker import BaseWorker
from common.eof_coordinator.queue_eof_coordinator import QueueEofCoordinator

FILTER_YEAR = 2020
HOST = os.environ.get("RABBITMQ_HOST")
INPUT_QUEUE_NAME = os.environ.get("INPUT_QUEUE_NAME")
Q1_OUTPUT_QUEUE_NAME = os.environ.get("Q1_OUTPUT_QUEUE_NAME")
Q2_OUTPUT_QUEUE_NAME = os.environ.get("Q2_OUTPUT_QUEUE_NAME")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 10))
CONTROL_EXCHANGE_NAME = os.environ.get("CONTROL_EXCHANGE_NAME")
CONTROL_ROUTING_KEY = os.environ.get("CONTROL_ROUTING_KEY", "")
REPLICA_ID = os.environ.get("REPLICA_ID", "default_replica")
PREVIOUS_STAGE_REPLICAS = int(os.environ.get("PREVIOUS_STAGE_REPLICAS", "1"))

class YearFilterWorker(BaseWorker):
    def __init__(self, batch_size, host, input_queue_name, control_routing_key, control_exchange_name, q1_output_queue_name, q2_output_queue_name, replica_id, previous_stage_replicas):
        super().__init__(batch_size, host)
        self._create_output_queue(q1_output_queue_name)
        self._create_output_queue(q2_output_queue_name)
        self.q1_output_queue_name = q1_output_queue_name
        self.q2_output_queue_name = q2_output_queue_name
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
        year, url, img, sections, name, date, language = message
        if year <= FILTER_YEAR:
            q1_projection = [language, url, date]
            self.emit(client_id, self.q1_output_queue_name, q1_projection)            
            q2_projection = [sections, name, url, img]
            self.emit(client_id, self.q2_output_queue_name, q2_projection)

def main():
    logging.basicConfig(level=logging.INFO)

    year_filter_worker = YearFilterWorker(BATCH_SIZE, HOST, INPUT_QUEUE_NAME, CONTROL_ROUTING_KEY, CONTROL_EXCHANGE_NAME, Q1_OUTPUT_QUEUE_NAME, Q2_OUTPUT_QUEUE_NAME, REPLICA_ID, PREVIOUS_STAGE_REPLICAS)
    year_filter_worker.start()
    return 0

if __name__ == "__main__":
    main()