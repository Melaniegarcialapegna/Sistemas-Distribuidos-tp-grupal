import json
import logging
import os
from common.base_worker import BaseWorker
from common.eof_coordinator.queue_eof_coordinator import QueueEofCoordinator

BIOGRAPHY_SECTION_NAMES = {'biographie', 'biography'}
HOST = os.environ.get("RABBITMQ_HOST")
INPUT_QUEUE_NAME = os.environ.get("INPUT_QUEUE_NAME")
Q2_OUTPUT_QUEUE_NAME = os.environ.get("Q2_OUTPUT_QUEUE_NAME")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 10))
CONTROL_EXCHANGE_NAME = os.environ.get("CONTROL_EXCHANGE_NAME")
CONTROL_ROUTING_KEY = os.environ.get("CONTROL_ROUTING_KEY", "")
REPLICA_ID = os.environ.get("REPLICA_ID", "default_replica")
PREVIOUS_STAGE_REPLICAS = int(os.environ.get("PREVIOUS_STAGE_REPLICAS", "1"))

class BiographyFilterWorker(BaseWorker):
    def __init__(self, batch_size, host, input_queue_name, control_routing_key, control_exchange_name, q2_output_queue_name, replica_id, previous_stage_replicas):
        super().__init__(batch_size, host)
        self._create_output_queue(q2_output_queue_name)
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
        sections, name, url, img_url = message
        if self.has_biography_section(sections):
            q2_projection = [name, url, img_url]
            self.emit(client_id, self.q2_output_queue_name, q2_projection)

    def has_biography_section(self, sections):
        if not isinstance(sections, str):
            return False
        section_names =  [section.get("name", "").lower() for section in json.loads(sections)]
        for biography_section_name in BIOGRAPHY_SECTION_NAMES:
            if biography_section_name in section_names:
                return True
        return False

def main():
    logging.basicConfig(level=logging.INFO)

    biography_filter_worker = BiographyFilterWorker(
        BATCH_SIZE, HOST, INPUT_QUEUE_NAME, CONTROL_ROUTING_KEY,
        CONTROL_EXCHANGE_NAME, Q2_OUTPUT_QUEUE_NAME, REPLICA_ID, PREVIOUS_STAGE_REPLICAS
    )
    biography_filter_worker.start()
    return 0

if __name__ == "__main__":
    main()