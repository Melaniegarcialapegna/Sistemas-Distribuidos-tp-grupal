import logging
import os
from common.base_worker import BaseWorker
from common.eof_coordinator.queue_eof_coordinator import QueueEofCoordinator

class YearFilterWorker(BaseWorker):
    def __init__(self, batch_size, host, input_queue_name, control_routing_key, control_exchange_name, q1_output_queue_name, q2_output_queue_name):
        super().__init__(batch_size, host)
        self._create_output_queue(q1_output_queue_name)
        self._create_output_queue(q2_output_queue_name)
        self.q1_output_queue_name = q1_output_queue_name
        self.q2_output_queue_name = q2_output_queue_name
        self.eof_coordinator = QueueEofCoordinator(
            replica_id=os.environ.get("REPLICA_ID", "default_replica"),
            previous_stage_replicas=int(os.environ.get("PREVIOUS_STAGE_REPLICAS", "1")),
            on_barrier=self._flush_all,
            publish_control=self._publish_control_message
        )
        self.input_queue_name = input_queue_name
        self._create_queue_input_handler(input_queue_name, control_exchange_name, control_routing_key)

    def process_data(self, client_id, message):
        # Base worker llama a este metodo sobre cada articulo dentro del batch
        # message es una lista con los parametros necesarios para esta etapa del articulo
        year, url, img, sections, name, date, language = message
        if year <= 2020:
            q1_projection = [language, url, date]
            self.emit(client_id, self.q1_output_queue_name, q1_projection)            
            q2_projection = [sections, name, url, img]
            self.emit(client_id, self.q2_output_queue_name, q2_projection)

def main():
    logging.basicConfig(level=logging.INFO)
    host = os.environ["RABBITMQ_HOST"]
    input_queue_name = os.environ["INPUT_QUEUE_NAME"]
    q1_output_queue_name = os.environ["Q1_OUTPUT_QUEUE_NAME"]
    q2_output_queue_name = os.environ["Q2_OUTPUT_QUEUE_NAME"]
    batch_size = int(os.environ.get("BATCH_SIZE", 10))
    control_exchange_name = os.environ["CONTROL_EXCHANGE_NAME"]
    control_routing_key = os.environ["CONTROL_ROUTING_KEY"]

    year_filter_worker = YearFilterWorker(batch_size, host, input_queue_name, control_routing_key, control_exchange_name, q1_output_queue_name, q2_output_queue_name)
    year_filter_worker.start()
    return 0

if __name__ == "__main__":
    main()