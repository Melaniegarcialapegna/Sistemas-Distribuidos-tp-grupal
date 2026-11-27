import logging
from common.base_worker import BaseWorker
from common.eof_coordinator.queue_eof_coordinator import QueueEofCoordinator
from common.utils import get_env, get_worker_info

FILTER_YEAR = 2020


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
        self._create_queue_input_handler(input_queue_name, control_exchange_name, control_routing_key,replica_id)

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
    info = get_worker_info(require_input_queue=True)

    year_filter_worker = YearFilterWorker(
        info.batch_size,
        info.service.rabbitmq_host,
        info.input_queue_name,
        info.control.control_routing_key,
        info.control.control_exchange_name,
        get_env("Q1_OUTPUT_QUEUE_NAME"),
        get_env("Q2_OUTPUT_QUEUE_NAME"),
        info.service.replica_id,
        info.control.previous_stage_replicas,
    )
    year_filter_worker.start()
    return 0

if __name__ == "__main__":
    main()