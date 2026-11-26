import logging
from common.base_worker import BaseWorker
from common.eof_coordinator.queue_eof_coordinator import QueueEofCoordinator
from common.utils import get_env, get_worker_info


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
        self._create_queue_input_handler(input_queue_name, control_exchange_name, control_routing_key,replica_id)

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
    info = get_worker_info(batch_size_default=100, require_input_queue=True)

    unique_words_filter_worker = UniqueWordsFilterWorker(
        info.batch_size,
        info.service.rabbitmq_host,
        info.input_queue_name,
        info.control.control_routing_key,
        info.control.control_exchange_name,
        get_env("Q3_4_OUTPUT_QUEUE_NAME"),
        info.service.replica_id,
        info.control.previous_stage_replicas,
    )
    unique_words_filter_worker.start()
    return 0

if __name__ == "__main__":
    main()

