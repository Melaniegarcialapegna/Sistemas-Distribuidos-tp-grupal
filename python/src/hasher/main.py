import logging
from common.base_worker import BaseWorker
from common.eof_coordinator.queue_eof_coordinator import QueueEofCoordinator
from common.utils import fnv1a_32, get_env, get_worker_info


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
        self._create_queue_input_handler(input_queue_name, control_exchange_name, control_routing_key,replica_id)


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
    info = get_worker_info(require_input_queue=True)

    hasher_worker = HasherWorker(
        info.batch_size,
        info.service.rabbitmq_host,
        info.input_queue_name,
        info.control.control_routing_key,
        info.control.control_exchange_name,
        get_env("OUTPUT_EXCHANGE_NAME"),
        get_env("OUTPUT_ROUTING_KEY_PREFIX"),
        info.service.replica_id,
        info.control.previous_stage_replicas,
        info.control.next_stage_replicas,
    )
    hasher_worker.start()
    return 0

if __name__ == "__main__":
    main()