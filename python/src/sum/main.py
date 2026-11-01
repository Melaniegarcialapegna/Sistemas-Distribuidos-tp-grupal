import os
from common.base_worker import BaseWorker
from common.eof_coordinator.queue_eof_coordinator import QueueEofCoordinator


REPLICA_ID = os.environ["REPLICA_ID"]
PREVIOUS_STAGE_REPLICAS = int(os.environ.get("PREVIOUS_STAGE_REPLICAS", "1"))

class SumWorker(BaseWorker):
    def __init__(self, batch_size, host, input_queue_name, control_exchange_name,
                 control_routing_key, sum_join_exchange, sum_join_amount):
        super().__init__(batch_size,host)
        self.totals = {} #{client_id: [total_len, total_abstracts]}

        self._create_output_exchange(sum_join_exchange,[f"sum_join_exchange_{i}" for i in range(sum_join_amount)])

        self.eof_coordinator = QueueEofCoordinator(
            replica_id=REPLICA_ID,
            previous_stage_replicas=PREVIOUS_STAGE_REPLICAS,
            on_barrier=self._on_barrier,
            publish_control=self._publish_control_message,
        )

        self._create_queue_input_handler(input_queue_name, control_exchange_name, control_routing_key)

    def process_data(self, client_id, message):
        total = self.totals.setdefault(client_id, [0, 0])
        total[0] += message[0] #len of the abstract
        total[1] += 1 # one abstract more


def main():
    logging.basicConfig(level=logging.INFO)
    sum_worker = SumWorker()
    sum_worker.start()
    return 0


if __name__ == "__main__":
    main()