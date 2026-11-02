import os
import logging
from common.base_worker import BaseWorker
from common.eof_coordinator.queue_eof_coordinator import QueueEofCoordinator
from common.utils import fnv1a_32


REPLICA_ID = os.environ["REPLICA_ID"]
PREVIOUS_STAGE_REPLICAS = int(os.environ.get("PREVIOUS_STAGE_REPLICAS", "1"))
SUM_JOIN_PREFIX="sum_join_exchange"

class SumWorker(BaseWorker):
    def __init__(self, batch_size, host, input_queue_name, control_exchange_name,
                 control_routing_key, sum_join_exchange, sum_join_amount):
        super().__init__(batch_size,host)
        self.totals = {} #{client_id: [total_len, total_abstracts]}

        self.sum_join_exchange = sum_join_exchange
        self.sum_join_amount = sum_join_amount

        self._create_output_exchange(sum_join_exchange,[f"{SUM_JOIN_PREFIX}_{i}" for i in range(sum_join_amount)])

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

    def _on_barrier(self,client_id):
        if client_id in self.totals:
            total_len, total_abstracts = self.totals.pop(client_id)
            client_id_bytes = client_id.to_bytes((client_id.bit_length() + 7) // 8 or 1, "big")
            routing_key = f"{SUM_JOIN_PREFIX}_{fnv1a_32(client_id_bytes) % self.sum_join_amount}"

            logging.info(f"Client {client_id}: partial sum len={total_len} abstracts={total_abstracts} | to {routing_key}")

            self.emit(client_id, self.sum_join_exchange,[total_len, total_abstracts],routing_keys=[routing_key])
        else:
            logging.info(f"Client {client_id}: this replica dont processed any abstract)")
        self._flush_all(client_id)


def main():
    logging.basicConfig(level=logging.INFO)

    host = os.environ["RABBITMQ_HOST"]
    input_queue_name = os.environ["INPUT_QUEUE_NAME"]
    batch_size = int(os.environ.get("BATCH_SIZE", 10))
    control_exchange_name = os.environ["CONTROL_EXCHANGE_NAME"]
    control_routing_key = os.environ["CONTROL_ROUTING_KEY"]
    sum_join_exchange = os.environ["SUM_JOIN_EXCHANGE"]
    sum_join_amount = int(os.environ["SUM_JOIN_AMOUNT"])

    sum_worker = SumWorker(batch_size, host, input_queue_name, control_exchange_name, control_routing_key, sum_join_exchange, sum_join_amount)

    logging.info(f"Sum replica {REPLICA_ID} started: input={input_queue_name} "
                 f"output={sum_join_exchange} x{sum_join_amount} previous_replicas={PREVIOUS_STAGE_REPLICAS}")
    
    sum_worker.start()
    return 0


if __name__ == "__main__":
    main()