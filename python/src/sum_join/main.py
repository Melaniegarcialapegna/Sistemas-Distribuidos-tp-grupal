import logging
from common.base_worker import BaseWorker
from common.eof_coordinator.exchange_eof_coordinator import ExchangeEofCoordinator
from common.utils import get_env, get_worker_info

SUM_JOIN_KEY_PREFIX = "sum_join_exchange"
AVERAGE_KEY_PREFIX = "average_exchange"


class SumJoinWorker(BaseWorker):
    def __init__(self, batch_size, host, sum_join_exchange, average_exchange, average_filter_amount, replica_id, previous_stage_replicas):
        super().__init__(batch_size, host)
        self.totals = {} #{client_id: [total_len, total_abstracts]}

        self.average_exchange = average_exchange
        self.average_routing_keys = [f"{AVERAGE_KEY_PREFIX}_{i}" for i in range(average_filter_amount)]
        self._create_output_exchange(average_exchange, self.average_routing_keys)

        self.eof_coordinator = ExchangeEofCoordinator( 
            replica_id=replica_id,
            previous_stage_replicas=previous_stage_replicas,
            on_barrier=self._on_barrier,
        )

        self._create_exchange_input_handler(sum_join_exchange, [f"{SUM_JOIN_KEY_PREFIX}_{replica_id}"],replica_id)

    def process_data(self, client_id, message):
        partial_len, partial_abstracts = message
        total = self.totals.setdefault(client_id, [0, 0])
        total[0] += partial_len
        total[1] += partial_abstracts

    def _on_barrier(self,client_id):
        if client_id in self.totals:
            total_len, total_abstracts = self.totals.pop(client_id)
            logging.info(f"Client {client_id}: totals len={total_len} "
                         f"abstracts={total_abstracts}")

            for routing_key in self.average_routing_keys:
                self.emit(client_id, self.average_exchange,[total_len, total_abstracts],routing_keys=[routing_key])
        else:
            logging.info(f"Client {client_id}: no partial sums on this replica")
        self._flush_all(client_id)

def main():
    logging.basicConfig(level=logging.INFO)
    info = get_worker_info()
    sum_join_exchange = get_env("SUM_JOIN_EXCHANGE")
    average_exchange = get_env("AVERAGE_EXCHANGE")
    average_filter_amount = get_env("AVERAGE_FILTER_AMOUNT", int, default=1)

    worker = SumJoinWorker(
        info.batch_size,
        info.service.rabbitmq_host,
        sum_join_exchange,
        average_exchange,
        average_filter_amount,
        info.service.replica_id,
        info.control.previous_stage_replicas,
    )
    logging.info(f"SumJoin replica {info.service.replica_id} started: "
                 f"listening {sum_join_exchange}/{SUM_JOIN_KEY_PREFIX}_{info.service.replica_id}, "
                 f"broadcasting to {average_filter_amount} Average filters via {average_exchange}")
    worker.start()
    return 0


if __name__ == "__main__":
    main()