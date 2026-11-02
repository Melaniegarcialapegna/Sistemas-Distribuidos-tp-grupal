import os
import logging
from common.base_worker import BaseWorker
from common.message_protocol import Query
from common.eof_coordinator.queue_eof_coordinator import QueEofCoordinator
from common.eof_coordinator.exchange_eof_coordinator import ExchangeEofCoordinator
from .input_stream.input_stream import InputStream

REPLICA_ID = os.environ["REPLICA_ID"]
AVERAGE_KEY_PREFIX="average_exchange"

class AverageFilterWorker(BaseWorker):
    def __init__(self, batch_size, host,recent_queue,
                 control_exchange, control_routing_key, average_exchange,
                 results_queue, tokenizer_replicas, sum_join_amount):
        super().__init__(batch_size,host)
        self.result_query_id = Query.Q5
        self.clients = {} #{client_id : ver}

        self._create_output_queue(results_queue)
        self.results_queue = results_queue 

        # Create a InputStream for each input
        queue_coordinaator = QueEofCoordinator(replica_id=REPLICA_ID, previous_stage_replicas=tokenizer_replicas, on_barrier=self._on_barrier, publish_control=self._publish_control_message)

        self.recent = InputStream("recent",queue_coordinaator, self._on_recent_data)

        exchange_coordinator= ExchangeEofCoordinator(replica_id=REPLICA_ID, previous_stage_replicas=sum_join_amount, on_barrier=self._on_barrier)

        self.totals = InputStream("totals",exchange_coordinator, self._on_total_data)


def main():
    logging.basicConfig(level=logging.INFO)

    host = os.environ["RABBITMQ_HOST"]
    batch_size = int(os.environ.get("BATCH_SIZE", 10))

    worker = AverageFilterWorker(batch_size,host)

    worker.start()
    return 0


if __name__ == "__main__":
    main()