import os
import logging
from common.base_worker import BaseWorker

class SumJoinWorker(BaseWorker):
    def __init__(self,batch_size, host):
        super().__init__(batch_size,host)
        self.totals = {} #{client_id: [total_len, total_abstracts]}

    def process_data(self, client_id, message):
        partial_len, partial_abstracts = message
        total = self.totals.setdefault(client_id, [0, 0])
        total[0] += partial_len
        total[1] += partial_abstracts


def main():
    logging.basicConfig(level=logging.INFO)

    host = os.environ["RABBITMQ_HOST"]
    input_queue_name = os.environ["INPUT_QUEUE_NAME"]
    batch_size = int(os.environ.get("BATCH_SIZE", 10))
    control_exchange_name = os.environ["CONTROL_EXCHANGE_NAME"]
    control_routing_key = os.environ["CONTROL_ROUTING_KEY"]
    sum_join_exchange = os.environ["SUM_JOIN_EXCHANGE"]
    sum_join_amount = int(os.environ["SUM_JOIN_AMOUNT"])

    sum_worker = SumJoinWorker(batch_size, host, input_queue_name, control_exchange_name, control_routing_key, sum_join_exchange, sum_join_amount)

    logging.info(f"Sum replica {REPLICA_ID} started: input={input_queue_name} "
                 f"output={sum_join_exchange} x{sum_join_amount} previous_replicas={PREVIOUS_STAGE_REPLICAS}")
    
    sum_worker.start()
    return 0


if __name__ == "__main__":
    main()