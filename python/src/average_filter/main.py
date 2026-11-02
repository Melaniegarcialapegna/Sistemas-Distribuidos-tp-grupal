import os
import logging
from common.base_worker import BaseWorker
from common.message_protocol.external import Query
from common.eof_coordinator.queue_eof_coordinator import QueueEofCoordinator
from common.eof_coordinator.exchange_eof_coordinator import ExchangeEofCoordinator
from input_stream.input_stream import InputStream
from common import middleware
from recent_store.recent_store import RecentStore

REPLICA_ID = os.environ["REPLICA_ID"]
AVERAGE_KEY_PREFIX="average_exchange"

class AverageFilterWorker(BaseWorker):
    def __init__(self, batch_size, host,exchange_control, recent_queue, average_exchange,
                 results_queue, tokenizer_replicas, sum_join_amount,store_path):
        super().__init__(batch_size,host)
        self.result_query_id = Query.Q5
        self.clients_average = {} #{client_id : average}
        self.barriers = {} 

        self._create_output_queue(results_queue)
        self.results_queue = results_queue 

        self.store = RecentStore(store_path)

        self.control_exchange_name = exchange_control
        self.control_routing_key = ""

        # Create a InputStream for each input
        queue_coordinator = QueueEofCoordinator(replica_id=REPLICA_ID, previous_stage_replicas=tokenizer_replicas, on_barrier=self._on_recent_barrier, publish_control=self._publish_control_message)

        self.recent = InputStream("recent",queue_coordinator, self._on_recent_data)

        exchange_coordinator= ExchangeEofCoordinator(replica_id=REPLICA_ID, previous_stage_replicas=sum_join_amount, on_barrier=self._on_total_barrier)

        self.totals = InputStream("totals",exchange_coordinator, self._on_total_data)

        self.input_handler = middleware.RabbitMQHandler(host)
        self.input_handler.subscribe_to_queue(recent_queue, self.recent.handle_message)

        self.input_handler.subscribe_to_exchange(exchange_control,[self.control_routing_key], self.recent.handle_message)

        self.input_handler.subscribe_to_exchange(average_exchange,[f"{AVERAGE_KEY_PREFIX}_{REPLICA_ID}"], self.totals.handle_message)

    def process_data(self, client_id, payload):
        #doc: el baseworker pide el process_data pero aca cada stream tiene la suya 
        raise NotImplementedError("AverageFilter process by the InputStream")

    def stop(self):
        self.store.close_all()
        super().stop()

    def _on_recent_data(self, client_id, payload):
        url, lenght = payload
        if client_id not in self.clients_average:
            self.store.append(client_id, url, lenght)
        else:
            self._emit_if_above_average(client_id, url, lenght)

    def _on_recent_barrier(self, client_id):
        self._barrier_reached(client_id)

    def _on_total_data(self, client_id, payload):
        total_len, total_abstracts = payload
        average = total_len / total_abstracts if total_abstracts > 0 else 0
        self._set_average(client_id, average)
    
    def _on_total_barrier(self, client_id):
        if client_id not in self.clients_average:
            self._set_average(client_id, 0)
        self._barrier_reached(client_id)
        
    #Helpers

    def _set_average(self, client_id, average):
        self.clients_average[client_id] = average
        logging.info(f"Client {client_id}: average={average}")
        for url, lenght in self.store.read_all(client_id):
            self._emit_if_above_average(client_id, url, lenght)
        self.store.delete(client_id)

    def _emit_if_above_average(self, client_id, url, lenght):
        average = self.clients_average[client_id]
        if lenght > average:
            self.emit(client_id, self.results_queue, [self.result_query_id,url,lenght])

    def _barrier_reached(self, client_id):
        logging.info(f"Client {client_id}: barrier reached")
        self.barriers[client_id] = self.barriers.get(client_id, 0) + 1
        if self.barriers[client_id] == 2:
            del self.barriers[client_id]
            del self.clients_average[client_id]
            self._flush_all(client_id)


def main():
    logging.basicConfig(level=logging.INFO)

    host = os.environ["RABBITMQ_HOST"]
    batch_size = int(os.environ.get("BATCH_SIZE", 10))
    exchange_control = os.environ["CONTROL_EXCHANGE_NAME"]
    recent_queue = os.environ["RECENT_QUEUE_NAME"]
    average_exchange = os.environ["AVERAGE_EXCHANGE"]
    results_queue = os.environ["RESULTS_QUEUE_NAME"]
    tokenizer_replicas = int(os.environ.get("TOKENIZER_REPLICAS", "1"))
    sum_join_amount = int(os.environ["SUM_JOIN_AMOUNT"])
    store_path = os.environ.get("STORE_PATH")
 
    worker = AverageFilterWorker(batch_size, host, exchange_control, recent_queue, average_exchange,
                                 results_queue, tokenizer_replicas, sum_join_amount, store_path)

    worker.start()
    return 0


if __name__ == "__main__":
    main()