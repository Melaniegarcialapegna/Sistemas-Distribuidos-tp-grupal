import os
import logging
from common.base_worker import BaseWorker
from common.message_protocol import Query
from common.eof_coordinator.queue_eof_coordinator import QueEofCoordinator
from common.eof_coordinator.exchange_eof_coordinator import ExchangeEofCoordinator
from .input_stream.input_stream import InputStream
from common import middleware

REPLICA_ID = os.environ["REPLICA_ID"]
AVERAGE_KEY_PREFIX="average_exchange"

class AverageFilterWorker(BaseWorker):
    def __init__(self, batch_size, host,recent_queue, average_exchange,
                 results_queue, tokenizer_replicas, sum_join_amount):
        super().__init__(batch_size,host)
        self.result_query_id = Query.Q5
        self.clients = {} #{client_id : average}

        self._create_output_queue(results_queue)
        self.results_queue = results_queue 

        # Create a InputStream for each input
        queue_coordinaator = QueEofCoordinator(replica_id=REPLICA_ID, previous_stage_replicas=tokenizer_replicas, on_barrier=self._on_recent_barrier, publish_control=self._publish_control_message)

        self.recent = InputStream("recent",queue_coordinaator, self._on_recent_data)

        exchange_coordinator= ExchangeEofCoordinator(replica_id=REPLICA_ID, previous_stage_replicas=sum_join_amount, on_barrier=self._on_total_barrier)

        self.totals = InputStream("totals",exchange_coordinator, self._on_total_data)

        self.recent_handler = middleware.RabbitMQHandler(host)
        self.recent_handler.subscribe_to_queue(recent_queue, self.recent.handle_message)

        self.totals_handler = middleware.RabbitMQHandler(host)
        self.totals_handler.subscribe_to_exchange(average_exchange,[f"{AVERAGE_KEY_PREFIX}_{i}" for i in range(sum_join_amount)], self.totals.handle_message)


    def process_data(self, client_id, payload):
        #doc: el baseworker pide el process_data pero aca cada stream tiene la suya 
        raise NotImplementedError("AverageFilter process by the InputStream")

    def _on_recent_data(self, client_id, payload):
        url, lenght = payload
        if client_id not in self.clients:
            #TODO : persistir data
            pass
        else:
            self._emit_if_above_average(client_id, url, lenght)

    def _on_recent_barrier(self, client_id):
        pass

    def _on_total_data(self, client_id, payload):
        total_len, total_abstracts = payload
        average = total_len / total_abstracts if total_abstracts > 0 else 0
        self.clients[client_id] = average
        logging.info(f"Client {client_id}: average={average}")

    def _on_total_barrier(self, client_id):
        #TODO: ver caso borde en que no me llega ningun promedio pq no hay de antes ¿¿
        for url, lenght in self.store.read_all(client_id):
            self._emit_if_above_average(client_id, url, lenght)

    #Helpers
    def _emit_if_above_average(self, client_id, url, lenght):
        average = self.clients[client_id]
        if lenght > average:
            self.emit(client_id, self.results_queue, [self.result_query_id,url,lenght])


def main():
    logging.basicConfig(level=logging.INFO)

    host = os.environ["RABBITMQ_HOST"]
    batch_size = int(os.environ.get("BATCH_SIZE", 10))

    worker = AverageFilterWorker(batch_size,host)

    worker.start()
    return 0


if __name__ == "__main__":
    main()